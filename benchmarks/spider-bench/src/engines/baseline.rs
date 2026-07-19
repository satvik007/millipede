//! Baseline "speed of light" control: a raw reqwest client fetching the exact
//! known URL list through the SAME server process and instrumented handler
//! path as the crawlers (review A-5).
//!
//! Note: PLAN.md sketches this driver with `FuturesUnordered`; the pinned
//! dependency set (§9, copied verbatim) does not include `futures-util`, so
//! the same bounded concurrency is implemented with C tokio worker tasks
//! pulling from a shared atomic index — behaviorally equivalent for a fixed
//! URL list.
//!
//! The entry URL list is computed by the ORCHESTRATOR and delivered to the
//! child before `go` (`TrialSpec::entry_urls`): building and allocating the
//! full list inside the timed region would depress the measured ceiling and
//! make the 70% server-bound publication gate artificially easy to pass.
//! Only the HTTP client construction and the fetch loop are timed (review
//! A-2: client construction is charged to every engine).
//!
//! The baseline always performs Accounting work only (count/bytes/checksum);
//! it is a fetch ceiling, not an extraction engine, so `Expected::records`/
//! `digest` gates do not apply to it.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::engines::{Accum, EngineOutcome};
use crate::scenario::{PageWork, TrialSpec};

pub async fn run(
    spec: &TrialSpec,
    concurrency: usize,
    _root_url: &str,
) -> anyhow::Result<EngineOutcome> {
    anyhow::ensure!(
        !spec.entry_urls.is_empty(),
        "baseline requires the orchestrator-supplied entry URL list"
    );

    // Client construction is INSIDE the timed region (review A-2).
    let client = reqwest::Client::builder()
        .user_agent("millipede-bench/1.0")
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::limited(7))
        .build()?;

    let accum = Arc::new(Accum::default());
    let next = Arc::new(AtomicUsize::new(0));
    let urls = Arc::new(spec.entry_urls.clone());
    let errors = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    let mut workers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let client = client.clone();
        let accum = Arc::clone(&accum);
        let next = Arc::clone(&next);
        let urls = Arc::clone(&urls);
        let errors = Arc::clone(&errors);
        workers.push(tokio::spawn(async move {
            loop {
                let i = next.fetch_add(1, Ordering::AcqRel);
                let Some(target) = urls.get(i) else { break };
                match fetch_one(&client, target).await {
                    Ok(body) => {
                        // Ceiling control: Accounting only, never Extract.
                        accum.record_body(&PageWork::Accounting, &body, |_| {});
                    }
                    Err(err) => errors
                        .lock()
                        .expect("baseline error mutex poisoned")
                        .push(format!("baseline fetch {target}: {err:#}")),
                }
            }
        }));
    }
    for worker in workers {
        worker.await?;
    }

    let mut errors = Arc::into_inner(errors)
        .ok_or_else(|| anyhow::anyhow!("worker still holds the error list after join"))?
        .into_inner()
        .expect("baseline error mutex poisoned");
    let expected = spec.expected.pages;
    let count = accum.count.load(Ordering::Acquire);
    if count != expected {
        errors.push(format!("baseline fetched {count} pages != expected {expected}"));
    }
    // All workers joined, so the snapshot is final.
    Ok(accum.snapshot(errors))
}

async fn fetch_one(client: &reqwest::Client, url: &str) -> anyhow::Result<bytes::Bytes> {
    let response = client.get(url).send().await?;
    let status = response.status();
    anyhow::ensure!(status.is_success(), "status {status}");
    Ok(response.bytes().await?)
}
