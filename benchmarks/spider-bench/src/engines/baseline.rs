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
//! The baseline always performs Accounting work only (count/bytes/checksum);
//! it is a fetch ceiling, not an extraction engine, so `Expected::records`/
//! `digest` gates do not apply to it.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::engines::{Accum, EngineOutcome};
use crate::scenario::{PageWork, ScenarioSpec};

pub async fn run(
    spec: &ScenarioSpec,
    concurrency: usize,
    root_url: &str,
) -> anyhow::Result<EngineOutcome> {
    let root = url::Url::parse(root_url)?;
    let base = format!(
        "{}://{}",
        root.scheme(),
        root.host_str()
            .map(|h| match root.port() {
                Some(p) => format!("{h}:{p}"),
                None => h.to_owned(),
            })
            .ok_or_else(|| anyhow::anyhow!("root URL has no host"))?
    );

    // Exact URL list: every unique page, but entered through its redirect
    // source when one exists, so the baseline pays the same 301 hops the
    // crawlers pay and the per-hop server gate holds for all engines.
    let mut source_for_target: std::collections::BTreeMap<&str, &str> = spec
        .site
        .redirects
        .iter()
        .map(|(source, target)| (target.as_str(), source.as_str()))
        .collect();
    let urls: Vec<String> = spec
        .site
        .pages
        .keys()
        .map(|path| {
            let entry = source_for_target.remove(path.as_str()).unwrap_or(path);
            format!("{base}{entry}")
        })
        .collect();

    // Client construction is INSIDE the timed region (review A-2).
    let client = reqwest::Client::builder()
        .user_agent("millipede-bench/1.0")
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::limited(7))
        .build()?;

    let accum = Arc::new(Accum::default());
    let next = Arc::new(AtomicUsize::new(0));
    let urls = Arc::new(urls);
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

    let accum = Arc::into_inner(accum)
        .ok_or_else(|| anyhow::anyhow!("worker still holds the accumulator after join"))?;
    let mut errors = Arc::into_inner(errors)
        .ok_or_else(|| anyhow::anyhow!("worker still holds the error list after join"))?
        .into_inner()
        .expect("baseline error mutex poisoned");
    let expected = spec.expected.pages;
    let count = accum.count.load(Ordering::Acquire);
    if count != expected {
        errors.push(format!("baseline fetched {count} pages != expected {expected}"));
    }
    Ok(accum.into_outcome(errors))
}

async fn fetch_one(client: &reqwest::Client, url: &str) -> anyhow::Result<bytes::Bytes> {
    let response = client.get(url).send().await?;
    let status = response.status();
    anyhow::ensure!(status.is_success(), "status {status}");
    Ok(response.bytes().await?)
}
