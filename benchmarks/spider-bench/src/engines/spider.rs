//! Spider driver — configuration exactly as PLAN.md §5.
//!
//! Subscription protocol (reviews A-CRIT-1 / B-3): the broadcast channel
//! capacity is `expected + 64`, i.e. at least the total number of messages
//! ever sent, so the channel mathematically cannot drop a page even with a
//! fully stalled consumer. `Lagged` handling remains as defense in depth and
//! invalidates the trial. ONE drain task performs the page work (no per-page
//! spawning). `scrape()` is never used.

use std::sync::Arc;
use std::time::Duration;

use spider::website::Website;
use tokio::sync::broadcast::error::RecvError;

use crate::engines::{Accum, EngineOutcome};
use crate::scenario::{PageWork, ScenarioSpec};

pub async fn run(
    spec: &ScenarioSpec,
    concurrency: usize,
    root_url: &str,
) -> anyhow::Result<EngineOutcome> {
    let expected = spec.expected.pages;
    let work = spec.work;
    let accum = Arc::new(Accum::default());

    // Website construction happens here, INSIDE the timed region (review A-2);
    // spider builds its HTTP client inside `crawl()`, also timed.
    let mut website = Website::new(root_url);
    website
        .with_concurrency_limit(Some(concurrency))
        .with_delay(0)
        .with_respect_robots_txt(false)
        .with_retry(0)
        .with_depth(0) // unlimited; the site is terminal
        .with_limit(u32::try_from(expected)? + 16)
        .with_user_agent(Some("millipede-bench/1.0"))
        .with_request_timeout(Some(Duration::from_secs(15)))
        .with_redirect_limit(7)
        .with_subdomains(false)
        .with_tld(false)
        .with_normalize(false)
        .with_return_page_links(false)
        .with_full_resources(false)
        .with_shared_queue(false)
        .with_modify_headers(false)
        .with_no_control_thread(true);

    // Capacity >= total messages: the channel can NEVER drop (A-CRIT-1/B-3).
    // Verified against the compiled spider 2.52.9 (`sync` feature): unlike
    // older docs suggesting Option, `subscribe(capacity)` returns a
    // `tokio::sync::broadcast::Receiver<Page>` directly.
    let capacity = usize::try_from(expected)? + 64;
    let mut rx = website.subscribe(capacity);

    let drain_accum = Arc::clone(&accum);
    let drain = tokio::spawn(async move {
        let mut lagged = false;
        loop {
            match rx.recv().await {
                Ok(page) => {
                    // Raw rows: count/bytes/checksum only — NO re-parse (A-4).
                    // Extraction rows: spider does not expose its lol_html DOM,
                    // so the shared scraper fn must re-parse the bytes here — a
                    // documented architectural difference (PLAN.md §7).
                    let body = page.get_html_bytes_u8();
                    drain_accum.record_body(&work, body, |digest| {
                        if let PageWork::Extract(extract) = work {
                            let html = scraper::Html::parse_document(&page.get_html());
                            extract(&html, digest);
                        }
                    });
                }
                Err(RecvError::Lagged(_)) => lagged = true, // defense in depth
                Err(RecvError::Closed) => break,
            }
        }
        lagged
    });

    website.crawl().await; // client construction happens inside — timed (§6)
    website.unsubscribe();
    let lagged = drain.await?;

    let accum = Arc::into_inner(accum)
        .ok_or_else(|| anyhow::anyhow!("drain still holds the accumulator after join"))?;
    let mut errors = Vec::new();
    if lagged {
        errors.push("spider broadcast receiver reported Lagged (trial invalid)".to_owned());
    }
    let count = accum.count.load(std::sync::atomic::Ordering::Acquire);
    if count != expected {
        errors.push(format!("spider drained {count} pages != expected {expected}"));
    }
    Ok(accum.into_outcome(errors))
}
