//! Spider driver — configuration exactly as PLAN.md §5.
//!
//! Subscription protocol (reviews A-CRIT-1 / B-3): the broadcast channel
//! capacity is `expected + 64`, i.e. at least the total number of messages
//! ever sent, so the channel mathematically cannot drop a page even with a
//! fully stalled consumer. `Lagged` handling remains as defense in depth and
//! invalidates the trial. ONE drain task performs the page work (no per-page
//! spawning). `scrape()` is never used.
//!
//! **RSS disclosure (stronger than PLAN.md §7's wording):** spider's
//! `subscribe` keeps the ORIGINAL broadcast receiver internally
//! (`Website.channel` stores `(Sender, Arc<Receiver>)`) and that receiver
//! never reads, so at drop-free capacity the channel retains EVERY page
//! until `unsubscribe` drops the channel — regardless of how fast our drain
//! keeps up. Spider's peak RSS therefore includes up to the full corpus, and
//! the channel teardown (an O(pages) slot cleanup) runs inside the timed
//! region. This cannot be avoided through spider's public API without giving
//! up the drop-free guarantee: any bounded capacity smaller than the corpus
//! reintroduces `Lagged` trials on extraction rows, where the single-task
//! scraper re-parse (mandated by parse parity, PLAN.md §7) is deterministic-
//! ally slower than C = 32 loopback fetches. This is disclosed in the report
//! as spider's subscription-model cost, per the A-CRIT-1 resolution
//! ("prevention, not detection").

use std::sync::Arc;
use std::time::Duration;

use spider::website::Website;
use tokio::sync::broadcast::error::RecvError;

use crate::engines::{Accum, EngineOutcome};
use crate::scenario::{PageWork, TrialSpec};

/// Scenarios spider cannot run, with the reason (reported as N/A, not failed).
///
/// `redirects`: spider 2.52.9 screens every redirect hop through its SSRF
/// guard (`Website::is_ssrf_redirect`) under EVERY redirect policy — `Loose`
/// and `Strict` both refuse hops to loopback/private addresses, and `None`
/// follows no redirects at all. The bench server lives on `127.0.0.1`, and
/// every link in `redirects` goes through a 301 hop, so spider fetches the
/// seed's 301, refuses the hop, and stops. No harness configuration can
/// produce a valid trial; the row reports millipede vs baseline only.
pub fn unsupported_reason(scenario: &str) -> Option<&'static str> {
    match scenario {
        "redirects" => Some(
            "spider 2.52.9 blocks redirects to loopback addresses (SSRF guard in every \
redirect policy); the loopback bench server cannot exercise spider's redirect path",
        ),
        _ => None,
    }
}

pub async fn run(
    spec: &TrialSpec,
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
                    // Extraction rows: spider's internal lol_html pass extracts
                    // links only and exposes no DOM; its native extraction API
                    // (spider_utils::css_query_select_map_streamed 2.52.9) also
                    // re-parses with spider_scraper, an html5ever full-DOM
                    // parser like scraper. Re-parsing here therefore matches
                    // what any spider extraction user pays (PLAN.md §7).
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

    let mut errors = Vec::new();
    if lagged {
        errors.push("spider broadcast receiver reported Lagged (trial invalid)".to_owned());
    }
    let count = accum.count.load(std::sync::atomic::Ordering::Acquire);
    if count != expected {
        errors.push(format!("spider drained {count} pages != expected {expected}"));
    }
    // The drain task has joined, so the snapshot is final.
    Ok(accum.snapshot(errors))
}
