//! Millipede driver — configuration as PLAN.md §5, with two deliberate
//! deviations found during review:
//!
//! 1. **No `max_requests_per_crawl` cap.** The plan's `expected + 16` slack is
//!    unsound against millipede's admission accounting: an admission slot is
//!    reserved per candidate link BEFORE queue dedup
//!    (`millipede-core/src/enqueue.rs`), and reservations from in-flight
//!    batches are only released after the batch resolves. With C = 32
//!    concurrent handlers each enqueueing ~10+ candidates, transient
//!    reservations far exceed 16, so unique URLs near the end of dense
//!    scenarios (e.g. `mesh`) get `SkipReason::MaxRequestsReached` and are
//!    silently dropped — the trial then always fails the exact-count gate.
//!    The synthetic sites are terminal and `SameHostname`-bounded, so an
//!    unbounded crawl is exactly `expected` pages; the server-side gates
//!    (unique hits == expected, duplicates == 0, unknown/off-host == 0)
//!    still catch any runaway.
//! 2. **Truncation is surfaced as a hard error.** The handler inspects each
//!    `EnqueueResult` and counts `MaxRequestsReached` skips; any non-zero
//!    count invalidates the trial loudly instead of relying solely on the
//!    exact-count gate (defense in depth if a cap is ever reintroduced).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use millipede::{
    CrawlPolicy, Crawler, EnqueueStrategy, HtmlContext, HtmlKind, HttpKind, MemoryStorageClient,
    SkipReason,
};

use crate::engines::{Accum, EngineOutcome};
use crate::scenario::{PageWork, TrialSpec};

pub async fn run(
    spec: &TrialSpec,
    concurrency: usize,
    root_url: &str,
) -> anyhow::Result<EngineOutcome> {
    let expected = spec.expected.pages;
    let work = spec.work;
    let accum = Arc::new(Accum::default());
    let truncated = Arc::new(AtomicU64::new(0));

    // Engine construction happens here, INSIDE the timed region (review A-2).
    let http = HttpKind::builder()
        .disable_sessions() // spider has no equivalent session-pool work in this baseline
        .coalesce_in_flight(false)
        .header_generator(false)
        .user_agents(["millipede-bench/1.0"])
        .retry_status_codes([])
        .retry_server_errors(false)
        .session_status_codes([])
        .request_timeout(Duration::from_secs(15))
        .max_redirects(7)
        .build()?;
    let kind = HtmlKind::from_http(http); // real DOM parse; parsed once, shared with handler

    let handler_accum = Arc::clone(&accum);
    let handler_truncated = Arc::clone(&truncated);
    let crawler = Crawler::builder(kind)
        .min_concurrency(concurrency)
        .max_concurrency(concurrency)
        .desired_concurrency(concurrency) // fixed; no autoscale
        .max_request_retries(0)
        .max_session_rotations(0)
        .same_domain_delay(Duration::ZERO)
        .storage_client(Arc::new(MemoryStorageClient::new()))
        // No max_requests_per_crawl: see module docs — the site is terminal
        // and SameHostname-bounded, and any cap small enough to be a "slack"
        // races millipede's pre-dedup admission reservations.
        .crawl_policy(CrawlPolicy::new().strategy(EnqueueStrategy::SameHostname))
        .request_handler(move |ctx: HtmlContext| {
            let accum = Arc::clone(&handler_accum);
            let truncated = Arc::clone(&handler_truncated);
            async move {
                // Raw rows: count + byte-sum + checksum from ctx.response (no
                // extra parse; the DOM was already built by HtmlKind).
                // Extraction rows: the shared selector fn REUSES that DOM.
                accum.record_body(&work, &ctx.response.body, |digest| {
                    if let PageWork::Extract(extract) = work {
                        ctx.html.with_html(|html| extract(html, digest));
                    }
                });
                let result = ctx
                    .enqueue
                    .options()
                    .strategy(EnqueueStrategy::SameHostname)
                    .send()
                    .await?;
                // Surface admission-cap truncation as a hard error instead of
                // silently dropping URLs (module docs, deviation 2).
                let dropped = result
                    .skipped
                    .iter()
                    .filter(|s| matches!(s.reason, SkipReason::MaxRequestsReached { .. }))
                    .count() as u64;
                if dropped > 0 {
                    truncated.fetch_add(dropped, Ordering::AcqRel);
                }
                Ok(())
            }
        })
        .build()
        .await?;

    let stats = crawler.run(root_url.to_owned()).await?;

    let mut errors = Vec::new();
    if stats.requests_finished != expected {
        errors.push(format!(
            "millipede requests_finished {} != expected {expected}",
            stats.requests_finished
        ));
    }
    if stats.requests_failed != 0 {
        errors.push(format!(
            "millipede requests_failed {} != 0",
            stats.requests_failed
        ));
    }
    let dropped = truncated.load(Ordering::Acquire);
    if dropped != 0 {
        errors.push(format!(
            "millipede dropped {dropped} candidate URLs via MaxRequestsReached"
        ));
    }
    // Read through the Arc: the crawler retains its handler closure (and thus
    // an Accum clone) after run(), so Arc::into_inner is never possible here.
    // run() returning with all requests finished means accumulation is done.
    Ok(accum.snapshot(errors))
}
