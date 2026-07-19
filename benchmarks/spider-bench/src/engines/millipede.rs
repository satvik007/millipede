//! Millipede driver — configuration exactly as PLAN.md §5.

use std::sync::Arc;
use std::time::Duration;

use millipede::{
    CrawlPolicy, Crawler, EnqueueStrategy, HtmlContext, HtmlKind, HttpKind, MemoryStorageClient,
};

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
    let crawler = Crawler::builder(kind)
        .min_concurrency(concurrency)
        .max_concurrency(concurrency)
        .desired_concurrency(concurrency) // fixed; no autoscale
        .max_request_retries(0)
        .max_session_rotations(0)
        .same_domain_delay(Duration::ZERO)
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .crawl_policy(
            CrawlPolicy::new()
                .strategy(EnqueueStrategy::SameHostname)
                // Slack; the exact-count gate catches truncation.
                .max_requests_per_crawl(expected + 16),
        )
        .request_handler(move |ctx: HtmlContext| {
            let accum = Arc::clone(&handler_accum);
            async move {
                // Raw rows: count + byte-sum + checksum from ctx.response (no
                // extra parse; the DOM was already built by HtmlKind).
                // Extraction rows: the shared selector fn REUSES that DOM.
                accum.record_body(&work, &ctx.response.body, |digest| {
                    if let PageWork::Extract(extract) = work {
                        ctx.html.with_html(|html| extract(html, digest));
                    }
                });
                let _ = ctx
                    .enqueue
                    .options()
                    .strategy(EnqueueStrategy::SameHostname)
                    .send()
                    .await?;
                Ok(())
            }
        })
        .build()
        .await?;

    let stats = crawler.run(root_url.to_owned()).await?;

    let accum = Arc::into_inner(accum)
        .ok_or_else(|| anyhow::anyhow!("handler still holds the accumulator after run"))?;
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
    Ok(accum.into_outcome(errors))
}
