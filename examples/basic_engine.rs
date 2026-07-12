//! Runs the Phase 2 fixed-concurrency crawler over synthetic requests.

use millipede::{
    BasicContext, BasicCrawler, BasicKind, CrawlError, FailedRequestContext, MemoryStorageClient,
};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let crawler = BasicCrawler::builder(BasicKind)
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .max_concurrency(16)
        .max_request_retries(2)
        .request_handler(|ctx: BasicContext| async move {
            let id = ctx
                .request
                .url
                .path_segments()
                .and_then(Iterator::last)
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or_default();
            tokio::time::sleep(Duration::from_millis(1 + id % 8)).await;

            if id % 100 == 0 {
                return Err(CrawlError::non_retryable(anyhow::anyhow!(
                    "synthetic permanent failure"
                )));
            }
            if id % 25 == 0 && ctx.request.retry_count == 0 {
                return Err(CrawlError::retry(anyhow::anyhow!(
                    "synthetic transient failure"
                )));
            }
            Ok(())
        })
        .failed_request_handler(|ctx: FailedRequestContext| async move {
            eprintln!("failed {}: {}", ctx.request.url, ctx.error);
            Ok(())
        })
        .build()
        .await?;

    let requests = (0..1_000)
        .map(|id| format!("https://example.com/items/{id}"))
        .collect::<Vec<_>>();
    let stats = crawler.run(requests).await?;

    assert_eq!(stats.requests_finished + stats.requests_failed, 1_000);
    assert!(stats.requests_retries > 0);
    println!(
        "finished={} failed={} retries={}",
        stats.requests_finished, stats.requests_failed, stats.requests_retries
    );
    Ok(())
}
