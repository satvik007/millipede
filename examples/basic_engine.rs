//! Phase 2 exit example: fixed-concurrency engine over 1000 synthetic requests.

use millipede::{BasicContext, BasicKind, CrawlError, Crawler, FailedRequestContext};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let storage = std::sync::Arc::new(millipede::MemoryStorageClient::new());
    let failed_handler_calls = Arc::new(AtomicU64::new(0));

    let handler = |ctx: BasicContext| async move {
        let i = ctx
            .request
            .url
            .path_segments()
            .and_then(Iterator::last)
            .expect("synthetic request URL should have an item index")
            .parse::<u64>()
            .expect("synthetic request item index should be an integer");

        let mut hasher = DefaultHasher::new();
        ctx.request.url.as_str().hash(&mut hasher);
        tokio::time::sleep(Duration::from_millis(hasher.finish() % 5)).await;

        if i % 97 == 0 {
            Err(CrawlError::non_retryable(anyhow::anyhow!(
                "synthetic permanent failure"
            )))
        } else if i % 7 == 0 && ctx.request.retry_count == 0 {
            Err(CrawlError::retry(anyhow::anyhow!(
                "synthetic transient failure"
            )))
        } else {
            Ok(())
        }
    };

    let failed_counter = Arc::clone(&failed_handler_calls);
    let failed = move |_ctx: FailedRequestContext| {
        let failed_counter = Arc::clone(&failed_counter);
        async move {
            failed_counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    };

    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage)
        .max_concurrency(32)
        .max_request_retries(3)
        .request_handler(handler)
        .failed_request_handler(failed)
        .results_capacity(2048)
        .build()
        .await?;

    let mut results = crawler.results();
    let terminal_counter = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(30), async move {
            let mut count = 0;
            while count < 1000 {
                results
                    .recv()
                    .await
                    .expect("terminal result stream closed or lagged");
                count += 1;
            }
            count
        })
        .await
        .expect("timed out waiting for 1000 terminal result snapshots")
    });

    let requests = (0..1000)
        .map(|i| format!("https://example.invalid/item/{i}"))
        .collect::<Vec<String>>();
    let stats = crawler.run(requests).await?;
    let terminal_snapshots_counted = terminal_counter.await?;

    println!(
        "requests_finished={} requests_failed={} requests_retries={} avg_duration={:?} runtime={:?} finished/min={:.2}",
        stats.requests_finished,
        stats.requests_failed,
        stats.requests_retries,
        stats.request_avg_duration,
        stats.crawler_runtime,
        stats.requests_finished_per_minute,
    );

    assert_eq!(stats.requests_finished + stats.requests_failed, 1000);
    assert!(stats.requests_retries > 0, "expected transient retries");
    assert_eq!(stats.requests_failed, 11);
    assert_eq!(failed_handler_calls.load(Ordering::SeqCst), 11);
    assert_eq!(terminal_snapshots_counted, 1000);
    println!(
        "basic_engine OK: 1000 requests, {} retries",
        stats.requests_retries
    );
    Ok(())
}
