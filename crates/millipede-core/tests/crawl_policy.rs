//! Crawl-policy integration across enqueueing and engine scheduling.

use millipede_core::{
    crawler::{BasicContext, BasicKind, Crawler},
    enqueue::{EnqueueLinker, SkipReason},
    link_extraction::{CrawlPolicy, EnqueueStrategy, TransformResult},
    prelude::{Request, StorageClient},
};
use millipede_storage_memory::MemoryStorageClient;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

fn storage() -> Arc<dyn StorageClient> {
    Arc::new(MemoryStorageClient::new())
}

#[tokio::test]
async fn skipped_handler_observes_mixed_pipeline_reasons() -> Result<(), Box<dyn std::error::Error>>
{
    let reasons = Arc::new(Mutex::new(Vec::new()));
    let policy = CrawlPolicy::new()
        .strategy(EnqueueStrategy::SameHostname)
        .on_skipped({
            let reasons = reasons.clone();
            move |_url: &str, reason: &SkipReason| reasons.lock().unwrap().push(reason.clone())
        });
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .crawl_policy(policy)
        .request_handler(|ctx: BasicContext| async move {
            if ctx.request.url.path() == "/root" {
                EnqueueLinker::new(ctx.crawler.clone(), &ctx.request)
                    .options()
                    .raw_urls(["::bad::", "https://outside.test/a", "/reject", "/ok"])
                    .transform(|request| {
                        Box::pin(async move {
                            if request.url.path() == "/reject" {
                                TransformResult::Skip {
                                    reason: "policy test".into(),
                                }
                            } else {
                                TransformResult::Enqueue
                            }
                        })
                    })
                    .send()
                    .await?;
            }
            Ok(())
        })
        .build()
        .await?;
    crawler.run(["http://example.local/root"]).await?;
    let reasons = reasons.lock().unwrap();
    assert_eq!(reasons.len(), 3);
    assert!(reasons.contains(&SkipReason::InvalidUrl));
    assert!(reasons.contains(&SkipReason::StrategyExcluded));
    assert!(
        reasons
            .iter()
            .any(|reason| matches!(reason, SkipReason::TransformRejected { .. }))
    );
    Ok(())
}

#[tokio::test]
async fn engine_stops_at_max_requests_without_hanging() -> Result<(), Box<dyn std::error::Error>> {
    let handled = Arc::new(AtomicUsize::new(0));
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .max_concurrency(10)
        .crawl_policy(CrawlPolicy::new().max_requests_per_crawl(3))
        .request_handler({
            let handled = handled.clone();
            move |_ctx: BasicContext| {
                let handled = handled.clone();
                async move {
                    handled.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        })
        .build()
        .await?;
    let starts: Vec<_> = (0..10)
        .map(|index| format!("http://example.local/{index}"))
        .collect();
    let stats =
        tokio::time::timeout(std::time::Duration::from_secs(5), crawler.run(starts)).await??;
    assert!(stats.requests_finished <= 3);
    assert!(handled.load(Ordering::SeqCst) <= 3);
    Ok(())
}

#[tokio::test]
async fn enqueue_reports_max_requests_when_queue_has_reached_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let callback_count = Arc::new(AtomicUsize::new(0));
    let transform_count = Arc::new(AtomicUsize::new(0));
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .crawl_policy(CrawlPolicy::new().max_requests_per_crawl(1).on_skipped({
            let callback_count = callback_count.clone();
            move |_url: &str, reason: &SkipReason| {
                if matches!(reason, SkipReason::MaxRequestsReached { .. }) {
                    callback_count.fetch_add(1, Ordering::SeqCst);
                }
            }
        }))
        .request_handler(|_: BasicContext| async { Ok(()) })
        .build()
        .await?;
    crawler
        .add_requests([Request::get("http://example.local/already").build()?])
        .await?;
    let parent = Request::get("http://example.local/root").build()?;
    let result = EnqueueLinker::new(crawler.handle(), &parent)
        .options()
        .raw_urls(["/blocked"])
        .transform({
            let transform_count = transform_count.clone();
            move |_request| {
                transform_count.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { TransformResult::Enqueue })
            }
        })
        .send()
        .await?;
    assert_eq!(result.added_count(), 0);
    assert_eq!(
        result.skipped[0].reason,
        SkipReason::MaxRequestsReached { limit: 1 }
    );
    assert_eq!(callback_count.load(Ordering::SeqCst), 1);
    assert_eq!(transform_count.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn concurrent_enqueue_calls_share_max_request_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .crawl_policy(CrawlPolicy::new().max_requests_per_crawl(1))
        .request_handler(|_: BasicContext| async { Ok(()) })
        .build()
        .await?;
    let parent = Request::get("http://example.local/root").build()?;
    let first = EnqueueLinker::new(crawler.handle(), &parent);
    let second = EnqueueLinker::new(crawler.handle(), &parent);

    let (first, second) = tokio::join!(
        first.options().raw_urls(["/first"]).send(),
        second.options().raw_urls(["/second"]).send(),
    );
    let results = [first?, second?];

    assert_eq!(
        results
            .iter()
            .map(|result| result.added_count())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        results
            .iter()
            .flat_map(|result| &result.skipped)
            .filter(|skip| skip.reason == SkipReason::MaxRequestsReached { limit: 1 })
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn transform_can_reentrantly_enqueue_under_max_request_policy()
-> Result<(), Box<dyn std::error::Error>> {
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .crawl_policy(CrawlPolicy::new().max_requests_per_crawl(2))
        .request_handler(|_: BasicContext| async { Ok(()) })
        .build()
        .await?;
    let parent = Request::get("http://example.local/root").build()?;
    let nested = EnqueueLinker::new(crawler.handle(), &parent);

    let outer_linker = EnqueueLinker::new(crawler.handle(), &parent);
    let outer = outer_linker
        .options()
        .raw_urls(["/outer"])
        .transform(move |_request| {
            let nested = nested.clone();
            Box::pin(async move {
                let result = nested
                    .options()
                    .raw_urls(["/nested"])
                    .send()
                    .await
                    .expect("reentrant enqueue should complete");
                assert_eq!(result.added_count(), 1);
                TransformResult::Enqueue
            })
        })
        .send();

    let result = tokio::time::timeout(std::time::Duration::from_secs(2), outer).await??;
    assert_eq!(result.added_count(), 1);
    let queue = crawler
        .handle()
        .request_queue()
        .expect("crawler queue should remain available");
    assert_eq!(queue.pending_count().await?, 2);
    Ok(())
}
