//! Public crawler construction and execution tests.

use futures_util::future::BoxFuture;
use millipede_core::{
    config::Configuration,
    crawler::{
        BasicContext, BasicKind, Crawler, CrawlerBuildError, CrawlerEnv, CrawlerKind, RequestEnv,
        RequestOutcome,
    },
    errors::CrawlError,
    events::{CrawlerEvent, RequestFinalState},
    handler::FailedRequestContext,
    request::Request,
    router::Router,
    statistics::STATISTICS_PERSIST_KEY,
    storage::{
        AddOptions, BatchAddHandle, Dataset, KeyValueStore, KeyValueStoreExt, Lease, LeaseId,
        ReclaimOptions, RequestQueue, RequestSource, StorageClient, StorageResult,
    },
};
use millipede_storage_memory::{MemoryRequestQueue, MemoryStorageClient};
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

fn storage() -> Arc<dyn StorageClient> {
    Arc::new(MemoryStorageClient::new())
}

#[tokio::test]
async fn run_processes_all_start_requests() -> Result<(), Box<dyn std::error::Error>> {
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler(|_ctx: BasicContext| async { Ok(()) })
        .build()
        .await?;
    let stats = crawler
        .run(
            (0..50)
                .map(|n| format!("https://example.com/{n}"))
                .collect::<Vec<_>>(),
        )
        .await?;
    assert_eq!(stats.requests_finished, 50);
    assert_eq!(stats.requests_failed, 0);
    Ok(())
}

#[tokio::test]
async fn retry_and_failure_end_to_end() -> Result<(), Box<dyn std::error::Error>> {
    let failures = Arc::new(AtomicUsize::new(0));
    let seen = failures.clone();
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler(|ctx: BasicContext| async move {
            let path = ctx.request.url.path();
            if path.ends_with("/fail") {
                Err(CrawlError::non_retryable(anyhow::anyhow!("failed")))
            } else if path.ends_with("/flaky") && ctx.request.retry_count == 0 {
                Err(CrawlError::retry(anyhow::anyhow!("flaky")))
            } else {
                Ok(())
            }
        })
        .failed_request_handler(move |_ctx: FailedRequestContext| {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .build()
        .await?;
    let stats = crawler
        .run([
            "https://example.com/ok",
            "https://example.com/fail",
            "https://example.com/flaky",
        ])
        .await?;
    assert_eq!(stats.requests_finished + stats.requests_failed, 3);
    assert_eq!(stats.requests_failed, 1);
    assert_eq!(failures.load(Ordering::SeqCst), 1);
    assert!(stats.requests_retries > 0);
    Ok(())
}

#[tokio::test]
async fn router_with_labels_and_missing_route() -> Result<(), Box<dyn std::error::Error>> {
    let missing = Arc::new(AtomicBool::new(false));
    let observed = missing.clone();
    let router = Router::<BasicContext>::new().route("a", |_ctx| async { Ok(()) });
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler(router)
        .failed_request_handler(move |ctx: FailedRequestContext| {
            let observed = observed.clone();
            async move {
                observed.store(
                    ctx.error.to_string().contains("missing route"),
                    Ordering::SeqCst,
                );
                Ok(())
            }
        })
        .build()
        .await?;
    let labeled = Request::get("https://example.com/a").label("a").build()?;
    let unlabeled = Request::get("https://example.com/missing").build()?;
    let stats = crawler.run(vec![labeled, unlabeled]).await?;
    assert_eq!(stats.requests_finished, 1);
    assert_eq!(stats.requests_failed, 1);
    assert!(missing.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn stop_drains_and_abort_cancels() -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let started = Arc::new(AtomicUsize::new(0));
        let count = started.clone();
        let crawler = Arc::new(
            Crawler::builder(BasicKind)
                .storage_client(storage())
                .max_concurrency(3)
                .request_handler(move |_ctx: BasicContext| {
                    let count = count.clone();
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok(())
                    }
                })
                .build()
                .await?,
        );
        let run_crawler = crawler.clone();
        let run = tokio::spawn(async move {
            run_crawler
                .run(
                    (0..30)
                        .map(|n| format!("https://example.com/{n}"))
                        .collect::<Vec<_>>(),
                )
                .await
        });
        while started.load(Ordering::SeqCst) < 3 {
            tokio::task::yield_now().await;
        }
        let started_at_stop = started.load(Ordering::SeqCst);
        crawler.stop();
        let stats = run.await??;
        assert!(stats.requests_finished >= started_at_stop as u64);
        assert!(stats.requests_finished < 30);
        assert_eq!(stats.requests_failed, 0);

        let started = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(tokio::sync::Notify::new());
        let count = started.clone();
        let signal = notify.clone();
        let crawler = Arc::new(
            Crawler::builder(BasicKind)
                .storage_client(storage())
                .max_concurrency(3)
                .request_handler(move |_ctx: BasicContext| {
                    let count = count.clone();
                    let signal = signal.clone();
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        signal.notify_one();
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        Ok(())
                    }
                })
                .build()
                .await?,
        );
        let run_crawler = crawler.clone();
        let run = tokio::spawn(async move {
            run_crawler
                .run(
                    (0..30)
                        .map(|n| format!("https://example.com/{n}"))
                        .collect::<Vec<_>>(),
                )
                .await
        });
        while started.load(Ordering::SeqCst) < 3 {
            notify.notified().await;
        }
        crawler.abort();
        let stats = run.await??;
        assert_eq!(stats.requests_finished, 0);
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn results_stream_via_public_api() -> Result<(), Box<dyn std::error::Error>> {
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler(|_ctx: BasicContext| async { Ok(()) })
        .build()
        .await?;
    let mut results = crawler.results();
    let first = Request::get("https://example.com/first").build()?;
    let second = Request::get("https://example.com/second").build()?;
    let expected = HashMap::from([
        (first.id.clone(), first.url.clone()),
        (second.id.clone(), second.url.clone()),
    ]);
    crawler.run([first, second]).await?;

    let mut received = HashMap::new();
    for _ in 0..expected.len() {
        let result = tokio::time::timeout(Duration::from_secs(1), results.recv()).await??;
        assert_eq!(result.outcome, RequestFinalState::Succeeded);
        assert_eq!(expected.get(&result.request.id), Some(&result.request.url));
        assert!(
            received
                .insert(result.request.id.clone(), result.request.url.clone())
                .is_none(),
            "received more than one terminal result for a request"
        );
    }
    assert_eq!(received, expected);
    assert!(results.try_recv().is_err());
    Ok(())
}

struct DeferredQueue {
    inner: Arc<MemoryRequestQueue>,
    add_calls: AtomicUsize,
    completion_pending: Arc<AtomicBool>,
    deferred_returned: Arc<tokio::sync::Notify>,
    complete_deferred: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl RequestQueue for DeferredQueue {
    async fn add(
        &self,
        request: Request,
        options: AddOptions,
    ) -> StorageResult<millipede_core::storage::QueueOpInfo> {
        self.inner.add(request, options).await
    }

    async fn add_batch(
        &self,
        requests: Vec<RequestSource>,
        options: AddOptions,
    ) -> StorageResult<BatchAddHandle> {
        if self.add_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return self.inner.add_batch(requests, options).await;
        }

        self.completion_pending.store(true, Ordering::SeqCst);
        let inner = self.inner.clone();
        let pending = self.completion_pending.clone();
        let complete = self.complete_deferred.clone();
        let task = tokio::spawn(async move {
            complete.notified().await;
            let result = inner.add_batch(requests, options).await?.wait().await;
            pending.store(false, Ordering::SeqCst);
            result
        });
        self.deferred_returned.notify_one();
        Ok(BatchAddHandle::deferred(Vec::new(), task))
    }

    async fn fetch_next(&self) -> StorageResult<Option<Lease>> {
        self.inner.fetch_next().await
    }
    async fn mark_handled(&self, lease: Lease) -> StorageResult<()> {
        self.inner.mark_handled(lease).await
    }
    async fn reclaim(&self, lease: Lease, options: ReclaimOptions) -> StorageResult<()> {
        self.inner.reclaim(lease, options).await
    }
    async fn renew(&self, lease_id: &LeaseId, extend_by: Duration) -> StorageResult<()> {
        self.inner.renew(lease_id, extend_by).await
    }
    async fn abandon(&self, lease: Lease) -> StorageResult<()> {
        self.inner.abandon(lease).await
    }
    async fn is_empty(&self) -> StorageResult<bool> {
        self.inner.is_empty().await
    }
    async fn is_finished(&self) -> StorageResult<bool> {
        Ok(!self.completion_pending.load(Ordering::SeqCst) && self.inner.is_finished().await?)
    }
    async fn handled_count(&self) -> StorageResult<u64> {
        self.inner.handled_count().await
    }
    async fn pending_count(&self) -> StorageResult<u64> {
        self.inner.pending_count().await
    }
}

struct DeferredStorage {
    inner: MemoryStorageClient,
    queue: Arc<DeferredQueue>,
}

#[async_trait::async_trait]
impl StorageClient for DeferredStorage {
    async fn open_dataset(&self, name: Option<&str>) -> StorageResult<Arc<dyn Dataset>> {
        self.inner.open_dataset(name).await
    }
    async fn open_key_value_store(
        &self,
        name: Option<&str>,
    ) -> StorageResult<Arc<dyn KeyValueStore>> {
        self.inner.open_key_value_store(name).await
    }
    async fn open_request_queue(&self, _: Option<&str>) -> StorageResult<Arc<dyn RequestQueue>> {
        Ok(self.queue.clone())
    }
    async fn purge(&self) -> StorageResult<()> {
        self.inner.purge().await
    }
}

#[tokio::test]
async fn crawler_add_requests_wakes_engine_after_deferred_completion()
-> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let queue = Arc::new(DeferredQueue {
            inner: Arc::new(MemoryRequestQueue::new("deferred")),
            add_calls: AtomicUsize::new(0),
            completion_pending: Arc::new(AtomicBool::new(false)),
            deferred_returned: Arc::new(tokio::sync::Notify::new()),
            complete_deferred: Arc::new(tokio::sync::Notify::new()),
        });
        let storage: Arc<dyn StorageClient> = Arc::new(DeferredStorage {
            inner: MemoryStorageClient::new(),
            queue: queue.clone(),
        });
        let initial_started = Arc::new(tokio::sync::Notify::new());
        let release_initial = Arc::new(tokio::sync::Notify::new());
        let crawler = Arc::new(
            Crawler::builder(BasicKind)
                .storage_client(storage)
                .request_handler({
                    let initial_started = initial_started.clone();
                    let release_initial = release_initial.clone();
                    move |ctx: BasicContext| {
                        let initial_started = initial_started.clone();
                        let release_initial = release_initial.clone();
                        async move {
                            if ctx.request.url.path() == "/initial" {
                                initial_started.notify_one();
                                release_initial.notified().await;
                            }
                            Ok(())
                        }
                    }
                })
                .build()
                .await?,
        );

        let running = {
            let crawler = crawler.clone();
            tokio::spawn(async move { crawler.run(["https://example.com/initial"]).await })
        };
        initial_started.notified().await;
        let adding = {
            let crawler = crawler.clone();
            tokio::spawn(async move {
                crawler
                    .add_requests([Request::get("https://example.com/deferred").build()?])
                    .await
            })
        };
        queue.deferred_returned.notified().await;
        release_initial.notify_one();
        tokio::task::yield_now().await;
        queue.complete_deferred.notify_one();

        adding.await??;
        let stats = running.await??;
        assert_eq!(stats.requests_finished, 2);
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn config_events_and_ids_are_honored() -> Result<(), Box<dyn std::error::Error>> {
    let storage: Arc<dyn StorageClient> = Arc::new(MemoryStorageClient::new());
    let config = Configuration::builder()
        .default_request_queue_id("my-queue")
        .default_key_value_store_id("my-kvs")
        .persist_state_interval(Duration::from_millis(25))
        .purge_on_start(false)
        .storage_client(storage.clone())
        .build()?;
    let mut events = config.events().subscribe();
    let crawler = Crawler::builder(BasicKind)
        .configuration(config)
        .request_handler(|_ctx: BasicContext| async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            Ok(())
        })
        .build()
        .await?;
    crawler
        .run(["https://example.com/1", "https://example.com/2"])
        .await?;
    let mut saw = false;
    while let Ok(event) = events.try_recv() {
        saw |= matches!(
            event,
            CrawlerEvent::PersistState { .. } | CrawlerEvent::RequestFinished(_)
        );
    }
    assert!(saw);
    assert!(
        storage
            .open_key_value_store(Some("my-kvs"))
            .await?
            .get::<serde_json::Value>(STATISTICS_PERSIST_KEY)
            .await?
            .is_some()
    );
    assert!(
        storage
            .open_key_value_store(None)
            .await?
            .get::<serde_json::Value>(STATISTICS_PERSIST_KEY)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn purge_on_start_purges() -> Result<(), Box<dyn std::error::Error>> {
    for (purge, survives) in [(true, false), (false, true)] {
        let storage: Arc<dyn StorageClient> = Arc::new(MemoryStorageClient::new());
        storage
            .open_key_value_store(None)
            .await?
            .set("marker", &true)
            .await?;
        let config = Configuration::builder().purge_on_start(purge).build()?;
        let _crawler = Crawler::builder(BasicKind)
            .configuration(config)
            .storage_client(storage.clone())
            .request_handler(|_ctx: BasicContext| async { Ok(()) })
            .build()
            .await?;
        assert_eq!(
            storage
                .open_key_value_store(None)
                .await?
                .get::<bool>("marker")
                .await?
                .is_some(),
            survives
        );
    }
    Ok(())
}

#[tokio::test]
async fn builder_validation() -> Result<(), Box<dyn std::error::Error>> {
    assert!(matches!(
        Crawler::builder(BasicKind)
            .storage_client(storage())
            .build()
            .await,
        Err(CrawlerBuildError::MissingRequestHandler)
    ));
    let config = Configuration::builder().purge_on_start(false).build()?;
    assert!(matches!(
        Crawler::builder(BasicKind)
            .configuration(config)
            .request_handler(|_ctx: BasicContext| async { Ok(()) })
            .build()
            .await,
        Err(CrawlerBuildError::MissingStorage)
    ));
    assert!(matches!(
        Crawler::builder(BasicKind)
            .storage_client(storage())
            .max_concurrency(0)
            .request_handler(|_ctx: BasicContext| async { Ok(()) })
            .build()
            .await,
        Err(CrawlerBuildError::ZeroMaxConcurrency)
    ));
    assert!(matches!(
        Crawler::builder(BasicKind)
            .storage_client(storage())
            .results_capacity(0)
            .request_handler(|_ctx: BasicContext| async { Ok(()) })
            .build()
            .await,
        Err(CrawlerBuildError::ZeroResultsCapacity)
    ));
    Ok(())
}

#[tokio::test]
async fn run_twice_errors() -> Result<(), Box<dyn std::error::Error>> {
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler(|_ctx: BasicContext| async { Ok(()) })
        .build()
        .await?;
    crawler.run(["https://example.com"]).await?;
    assert!(
        crawler
            .run(["https://example.com"])
            .await
            .unwrap_err()
            .to_string()
            .contains("already been run")
    );
    Ok(())
}

struct LifecycleKind {
    start_fails: bool,
    stop_called: Arc<AtomicBool>,
}
#[derive(Clone)]
struct LifecycleContext;
impl CrawlerKind for LifecycleKind {
    type Context = LifecycleContext;
    fn start<'a>(&'a self, _env: &'a CrawlerEnv) -> BoxFuture<'a, Result<(), CrawlError>> {
        Box::pin(async move {
            if self.start_fails {
                Err(CrawlError::critical(anyhow::anyhow!("start")))
            } else {
                Ok(())
            }
        })
    }
    fn execute<'a>(
        &'a self,
        env: RequestEnv<'a>,
    ) -> BoxFuture<'a, Result<Self::Context, CrawlError>> {
        let _ = env;
        Box::pin(async move { Ok(LifecycleContext) })
    }
    fn cleanup(
        &self,
        _outcome: RequestOutcome<Self::Context>,
    ) -> BoxFuture<'_, Result<(), CrawlError>> {
        Box::pin(async { Ok(()) })
    }
    fn stop<'a>(&'a self, _env: &'a CrawlerEnv) -> BoxFuture<'a, Result<(), CrawlError>> {
        Box::pin(async move {
            self.stop_called.store(true, Ordering::SeqCst);
            Ok(())
        })
    }
}

#[tokio::test]
async fn kind_stop_lifecycle_balance() -> Result<(), Box<dyn std::error::Error>> {
    let stopped = Arc::new(AtomicBool::new(false));
    let crawler = Crawler::builder(LifecycleKind {
        start_fails: false,
        stop_called: stopped.clone(),
    })
    .storage_client(storage())
    .request_handler(|_ctx: LifecycleContext| async {
        Err(CrawlError::critical(anyhow::anyhow!("engine")))
    })
    .build()
    .await?;
    assert!(crawler.run(["https://example.com"]).await.is_err());
    assert!(stopped.load(Ordering::SeqCst));
    let stopped = Arc::new(AtomicBool::new(false));
    let crawler = Crawler::builder(LifecycleKind {
        start_fails: true,
        stop_called: stopped.clone(),
    })
    .storage_client(storage())
    .request_handler(|_ctx: LifecycleContext| async { Ok(()) })
    .build()
    .await?;
    assert!(crawler.run(["https://example.com"]).await.is_err());
    assert!(!stopped.load(Ordering::SeqCst));
    Ok(())
}
