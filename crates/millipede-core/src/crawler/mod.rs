//! The crawler engine: lifecycle kinds, handles, and shared state.

mod basic;
mod builder;
mod engine;
mod start;

pub use basic::{BasicContext, BasicKind};
pub use builder::{CrawlerBuildError, CrawlerBuilder};
pub use start::{IntoStartRequest, IntoStartRequests};

use crate::{
    config::Configuration,
    errors::CrawlError,
    events::{EventBus, EventStream, HandledRequest, ResultStream},
    handler::{FailedRequestHandler, RequestHandler},
    request::Request,
    statistics::{FinalStatistics, StatisticsHandle, StatisticsSnapshot},
    storage::{AddOptions, BatchAddHandle, RequestQueue, RequestSource},
};
use futures_util::future::BoxFuture;
use std::{
    fmt,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use engine::{Engine, EngineOptions};

/// A configured crawler using lifecycle behavior supplied by `K`.
pub struct Crawler<K: CrawlerKind> {
    kind: Arc<K>,
    shared: Arc<CrawlerShared>,
    config: Arc<Configuration>,
    handler: Arc<dyn RequestHandler<K::Context>>,
    failed_handler: Option<Arc<dyn FailedRequestHandler>>,
    kvs: Option<Arc<dyn crate::storage::KeyValueStore>>,
    opts: EngineOptions,
    started: AtomicBool,
}

/// The no-HTTP crawler: drives the queue and hands requests straight to the handler.
pub type BasicCrawler = Crawler<BasicKind>;

impl<K: CrawlerKind> Crawler<K> {
    /// Starts building a crawler around the given kind.
    pub fn builder(kind: K) -> CrawlerBuilder<K> {
        CrawlerBuilder::new(kind)
    }

    /// Runs the crawl to completion.
    ///
    /// A crawler runs at most once; a second call returns a non-retryable error.
    pub async fn run(&self, start: impl IntoStartRequests) -> Result<FinalStatistics, CrawlError> {
        if self.started.swap(true, Ordering::SeqCst) {
            return Err(CrawlError::non_retryable(anyhow::anyhow!(
                "this crawler has already been run"
            )));
        }
        let start_requests = start.into_start_requests()?;
        let env = CrawlerEnv {
            shared: self.shared.clone(),
            config: self.config.clone(),
        };
        self.kind.start(&env).await?;
        let result = async {
            let sources = start_requests
                .into_iter()
                .map(RequestSource::from)
                .collect();
            let batch = tokio::time::timeout(
                self.opts.internal_operation_timeout,
                self.shared.queue.add_batch(sources, AddOptions::default()),
            )
            .await
            .map_err(|_| CrawlError::retry(anyhow::anyhow!("queue add timed out")))??;
            batch.wait().await?;
            self.shared.notify.notify_waiters();
            Engine {
                kind: self.kind.clone(),
                handler: self.handler.clone(),
                failed_handler: self.failed_handler.clone(),
                shared: self.shared.clone(),
                kvs: self.kvs.clone(),
                opts: self.opts.clone(),
            }
            .run()
            .await
        }
        .await;
        if let Err(error) = self.kind.stop(&env).await {
            tracing::warn!(%error, "crawler kind stop failed");
        }
        result
    }

    /// Creates a weak handle to this crawler.
    pub fn handle(&self) -> CrawlerHandle {
        CrawlerHandle::new(Arc::downgrade(&self.shared))
    }
    /// Adds requests and waits until the complete batch has been accepted.
    pub async fn add_requests(
        &self,
        reqs: impl IntoIterator<Item = Request> + Send,
    ) -> Result<(), CrawlError> {
        self.handle().add_requests(reqs).await?.wait().await?;
        Ok(())
    }
    /// Subscribes to terminal request snapshots.
    pub fn results(&self) -> ResultStream {
        self.shared.results_tx.subscribe()
    }
    /// Subscribes to control-plane crawler events.
    pub fn events(&self) -> EventStream {
        self.shared.events.subscribe()
    }
    /// Returns the live statistics handle.
    pub fn stats(&self) -> StatisticsHandle {
        self.shared.stats.clone()
    }
    /// Signals a graceful drain.
    pub fn stop(&self) {
        self.handle().stop();
    }
    /// Signals immediate cancellation.
    pub fn abort(&self) {
        self.handle().abort();
    }
}

pub(crate) struct CrawlerShared {
    pub(crate) queue: Arc<dyn RequestQueue>,
    pub(crate) stats: StatisticsHandle,
    pub(crate) events: EventBus,
    pub(crate) results_tx: tokio::sync::broadcast::Sender<HandledRequest>,
    pub(crate) drain: tokio_util::sync::CancellationToken,
    pub(crate) cancel: tokio_util::sync::CancellationToken,
    pub(crate) notify: tokio::sync::Notify,
    pub(crate) internal_operation_timeout: Duration,
}

impl CrawlerShared {
    /// Creates shared crawler state with fresh statistics, result, and cancellation channels.
    ///
    /// `results_capacity` must be at least one. The crawler builder validates this before
    /// constructing shared state.
    #[allow(dead_code)]
    pub(crate) fn new(
        queue: Arc<dyn RequestQueue>,
        events: EventBus,
        results_capacity: usize,
        internal_operation_timeout: Duration,
    ) -> Self {
        debug_assert!(results_capacity >= 1);
        let (results_tx, _) = tokio::sync::broadcast::channel(results_capacity);
        Self {
            queue,
            stats: StatisticsHandle::new(),
            events,
            results_tx,
            drain: tokio_util::sync::CancellationToken::new(),
            cancel: tokio_util::sync::CancellationToken::new(),
            notify: tokio::sync::Notify::new(),
            internal_operation_timeout,
        }
    }
}

/// A cheaply cloned weak back-reference to a running crawler.
#[derive(Clone)]
pub struct CrawlerHandle {
    inner: Weak<CrawlerShared>,
}

impl fmt::Debug for CrawlerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CrawlerHandle")
            .field("alive", &(self.inner.strong_count() > 0))
            .finish()
    }
}

impl CrawlerHandle {
    pub(crate) fn new(inner: Weak<CrawlerShared>) -> Self {
        Self { inner }
    }

    /// Adds requests to the crawler's queue.
    pub async fn add_requests(
        &self,
        reqs: impl IntoIterator<Item = Request> + Send,
    ) -> Result<BatchAddHandle, CrawlError> {
        let shared = self.inner.upgrade().ok_or_else(|| {
            CrawlError::non_retryable(anyhow::anyhow!("crawler is no longer running"))
        })?;
        let sources = reqs.into_iter().map(RequestSource::from).collect();
        let handle = tokio::time::timeout(
            shared.internal_operation_timeout,
            shared.queue.add_batch(sources, AddOptions::default()),
        )
        .await
        .map_err(|_| CrawlError::retry(anyhow::anyhow!("queue add timed out")))??;
        let handle = handle.notify_on_completion({
            let shared = shared.clone();
            move || shared.notify.notify_waiters()
        });
        Ok(handle)
    }

    /// Returns a snapshot of live crawl statistics while the crawler exists.
    pub fn stats(&self) -> Option<StatisticsSnapshot> {
        self.inner.upgrade().map(|shared| shared.stats.snapshot())
    }

    /// Subscribes to control-plane crawler events while the crawler exists.
    pub fn events(&self) -> Option<EventStream> {
        self.inner.upgrade().map(|shared| shared.events.subscribe())
    }

    /// Subscribes to terminal request snapshots while the crawler exists.
    pub fn results(&self) -> Option<crate::events::ResultStream> {
        self.inner
            .upgrade()
            .map(|shared| shared.results_tx.subscribe())
    }

    /// Requests a graceful stop that finishes in-flight work and fetches no more requests.
    pub fn stop(&self) {
        if let Some(shared) = self.inner.upgrade() {
            shared.drain.cancel();
            shared.notify.notify_waiters();
        }
    }

    /// Requests immediate cancellation of crawler work.
    pub fn abort(&self) {
        if let Some(shared) = self.inner.upgrade() {
            shared.cancel.cancel();
            shared.notify.notify_waiters();
        }
    }
}

/// Shared process-level state supplied to crawler lifecycle hooks.
pub struct CrawlerEnv {
    pub(crate) shared: Arc<CrawlerShared>,
    pub(crate) config: Arc<Configuration>,
}

impl CrawlerEnv {
    /// Returns the crawler's adopted event bus.
    pub fn events(&self) -> &EventBus {
        &self.shared.events
    }

    /// Returns the live statistics handle.
    pub fn stats(&self) -> &StatisticsHandle {
        &self.shared.stats
    }

    /// Returns the resolved crawler configuration.
    pub fn config(&self) -> &Configuration {
        &self.config
    }

    /// Creates a weak handle to the crawler.
    pub fn handle(&self) -> CrawlerHandle {
        CrawlerHandle::new(Arc::downgrade(&self.shared))
    }
}

/// Engine-owned scratch space passed to [`CrawlerKind::before_request`].
#[non_exhaustive]
pub struct RequestPrep {
    /// The request being prepared for its next attempt.
    pub request: Request,
}

/// Per-attempt inputs supplied to [`CrawlerKind::execute`].
#[non_exhaustive]
pub struct RequestEnv<'a> {
    /// The request being executed.
    pub request: Arc<Request>,
    /// A weak back-reference to the running crawler.
    pub crawler: CrawlerHandle,
    /// The crawler's event bus.
    pub events: &'a EventBus,
}

/// The outcome supplied to per-attempt cleanup.
pub enum RequestOutcome<C> {
    /// Execution and the user handler succeeded.
    Handled(C),
    /// The user handler failed after context creation.
    HandlerFailed {
        /// The context returned by execution.
        ctx: C,
        /// The handler error, shared with the failure handler.
        error: Arc<CrawlError>,
    },
    /// Request preparation or execution failed before a handler completed.
    ExecuteFailed {
        /// The request whose attempt failed.
        request: Arc<Request>,
        /// The execution error, shared with the failure handler.
        error: Arc<CrawlError>,
    },
}

/// Defines the complete lifecycle for one crawler flavor.
pub trait CrawlerKind: Send + Sync + 'static {
    /// The context passed to user handlers and lifecycle hooks.
    ///
    /// A context must be a cheap aliasing handle over shared state, typically through `Arc`-backed
    /// fields. Clones must observe the same underlying resources so mutations through a handler's
    /// clone remain visible to `after_success` and `cleanup`; plain-value contexts that diverge on
    /// clone violate this contract. `Clone` is required because the handler consumes an owned
    /// context while `after_success` and `cleanup` still need it.
    type Context: Send + Clone + 'static;

    /// Runs once before the crawler fetches any request.
    fn start<'a>(&'a self, env: &'a CrawlerEnv) -> BoxFuture<'a, Result<(), CrawlError>> {
        let _ = env;
        Box::pin(async { Ok(()) })
    }

    /// Mutates a request before an attempt executes.
    fn before_request<'a>(
        &'a self,
        prep: &'a mut RequestPrep,
    ) -> BoxFuture<'a, Result<(), CrawlError>> {
        let _ = prep;
        Box::pin(async { Ok(()) })
    }

    /// Executes one request attempt and constructs its handler context.
    fn execute<'a>(
        &'a self,
        env: RequestEnv<'a>,
    ) -> BoxFuture<'a, Result<Self::Context, CrawlError>>;

    /// Runs after the user handler succeeds.
    fn after_success<'a>(
        &'a self,
        ctx: &'a mut Self::Context,
    ) -> BoxFuture<'a, Result<(), CrawlError>> {
        let _ = ctx;
        Box::pin(async { Ok(()) })
    }

    /// Runs after every attempt concludes, regardless of its outcome.
    fn cleanup(
        &self,
        outcome: RequestOutcome<Self::Context>,
    ) -> BoxFuture<'_, Result<(), CrawlError>>;

    /// Runs once when crawler shutdown begins.
    fn stop<'a>(&'a self, env: &'a CrawlerEnv) -> BoxFuture<'a, Result<(), CrawlError>> {
        let _ = env;
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Lease, LeaseId, ProcessedRequest, ReclaimOptions, StorageResult};
    use std::sync::Mutex;

    #[derive(Default)]
    struct TestQueue(Mutex<Vec<Request>>);

    #[async_trait::async_trait]
    impl RequestQueue for TestQueue {
        async fn add(&self, request: Request, _: AddOptions) -> StorageResult<ProcessedRequest> {
            let mut requests = self.0.lock().unwrap();
            let duplicate = requests
                .iter()
                .any(|known| known.unique_key == request.unique_key);
            let info = ProcessedRequest {
                request_id: request.id.clone(),
                unique_key: request.unique_key.clone(),
                was_already_present: duplicate,
                was_already_handled: false,
            };
            if !duplicate {
                requests.push(request);
            }
            Ok(info)
        }

        async fn add_batch(
            &self,
            requests: Vec<RequestSource>,
            options: AddOptions,
        ) -> StorageResult<BatchAddHandle> {
            let mut added = Vec::with_capacity(requests.len());
            for source in requests {
                let RequestSource::Request(request) = source;
                added.push(self.add(request, options.clone()).await?);
            }
            Ok(BatchAddHandle::ready(added))
        }

        async fn fetch_next(&self) -> StorageResult<Option<Lease>> {
            Ok(None)
        }
        async fn mark_handled(&self, _: Lease) -> StorageResult<()> {
            Ok(())
        }
        async fn reclaim(&self, _: Lease, _: ReclaimOptions) -> StorageResult<()> {
            Ok(())
        }
        async fn renew(&self, _: &LeaseId, _: Duration) -> StorageResult<()> {
            Ok(())
        }
        async fn abandon(&self, _: Lease) -> StorageResult<()> {
            Ok(())
        }
        async fn is_empty(&self) -> StorageResult<bool> {
            Ok(self.0.lock().unwrap().is_empty())
        }
        async fn is_finished(&self) -> StorageResult<bool> {
            self.is_empty().await
        }
        async fn handled_count(&self) -> StorageResult<u64> {
            Ok(0)
        }
        async fn pending_count(&self) -> StorageResult<u64> {
            Ok(self.0.lock().unwrap().len() as u64)
        }
    }

    pub(super) fn shared() -> Arc<CrawlerShared> {
        let queue = Arc::new(TestQueue::default());
        Arc::new(CrawlerShared::new(
            queue,
            EventBus::default(),
            8,
            Duration::from_secs(1),
        ))
    }

    #[tokio::test]
    async fn crawler_handle_adds_deduplicated_requests_and_observes_liveness() {
        let shared = shared();
        let queue = shared.queue.clone();
        let handle = CrawlerHandle::new(Arc::downgrade(&shared));
        let request = Request::get("https://example.com/item").build().unwrap();
        let batch = handle
            .add_requests([request.clone(), request])
            .await
            .unwrap();
        assert_eq!(batch.added.len(), 2);
        assert!(!batch.added[0].was_already_present);
        assert!(batch.added[1].was_already_present);
        assert_eq!(batch.wait().await.unwrap().processed.len(), 2);
        assert_eq!(queue.pending_count().await.unwrap(), 1);
        assert!(handle.stats().is_some());
        assert!(handle.events().is_some());
        assert!(handle.results().is_some());
        assert_eq!(format!("{handle:?}"), "CrawlerHandle { alive: true }");

        drop(shared);
        assert!(handle.add_requests(Vec::new()).await.is_err());
        assert!(handle.stats().is_none());
        assert_eq!(format!("{handle:?}"), "CrawlerHandle { alive: false }");
    }

    #[tokio::test]
    async fn crawler_handle_stop_and_abort_cancel_their_tokens() {
        let shared = shared();
        let handle = CrawlerHandle::new(Arc::downgrade(&shared));
        handle.stop();
        assert!(shared.drain.is_cancelled());
        handle.abort();
        assert!(shared.cancel.is_cancelled());
    }
}
