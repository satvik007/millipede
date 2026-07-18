//! Construction of configured crawlers.

use super::{Crawler, CrawlerKind, CrawlerShared, engine::EngineOptions};
use crate::{
    autoscale::{AutoscaleMode, AutoscaledPool, AutoscaledPoolOptions},
    config::{ConfigError, Configuration},
    handler::{FailedRequestHandler, RequestHandler},
    link_extraction::CrawlPolicy,
    storage::{KeyValueStore, RequestQueue, StorageClient, StorageError},
};
use std::{sync::Arc, time::Duration};

/// An error produced while building a crawler.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CrawlerBuildError {
    /// No request handler was supplied.
    #[error("a request handler is required")]
    MissingRequestHandler,
    /// Neither the builder nor its configuration supplied storage.
    #[error(
        "no storage client configured (set .storage_client(...) or Configuration::storage_client)"
    )]
    MissingStorage,
    /// The configured concurrency is zero.
    #[error("max_concurrency must be at least 1")]
    ZeroMaxConcurrency,
    /// The configured dynamic scheduler options are invalid.
    #[error(
        "dynamic scheduler options require valid concurrency bounds and a non-zero maybe_run_interval"
    )]
    InvalidConcurrencyBounds,
    /// The configured result channel capacity is zero.
    #[error("results_capacity must be at least 1")]
    ZeroResultsCapacity,
    /// Configuration resolution failed.
    #[error("configuration: {0}")]
    Config(#[from] ConfigError),
    /// Storage initialization failed.
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
}

/// Builds a [`Crawler`] around a crawler kind.
#[must_use = "builders do nothing unless consumed by build"]
pub struct CrawlerBuilder<K: CrawlerKind> {
    kind: K,
    handler: Option<Arc<dyn RequestHandler<K::Context>>>,
    failed_handler: Option<Arc<dyn FailedRequestHandler>>,
    autoscaled_pool: AutoscaledPoolOptions,
    max_request_retries: u32,
    max_session_rotations: u32,
    request_handler_timeout: Duration,
    internal_operation_timeout: Duration,
    configuration: Option<Configuration>,
    storage_client: Option<Arc<dyn StorageClient>>,
    request_queue: Option<Arc<dyn RequestQueue>>,
    results_capacity: usize,
    retry_strategy: Option<Arc<dyn crate::retry_strategy::RetryStrategy>>,
    crawl_policy: Option<Arc<CrawlPolicy>>,
}

impl<K: CrawlerKind> CrawlerBuilder<K> {
    /// Creates a builder with engine defaults.
    pub fn new(kind: K) -> Self {
        Self {
            kind,
            handler: None,
            failed_handler: None,
            autoscaled_pool: AutoscaledPoolOptions {
                fixed_concurrency: Some(10),
                max_concurrency: 10,
                ..Default::default()
            },
            max_request_retries: 3,
            max_session_rotations: 10,
            request_handler_timeout: Duration::from_secs(60),
            internal_operation_timeout: Duration::from_secs(30),
            configuration: None,
            storage_client: None,
            request_queue: None,
            results_capacity: 1024,
            retry_strategy: None,
            crawl_policy: None,
        }
    }

    /// Sets the request handler.
    pub fn request_handler<H: RequestHandler<K::Context>>(mut self, handler: H) -> Self {
        self.handler = Some(Arc::new(handler));
        self
    }

    /// Sets the permanent-failure handler.
    pub fn failed_request_handler<H: FailedRequestHandler>(mut self, handler: H) -> Self {
        self.failed_handler = Some(Arc::new(handler));
        self
    }

    /// Sets the maximum number of concurrent requests and pins concurrency to that value.
    ///
    /// Calling this after [`Self::autoscale_mode`] re-pins the crawler to fixed concurrency.
    /// Calling [`Self::autoscale_mode`] after this method retains `count` as the dynamic ceiling.
    pub fn max_concurrency(mut self, count: usize) -> Self {
        self.autoscaled_pool.fixed_concurrency = Some(count);
        self.autoscaled_pool.max_concurrency = count;
        self
    }
    /// Sets the minimum dynamic concurrency.
    pub fn min_concurrency(mut self, count: usize) -> Self {
        self.autoscaled_pool.min_concurrency = count;
        self
    }
    /// Sets the initial desired dynamic concurrency.
    pub fn desired_concurrency(mut self, count: usize) -> Self {
        self.autoscaled_pool.desired_concurrency = Some(count);
        self
    }
    /// Selects dynamic autoscaling.
    ///
    /// Calling this after [`Self::max_concurrency`] keeps that value as the dynamic ceiling.
    /// Calling [`Self::max_concurrency`] afterward re-pins concurrency to a fixed value.
    pub fn autoscale_mode(mut self, mode: AutoscaleMode) -> Self {
        self.autoscaled_pool.mode = mode;
        self.autoscaled_pool.fixed_concurrency = None;
        self
    }
    /// Sets the maximum number of task starts permitted per minute.
    pub fn max_tasks_per_minute(mut self, count: u32) -> Self {
        self.autoscaled_pool.max_tasks_per_minute = Some(count);
        self
    }
    /// Sets the minimum delay between requests to the same domain.
    pub fn same_domain_delay(mut self, delay: Duration) -> Self {
        self.autoscaled_pool.same_domain_delay = delay;
        self
    }
    /// Replaces all autoscaled-pool options for advanced use.
    ///
    /// The pool's `task_timeout` bounds request preparation, execution, and handler work after an
    /// attempt starts. Its `maybe_run_interval` periodically rechecks the queue as a missed-wakeup
    /// safety net and must be greater than zero.
    pub fn autoscaled_pool_options(mut self, options: AutoscaledPoolOptions) -> Self {
        self.autoscaled_pool = options;
        self
    }
    /// Sets the maximum number of ordinary request retries.
    pub fn max_request_retries(mut self, count: u32) -> Self {
        self.max_request_retries = count;
        self
    }
    /// Sets the maximum number of session rotations.
    pub fn max_session_rotations(mut self, count: u32) -> Self {
        self.max_session_rotations = count;
        self
    }
    /// Sets the request-handler timeout.
    pub fn request_handler_timeout(mut self, timeout: Duration) -> Self {
        self.request_handler_timeout = timeout;
        self
    }
    /// Sets the internal storage-operation timeout.
    pub fn internal_operation_timeout(mut self, timeout: Duration) -> Self {
        self.internal_operation_timeout = timeout;
        self
    }
    /// Sets the resolved crawler configuration.
    pub fn configuration(mut self, configuration: Configuration) -> Self {
        self.configuration = Some(configuration);
        self
    }
    /// Overrides the configuration's storage client.
    pub fn storage_client(mut self, storage: Arc<dyn StorageClient>) -> Self {
        self.storage_client = Some(storage);
        self
    }
    /// Overrides the request queue opened from storage.
    ///
    /// This hook lets a [`crate::sitemap::RequestQueueWithSitemap`] tandem drive the crawler.
    /// Callers resuming a pre-populated persistent queue should pair it with
    /// [`crate::config::ConfigurationBuilder::purge_on_start`] set to `false`. Storage still
    /// supplies the crawler's key-value store and other storage objects.
    pub fn request_queue(mut self, queue: Arc<dyn RequestQueue>) -> Self {
        self.request_queue = Some(queue);
        self
    }
    /// Sets the terminal-result broadcast capacity.
    pub fn results_capacity(mut self, capacity: usize) -> Self {
        self.results_capacity = capacity;
        self
    }

    /// Installs an attempt-level retry strategy.
    pub fn retry_strategy<S: crate::retry_strategy::RetryStrategy>(mut self, strategy: S) -> Self {
        self.retry_strategy = Some(Arc::new(strategy));
        self
    }

    /// Sets the long-lived link admission and crawl-limit policy.
    pub fn crawl_policy(mut self, policy: CrawlPolicy) -> Self {
        self.crawl_policy = Some(Arc::new(policy));
        self
    }

    /// Builds the crawler and opens its configured storage objects.
    ///
    /// The default [`Configuration`] purges all data managed by the selected storage client before
    /// opening the queue and key-value store. Set `purge_on_start(false)` to retain existing data.
    pub async fn build(self) -> Result<Crawler<K>, CrawlerBuildError> {
        let handler = self
            .handler
            .ok_or(CrawlerBuildError::MissingRequestHandler)?;
        if self.autoscaled_pool.fixed_concurrency == Some(0) {
            return Err(CrawlerBuildError::ZeroMaxConcurrency);
        }
        if (self.autoscaled_pool.fixed_concurrency.is_none()
            && (self.autoscaled_pool.min_concurrency < 1
                || self.autoscaled_pool.max_concurrency < self.autoscaled_pool.min_concurrency))
            || self.autoscaled_pool.maybe_run_interval.is_zero()
        {
            return Err(CrawlerBuildError::InvalidConcurrencyBounds);
        }
        if self.results_capacity == 0 {
            return Err(CrawlerBuildError::ZeroResultsCapacity);
        }
        let config = match self.configuration {
            Some(configuration) => configuration,
            None => Configuration::builder().build()?,
        };
        let storage = self
            .storage_client
            .or_else(|| config.storage_client().cloned())
            .ok_or(CrawlerBuildError::MissingStorage)?;
        if config.purge_on_start() {
            storage.purge().await?;
        }
        let queue = match self.request_queue {
            Some(queue) => queue,
            None => {
                storage
                    .open_request_queue(Some(config.default_request_queue_id()))
                    .await?
            }
        };
        let kvs: Option<Arc<dyn KeyValueStore>> = Some(
            storage
                .open_key_value_store(Some(config.default_key_value_store_id()))
                .await?,
        );
        let task_timeout = self.autoscaled_pool.task_timeout;
        let maybe_run_interval = self.autoscaled_pool.maybe_run_interval;
        let opts = EngineOptions {
            max_request_retries: self.max_request_retries,
            max_session_rotations: self.max_session_rotations,
            request_handler_timeout: self.request_handler_timeout,
            internal_operation_timeout: self.internal_operation_timeout,
            persist_state_interval: config.persist_state_interval(),
            task_timeout,
            maybe_run_interval,
            retry_strategy: self.retry_strategy,
            max_requests_per_crawl: self
                .crawl_policy
                .as_ref()
                .and_then(|policy| policy.max_requests_per_crawl),
        };
        let pool = Arc::new(AutoscaledPool::new(self.autoscaled_pool));
        let shared = Arc::new(CrawlerShared::new_with_policy(
            queue,
            config.events().clone(),
            self.results_capacity,
            self.internal_operation_timeout,
            pool,
            self.crawl_policy,
        ));
        Ok(Crawler {
            kind: Arc::new(self.kind),
            shared,
            config: Arc::new(config),
            handler,
            failed_handler: self.failed_handler,
            kvs,
            storage: Some(storage),
            opts,
            started: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crawler::{BasicContext, BasicKind},
        memory_client as millipede_storage_memory,
    };

    fn aimd() -> AutoscaleMode {
        AutoscaleMode::Aimd {
            increase_after_successes: 1,
            decrease_factor: 0.5,
        }
    }

    #[tokio::test]
    async fn invalid_dynamic_bounds_are_rejected_before_storage_resolution() {
        let zero_min = CrawlerBuilder::new(BasicKind)
            .request_handler(|_ctx: BasicContext| async { Ok(()) })
            .autoscale_mode(aimd())
            .min_concurrency(0)
            .build()
            .await;
        assert!(matches!(
            zero_min,
            Err(CrawlerBuildError::InvalidConcurrencyBounds)
        ));

        let inverted = CrawlerBuilder::new(BasicKind)
            .request_handler(|_ctx: BasicContext| async { Ok(()) })
            .min_concurrency(2)
            .max_concurrency(1)
            .autoscale_mode(aimd())
            .build()
            .await;
        assert!(matches!(
            inverted,
            Err(CrawlerBuildError::InvalidConcurrencyBounds)
        ));
    }

    #[tokio::test]
    async fn zero_maybe_run_interval_is_rejected_before_storage_resolution() {
        let result = CrawlerBuilder::new(BasicKind)
            .request_handler(|_ctx: BasicContext| async { Ok(()) })
            .autoscaled_pool_options(AutoscaledPoolOptions {
                maybe_run_interval: Duration::ZERO,
                ..Default::default()
            })
            .build()
            .await;

        assert!(matches!(
            result,
            Err(CrawlerBuildError::InvalidConcurrencyBounds)
        ));
    }

    #[tokio::test]
    async fn autoscale_mode_after_max_concurrency_keeps_dynamic_ceiling() {
        let crawler = CrawlerBuilder::new(BasicKind)
            .request_handler(|_ctx: BasicContext| async { Ok(()) })
            .storage_client(Arc::new(
                millipede_storage_memory::MemoryStorageClient::new(),
            ))
            .max_concurrency(5)
            .autoscale_mode(aimd())
            .build()
            .await
            .unwrap();

        let snapshot = crawler.autoscaler_snapshot();
        assert!(!snapshot.is_fixed);
        assert_eq!(snapshot.max_concurrency, 5);
    }

    #[tokio::test]
    async fn max_concurrency_after_autoscale_mode_repins_to_fixed() {
        let crawler = CrawlerBuilder::new(BasicKind)
            .request_handler(|_ctx: BasicContext| async { Ok(()) })
            .storage_client(Arc::new(
                millipede_storage_memory::MemoryStorageClient::new(),
            ))
            .autoscale_mode(aimd())
            .max_concurrency(5)
            .build()
            .await
            .unwrap();

        let snapshot = crawler.autoscaler_snapshot();
        assert!(snapshot.is_fixed);
        assert_eq!(snapshot.desired_concurrency, 5);
    }
}
