//! Construction of configured crawlers.

use super::{Crawler, CrawlerKind, CrawlerShared, engine::EngineOptions};
use crate::{
    config::{ConfigError, Configuration},
    handler::{FailedRequestHandler, RequestHandler},
    storage::{KeyValueStore, StorageClient, StorageError},
};
use std::{sync::Arc, time::Duration};

/// An error produced while building a crawler.
#[derive(Debug, thiserror::Error)]
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
pub struct CrawlerBuilder<K: CrawlerKind> {
    kind: K,
    handler: Option<Arc<dyn RequestHandler<K::Context>>>,
    failed_handler: Option<Arc<dyn FailedRequestHandler>>,
    max_concurrency: usize,
    max_request_retries: u32,
    max_session_rotations: u32,
    request_handler_timeout: Duration,
    internal_operation_timeout: Duration,
    configuration: Option<Configuration>,
    storage_client: Option<Arc<dyn StorageClient>>,
    results_capacity: usize,
    retry_strategy: Option<Arc<dyn crate::retry_strategy::RetryStrategy>>,
}

impl<K: CrawlerKind> CrawlerBuilder<K> {
    /// Creates a builder with engine defaults.
    pub fn new(kind: K) -> Self {
        Self {
            kind,
            handler: None,
            failed_handler: None,
            max_concurrency: 10,
            max_request_retries: 3,
            max_session_rotations: 10,
            request_handler_timeout: Duration::from_secs(60),
            internal_operation_timeout: Duration::from_secs(30),
            configuration: None,
            storage_client: None,
            results_capacity: 1024,
            retry_strategy: None,
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

    /// Sets the maximum number of concurrent requests.
    pub fn max_concurrency(mut self, count: usize) -> Self {
        self.max_concurrency = count;
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

    /// Builds the crawler and opens its configured storage objects.
    ///
    /// The default [`Configuration`] purges all data managed by the selected storage client before
    /// opening the queue and key-value store. Set `purge_on_start(false)` to retain existing data.
    pub async fn build(self) -> Result<Crawler<K>, CrawlerBuildError> {
        let handler = self
            .handler
            .ok_or(CrawlerBuildError::MissingRequestHandler)?;
        if self.max_concurrency == 0 {
            return Err(CrawlerBuildError::ZeroMaxConcurrency);
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
        let queue = storage
            .open_request_queue(Some(config.default_request_queue_id()))
            .await?;
        let kvs: Option<Arc<dyn KeyValueStore>> = Some(
            storage
                .open_key_value_store(Some(config.default_key_value_store_id()))
                .await?,
        );
        let opts = EngineOptions {
            max_concurrency: self.max_concurrency,
            max_request_retries: self.max_request_retries,
            max_session_rotations: self.max_session_rotations,
            request_handler_timeout: self.request_handler_timeout,
            internal_operation_timeout: self.internal_operation_timeout,
            persist_state_interval: config.persist_state_interval(),
            retry_strategy: self.retry_strategy,
        };
        let shared = Arc::new(CrawlerShared::new(
            queue,
            config.events().clone(),
            self.results_capacity,
            self.internal_operation_timeout,
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
