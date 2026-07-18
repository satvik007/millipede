#![doc = include_str!("../README.md")]

#[cfg(test)]
extern crate self as millipede_core;

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../millipede-storage-memory/src/dataset.rs"]
mod dataset;
#[cfg(test)]
#[allow(dead_code)]
#[path = "../../millipede-storage-memory/src/kvs.rs"]
mod kvs;
#[cfg(test)]
#[allow(dead_code)]
#[path = "../../millipede-storage-memory/src/client.rs"]
mod memory_client;
#[cfg(test)]
#[allow(dead_code)]
#[path = "../../millipede-storage-memory/src/policy.rs"]
mod policy;
#[cfg(test)]
#[allow(dead_code, unreachable_patterns)]
#[path = "../../millipede-storage-memory/src/queue.rs"]
mod queue;
#[cfg(test)]
use dataset::MemoryDataset;
#[cfg(test)]
use kvs::MemoryKeyValueStore;
#[cfg(test)]
use queue::MemoryRequestQueue;

/// Content-based anti-bot and web application firewall detection.
pub mod antibot;
/// Autoscaling: dynamic concurrency, load signals, and rate limiting.
pub mod autoscale;
/// Crawler configuration and environment resolution.
pub mod config;
/// Session cookie storage and persistence.
pub mod cookies;
/// Crawler lifecycle kinds, handles, and shared state.
pub mod crawler;
/// Link enqueueing from handler contexts.
pub mod enqueue;
/// Crawl error taxonomy and retry classification.
pub mod errors;
/// Crawler lifecycle events and broadcast support.
pub mod events;
/// Request handler and middleware contracts.
pub mod handler;
/// Backend-independent HTTP request, response, and client abstractions.
pub mod http_client;
/// Link-extraction strategies, URL patterns, and crawl policy.
pub mod link_extraction;
/// Proxy configuration and rotation strategies.
pub mod proxy;
/// Request data types and construction helpers.
pub mod request;
/// Attempt-level retry strategy hooks.
pub mod retry_strategy;
/// Label- and method-based request routing.
pub mod router;
/// Sessions and reusable session pools.
pub mod session;
/// Streaming sitemap ingestion.
pub mod sitemap;
/// Failure-artifact capture and reload support.
pub mod snapshot;
/// Crawl statistics, rates, and persistence.
pub mod statistics;
/// Object-safe storage abstractions and typed convenience wrappers.
pub mod storage;

mod util;

/// Commonly used items from this crate.
pub mod prelude {
    pub use crate::antibot::{AntiBotDetector, AntiBotSignals, DefaultAntiBotDetector};
    pub use crate::autoscale::{
        AimdController, AutoscaleMode, AutoscaledPool, AutoscaledPoolOptions, ClientLoadSignal,
        ClientLoadSignalHandle, CpuLoadSignal, CpuLoadSignalOptions, LoadSignal, LoadSnapshot,
        MemoryLoadSignal, MemoryLoadSignalOptions, ScaleDecision, Snapshotter, SnapshotterOptions,
        SystemStatus, SystemStatusOptions, TokioRuntimeLoadSignal, TokioRuntimeLoadSignalOptions,
    };
    pub use crate::config::{Configuration, ConfigurationBuilder, LogLevel};
    pub use crate::cookies::{Cookie, CookieJar, CookieJarError, SameSite};
    pub use crate::crawler::{
        AttemptObservation, AutoscalerSnapshot, BasicContext, BasicCrawler, BasicKind, Crawler,
        CrawlerBuildError, CrawlerBuilder, CrawlerEnv, CrawlerHandle, CrawlerKind,
        IntoStartRequest, IntoStartRequests, RequestEnv, RequestOutcome, RequestPrep,
    };
    pub use crate::enqueue::{
        EnqueueLinker, EnqueueLinksOptions, EnqueueResult, SkipReason, SkippedUrl,
    };
    pub use crate::errors::{AntiBotTech, CrawlError};
    pub use crate::events::{
        CrawlerEvent, EventBus, EventStream, HandledRequest, RequestFinalState, ResultStream,
    };
    pub use crate::handler::{
        FailedRequestContext, FailedRequestHandler, Middleware, RequestHandler,
    };
    pub use crate::http_client::{
        HttpClient, HttpClientError, HttpRequest, HttpResponse, HttpStatusError, StreamingResponse,
    };
    pub use crate::link_extraction::{
        CrawlPolicy, EnqueueStrategy, ExtractedLink, GlobPattern, LinkExtractor, LinkPatternError,
        SkippedHandler, TransformResult, UrlMatch, UrlPattern,
    };
    pub use crate::proxy::{
        ProxyBuckets, ProxyConfiguration, ProxyInfo, ProxyKind, ProxyResolveContext, ProxyResolver,
        ProxyRouteContext, ProxyStrategy, RotationStrategy,
    };
    pub use crate::request::{
        HeaderMap, IntoUrl, Method, Request, RequestBody, RequestBuildError, RequestBuilder,
        RequestId, RequestState, UserData,
    };
    pub use crate::retry_strategy::{
        AttemptOutcome, AttemptOverrides, RetryDirective, RetryStrategy, SessionRetryAction,
    };
    pub use crate::router::{HasRequest, MethodFilter, Router};
    pub use crate::session::{
        SESSION_POOL_PERSIST_KEY, Session, SessionConfig, SessionId, SessionPool,
        SessionPoolOptions,
    };
    pub use crate::sitemap::{
        RequestQueueWithSitemap, SITEMAP_STATE_KEY, SitemapEntry, SitemapRequestList,
        SitemapRequestListBuilder,
    };
    pub use crate::snapshot::{ErrorSnapshot, ErrorSnapshotter};
    pub use crate::statistics::{
        FinalStatistics, STATISTICS_PERSIST_KEY, StatisticsHandle, StatisticsSnapshot,
    };
    pub use crate::storage::{
        AddOptions, AddRequestsBatchedResult, AutoSaved, BatchAddHandle, Dataset, DatasetExt,
        DatasetInfo, KeyInfo, KeyList, KeyValueStore, KeyValueStoreExt, KvEntry, Lease, LeaseId,
        ListKeysOptions, ListOptions, Page, ProcessedRequest, QueueOpInfo,
        RateLimitReportingClient, ReclaimOptions, RequestQueue, RequestSource, StorageClient,
        StorageError, StorageHandle, StorageResult,
    };
}
