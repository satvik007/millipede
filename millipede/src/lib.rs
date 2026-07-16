#![doc = include_str!("../README.md")]

pub use millipede_core as core;

#[cfg(feature = "browser")]
pub use millipede_browser as browser;
#[cfg(feature = "browser-chromiumoxide")]
pub use millipede_browser_chromiumoxide as browser_chromiumoxide;
#[cfg(feature = "fingerprint")]
pub use millipede_fingerprint as fingerprint;
#[cfg(feature = "html")]
pub use millipede_html as html;
#[cfg(feature = "http")]
pub use millipede_http as http;
#[cfg(feature = "storage-fs")]
pub use millipede_storage_fs as storage_fs;
#[cfg(feature = "storage-memory")]
pub use millipede_storage_memory as storage_memory;

/// Curated public API for ergonomic `millipede::Type` imports.
pub use millipede_core::autoscale::{
    AimdController, AutoscaleMode, AutoscaledPool, AutoscaledPoolOptions, ClientLoadSignal,
    ClientLoadSignalHandle, CpuLoadSignal, CpuLoadSignalOptions, LoadSignal, LoadSnapshot,
    MemoryLoadSignal, MemoryLoadSignalOptions, ScaleDecision, Snapshotter, SnapshotterOptions,
    SystemStatus, SystemStatusOptions, TokioRuntimeLoadSignal, TokioRuntimeLoadSignalOptions,
};
pub use millipede_core::config::{Configuration, ConfigurationBuilder, LogLevel};
pub use millipede_core::cookies::{CookieJar, CookieJarError};
pub use millipede_core::crawler::{
    AttemptObservation, AutoscalerSnapshot, BasicContext, BasicCrawler, BasicKind, Crawler,
    CrawlerBuildError, CrawlerBuilder, CrawlerEnv, CrawlerHandle, CrawlerKind, IntoStartRequest,
    IntoStartRequests, RequestEnv, RequestOutcome, RequestPrep,
};
pub use millipede_core::enqueue::{
    EnqueueLinker, EnqueueLinksOptions, EnqueueResult, SkipReason, SkippedUrl,
};
pub use millipede_core::errors::{AntiBotTech, CrawlError};
pub use millipede_core::events::{
    CrawlerEvent, EventBus, EventStream, HandledRequest, RequestFinalState, ResultStream,
};
pub use millipede_core::handler::{
    FailedRequestContext, FailedRequestHandler, Middleware, RequestHandler,
};
pub use millipede_core::http_client::{
    HttpClient, HttpClientError, HttpRequest, HttpResponse, HttpStatusError, StreamingResponse,
};
pub use millipede_core::proxy::{
    ProxyBuckets, ProxyConfiguration, ProxyInfo, ProxyKind, ProxyResolveContext, ProxyResolver,
    ProxyRouteContext, ProxyStrategy, RotationStrategy,
};
pub use millipede_core::request::{
    HeaderMap, IntoUrl, Method, Request, RequestBody, RequestBuilder, RequestId, RequestState,
    UserData,
};
pub use millipede_core::retry_strategy::{
    AttemptOutcome, AttemptOverrides, RetryDirective, RetryStrategy, SessionRetryAction,
};
pub use millipede_core::router::{HasRequest, MethodFilter, Router};
pub use millipede_core::session::{
    SESSION_POOL_PERSIST_KEY, Session, SessionConfig, SessionId, SessionPool, SessionPoolOptions,
};
pub use millipede_core::statistics::{
    FinalStatistics, STATISTICS_PERSIST_KEY, StatisticsHandle, StatisticsSnapshot,
};
pub use millipede_core::storage::{
    AddOptions, AutoSaved, Dataset, DatasetExt, KeyValueStore, KeyValueStoreExt, Lease, LeaseId,
    ListOptions, ProcessedRequest, QueueOpInfo, ReclaimOptions, RequestQueue, RequestSource,
    StorageClient, StorageError, StorageHandle, StorageResult,
};
#[cfg(feature = "http")]
pub use millipede_http::{
    CoalescingClient, HttpContext, HttpCrawler, HttpKind, HttpKindBuilder, ReqwestClient,
    ReqwestClientOptions,
};
#[cfg(feature = "storage-memory")]
pub use millipede_storage_memory::{MemoryQueuePolicy, MemoryRequestQueue, MemoryStorageClient};

/// Commonly used items across all enabled Millipede crates.
pub mod prelude {
    // Some feature-gated sub-crate preludes remain empty until their phases land, so those glob
    // re-exports would otherwise trip `unused_imports` under -D warnings.
    #![allow(unused_imports)]

    pub use millipede_core::prelude::*;

    #[cfg(feature = "browser")]
    pub use millipede_browser::prelude::*;
    #[cfg(feature = "browser-chromiumoxide")]
    pub use millipede_browser_chromiumoxide::prelude::*;
    #[cfg(feature = "fingerprint")]
    pub use millipede_fingerprint::prelude::*;
    #[cfg(feature = "html")]
    pub use millipede_html::prelude::*;
    #[cfg(feature = "http")]
    pub use millipede_http::prelude::*;
    #[cfg(feature = "storage-fs")]
    pub use millipede_storage_fs::prelude::*;
    #[cfg(feature = "storage-memory")]
    pub use millipede_storage_memory::prelude::*;
}
