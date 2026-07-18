#![doc = include_str!("../README.md")]

pub use millipede_core as core;

#[cfg(feature = "browser")]
pub use millipede_browser as browser;
#[cfg(feature = "browser-chromiumoxide")]
pub use millipede_browser_chromiumoxide as browser_chromiumoxide;
#[cfg(feature = "fingerprint")]
pub use millipede_fingerprint as fingerprint;
#[cfg(feature = "fingerprint")]
pub use millipede_fingerprint::{BrowserFingerprintGenerator, HeaderGenerator, HeaderProfile};
#[cfg(feature = "html")]
pub use millipede_html as html;
#[cfg(feature = "http")]
pub use millipede_http as http;
#[cfg(feature = "storage-fs")]
pub use millipede_storage_fs as storage_fs;
#[cfg(feature = "storage-memory")]
pub use millipede_storage_memory as storage_memory;

#[cfg(feature = "browser")]
pub use millipede_browser::{
    BrowserContext, BrowserCrawler, BrowserError, BrowserHooks, BrowserKind, BrowserKindBuilder,
    BrowserPage, BrowserPool, BrowserPoolOptions, BrowserPostHookCtx, BrowserPostNavigationHook,
    BrowserPreHookCtx, BrowserPreNavigationHook, BrowserPromotionDetector, BrowserProvider,
    BrowserResponse, DefaultPromotionDetector, GotoOptions, HttpAttemptSnapshot, LaunchContext,
    PageClosedHook, PageHandle, PageHook, PageId, PageOptions, PagePrepHook, PreLaunchHook,
    PromotionReason, ScreenshotOptions, SmartContext, SmartCrawler, SmartKind, SmartKindBuilder,
    WaitUntil,
};
#[cfg(feature = "browser-chromiumoxide")]
pub use millipede_browser_chromiumoxide::discovery::find_browser;
#[cfg(feature = "browser-chromiumoxide")]
pub use millipede_browser_chromiumoxide::{
    ChromiumBrowser, ChromiumLaunchOptions, ChromiumPage, ChromiumoxideProvider,
};
/// Curated public API for ergonomic `millipede::Type` imports.
pub use millipede_core::antibot::{AntiBotDetector, AntiBotSignals, DefaultAntiBotDetector};
pub use millipede_core::autoscale::{
    AimdController, AutoscaleMode, AutoscaledPool, AutoscaledPoolOptions, ClientLoadSignal,
    ClientLoadSignalHandle, CpuLoadSignal, CpuLoadSignalOptions, LoadSignal, LoadSnapshot,
    MemoryLoadSignal, MemoryLoadSignalOptions, ScaleDecision, Snapshotter, SnapshotterOptions,
    SystemStatus, SystemStatusOptions, TokioRuntimeLoadSignal, TokioRuntimeLoadSignalOptions,
};
pub use millipede_core::config::{ConfigError, Configuration, ConfigurationBuilder, LogLevel};
pub use millipede_core::cookies::{Cookie, CookieJar, CookieJarError, SameSite};
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
pub use millipede_core::link_extraction::{
    CrawlPolicy, EnqueueStrategy, ExtractedLink, GlobPattern, LinkExtractor, LinkPatternError,
    SkippedHandler, TransformResult, UrlMatch, UrlPattern,
};
pub use millipede_core::proxy::{
    ProxyBuckets, ProxyConfiguration, ProxyInfo, ProxyKind, ProxyResolveContext, ProxyResolver,
    ProxyRouteContext, ProxyStrategy, RotationStrategy,
};
pub use millipede_core::request::{
    HeaderMap, IntoUrl, Method, Request, RequestBody, RequestBuildError, RequestBuilder, RequestId,
    RequestState, UserData,
};
pub use millipede_core::retry_strategy::{
    AttemptOutcome, AttemptOverrides, RetryDirective, RetryStrategy, SessionRetryAction,
};
pub use millipede_core::router::{HasRequest, MethodFilter, Router};
pub use millipede_core::session::{
    SESSION_POOL_PERSIST_KEY, Session, SessionConfig, SessionId, SessionPool, SessionPoolOptions,
    SessionToken,
};
pub use millipede_core::sitemap::{
    RequestQueueWithSitemap, SITEMAP_STATE_KEY, SitemapEntry, SitemapRequestList,
    SitemapRequestListBuilder,
};
pub use millipede_core::snapshot::{ErrorSnapshot, ErrorSnapshotter};
pub use millipede_core::statistics::{
    FinalStatistics, STATISTICS_PERSIST_KEY, StatisticsHandle, StatisticsSnapshot,
};
pub use millipede_core::storage::{
    AddOptions, AddRequestsBatchedResult, AutoSaved, BatchAddHandle, Dataset, DatasetExt,
    DatasetInfo, KeyInfo, KeyList, KeyValueStore, KeyValueStoreExt, KvEntry, Lease, LeaseId,
    ListKeysOptions, ListOptions, Page, ProcessedRequest, QueueOpInfo, RateLimitReportingClient,
    ReclaimOptions, RequestQueue, RequestSource, StorageClient, StorageError, StorageHandle,
    StorageResult,
};
#[cfg(feature = "html")]
pub use millipede_html::{
    HtmlContext, HtmlCrawler, HtmlError, HtmlKind, HtmlKindBuilder, HtmlLinkExtractor,
    SynchronizedHtml,
};
#[cfg(feature = "http")]
pub use millipede_http::{
    CoalescingClient, HttpContext, HttpCrawler, HttpKind, HttpKindBuilder, HttpPostHookCtx,
    HttpPostNavigationHook, HttpPreHookCtx, HttpPreNavigationHook, ReqwestClient,
    ReqwestClientOptions,
};
#[cfg(feature = "storage-fs")]
pub use millipede_storage_fs::{FsDataset, FsKeyValueStore, FsRequestQueue, FsStorageClient};
#[cfg(feature = "storage-memory")]
pub use millipede_storage_memory::{
    DomainRoundRobin, MemoryDataset, MemoryKeyValueStore, MemoryQueuePolicy, MemoryRequestQueue,
    MemoryStorageClient,
};

/// Commonly used items across all enabled Millipede crates.
pub mod prelude {
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

#[cfg(all(test, feature = "browser", feature = "browser-chromiumoxide"))]
mod browser_reexport_compile_guard {
    use super::*;

    fn assert_provider<P: BrowserProvider>() {}
    fn assert_page<P: BrowserPage>() {}

    #[test]
    fn umbrella_exposes_browser_types() {
        assert_provider::<ChromiumoxideProvider>();
        assert_page::<ChromiumPage>();
        let _ = ChromiumLaunchOptions::default();
        let _ = find_browser as fn() -> Option<std::path::PathBuf>;
        let _: Option<BrowserKindBuilder<ChromiumoxideProvider>> = None;
        let _: Option<SmartKindBuilder<ChromiumoxideProvider>> = None;
    }
}
