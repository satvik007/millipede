//! Browser-backed crawler kind and handler context.

use std::{
    fmt,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use anyhow::anyhow;
use futures_util::future::BoxFuture;
use millipede_core::{
    crawler::{
        AttemptObservation, Crawler, CrawlerEnv, CrawlerHandle, CrawlerKind, RequestEnv,
        RequestOutcome,
    },
    enqueue::EnqueueLinker,
    errors::CrawlError,
    events::CrawlerEvent,
    http_client::{HttpClient, HttpClientError, HttpStatusError},
    link_extraction::{ExtractedLink, LinkExtractor},
    proxy::{ProxyConfiguration, ProxyInfo},
    request::Request,
    router::HasRequest,
    session::{Session, SessionPool, SessionPoolOptions},
    storage::StorageHandle,
};

use crate::{
    BrowserError, BrowserHooks, BrowserPool, BrowserPoolOptions, BrowserProvider, BrowserResponse,
    GotoOptions, PageHandle, PageOpts, WaitUntil,
};

struct BrowserLinkExtractor {
    page: PageHandle,
}

#[async_trait::async_trait]
impl LinkExtractor for BrowserLinkExtractor {
    async fn extract(&self, selector: Option<&str>) -> Result<Vec<ExtractedLink>, CrawlError> {
        self.page
            .evaluate_anchors(selector)
            .await
            .map_err(BrowserError::classify)
            .map(|urls| {
                urls.into_iter()
                    .map(|url| ExtractedLink {
                        url: url.to_string(),
                        base: None,
                    })
                    .collect()
            })
    }
}

/// Per-request context produced by [`BrowserKind`].
///
/// This matches the browser context in INTERFACE §4.2 except that the proposed `log: Log` field
/// is omitted because no `Log` type exists; use `tracing` macros in the meantime.
#[derive(Clone)]
#[non_exhaustive]
pub struct BrowserContext {
    /// Crawl request that produced this context.
    pub request: Arc<Request>,
    /// Live browser page.
    ///
    /// Cloning the context shares the same underlying page. Every clone uses the page handle's
    /// common at-most-once close state, so cleanup cannot close the provider page twice.
    pub page: PageHandle,
    /// Navigation response metadata, when exposed by the provider.
    pub response: Option<BrowserResponse>,
    /// Session used for this attempt, if sessions are enabled.
    pub session: Option<Arc<Session>>,
    /// Proxy owned by the browser containing this page.
    ///
    /// Browser proxies are applied at browser launch, not per navigation. Consequently this is
    /// the owning browser's proxy and per-request proxy rotation is not possible for browser kinds.
    pub proxy_info: Option<ProxyInfo>,
    /// DOM-aware URL enqueue helper linked to the running crawler.
    pub enqueue: EnqueueLinker,
    /// Open default storage resources.
    pub storage: StorageHandle,
    /// HTTP client for out-of-band requests made during browser flows.
    pub send_request: Arc<dyn HttpClient>,
    /// Weak handle back to the running crawler.
    pub crawler: CrawlerHandle,
}

impl HasRequest for BrowserContext {
    fn request(&self) -> &Request {
        &self.request
    }
}

impl fmt::Debug for BrowserContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrowserContext")
            .field("request", &self.request)
            .field("page", &self.page)
            .field("response", &self.response)
            .field("session", &self.session)
            .field("proxy_info", &self.proxy_info)
            .field("enqueue", &self.enqueue)
            .field("storage", &self.storage)
            .field("send_request", &"<dyn HttpClient>")
            .field("crawler", &self.crawler)
            .finish()
    }
}

enum SessionMode {
    Disabled,
    Owned(Arc<SessionPool>),
    Shared(Arc<SessionPool>),
}

/// Browser fetching behavior used by [`BrowserCrawler`].
///
/// Session persistence has explicit ownership semantics. An owned pool is attached to crawler
/// storage, restored in [`CrawlerKind::start`], persisted on `PersistState`, and persisted once
/// more during [`CrawlerKind::stop`]. A shared pool is used for session selection and cookie
/// synchronization only: the sharing owner (for example, a smart kind) must restore and persist
/// it. This prevents double restore and double persistence when kinds share one pool.
pub struct BrowserKind<P: BrowserProvider> {
    pool: BrowserPool<P>,
    sessions: SessionMode,
    send_request: Arc<dyn HttpClient>,
    retry_status_codes: Vec<u16>,
    retry_server_errors: bool,
    session_status_codes: Vec<u16>,
    goto: GotoOptions,
    storage: OnceLock<StorageHandle>,
    persist_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl<P: BrowserProvider> BrowserKind<P> {
    /// Starts configuring a browser crawler kind backed by `provider`.
    pub fn builder(provider: P) -> BrowserKindBuilder<P> {
        BrowserKindBuilder::new(provider)
    }

    fn session_pool(&self) -> Option<&Arc<SessionPool>> {
        match &self.sessions {
            SessionMode::Disabled => None,
            SessionMode::Owned(pool) | SessionMode::Shared(pool) => Some(pool),
        }
    }

    async fn close_after_error(&self, page: &PageHandle, reason: &'static str) {
        if let Err(close_error) = page.close().await {
            tracing::warn!(%close_error, "failed to close page after {reason}");
        }
    }

    async fn classify_status(
        &self,
        status: http::StatusCode,
        session: Option<&Arc<Session>>,
    ) -> Result<(), CrawlError> {
        let code = status.as_u16();
        if self.session_status_codes.contains(&code) {
            if let Some(session) = session {
                session.mark_bad().await;
            }
            return Err(CrawlError::session(HttpStatusError::new(status)));
        }
        if self.retry_status_codes.contains(&code)
            || (self.retry_server_errors && status.is_server_error())
        {
            return Err(CrawlError::retry(HttpStatusError::new(status)));
        }
        if !status.is_success() && !status.is_redirection() {
            return Err(CrawlError::non_retryable(HttpStatusError::new(status)));
        }
        if let Some(session) = session {
            session.mark_good().await;
        }
        Ok(())
    }

    pub(crate) async fn execute_with_session(
        &self,
        env: RequestEnv<'_>,
        session: Option<Arc<Session>>,
    ) -> Result<BrowserContext, CrawlError> {
        let mut page_opts = PageOpts::new();
        if let Some(session) = &session {
            page_opts = page_opts.session(Arc::clone(session));
        }
        let page = self
            .pool
            .new_page(page_opts)
            .await
            .map_err(BrowserError::classify)?;
        let response = match page.goto(&env.request.url, self.goto.clone()).await {
            Ok(response) => response,
            Err(error) => {
                self.close_after_error(&page, "navigation error").await;
                return Err(error.classify());
            }
        };
        if let Some(status) = response.as_ref().and_then(|response| response.status) {
            if let Err(error) = self.classify_status(status, session.as_ref()).await {
                self.close_after_error(&page, "HTTP status error").await;
                return Err(error);
            }
        } else if let Some(session) = &session {
            session.mark_good().await;
        }
        let storage = match self.storage.get().cloned() {
            Some(storage) => storage,
            None => {
                self.close_after_error(&page, "storage initialization error")
                    .await;
                return Err(CrawlError::critical(anyhow!(
                    "BrowserKind::execute before start"
                )));
            }
        };
        let enqueue = EnqueueLinker::with_extractor(
            env.crawler.clone(),
            &env.request,
            Arc::new(BrowserLinkExtractor { page: page.clone() }),
        );
        Ok(BrowserContext {
            request: env.request.clone(),
            proxy_info: page.proxy_info().cloned(),
            page,
            response,
            session,
            enqueue,
            storage,
            send_request: self.send_request.clone(),
            crawler: env.crawler,
        })
    }
}

/// Configures a [`BrowserKind`].
pub struct BrowserKindBuilder<P: BrowserProvider> {
    provider: P,
    pool_options: BrowserPoolOptions<P::LaunchOptions>,
    session_pool: Option<SessionPoolOptions>,
    shared_sessions: Option<Arc<SessionPool>>,
    http_client: Option<Arc<dyn HttpClient>>,
    retry_status_codes: Vec<u16>,
    retry_server_errors: bool,
    session_status_codes: Vec<u16>,
    navigation_timeout: Duration,
    wait_until: WaitUntil,
}

impl<P: BrowserProvider> BrowserKindBuilder<P> {
    fn new(provider: P) -> Self {
        let pool_options =
            BrowserPoolOptions::default().hooks(BrowserHooks::default().with_session_cookie_sync());
        Self {
            provider,
            pool_options,
            session_pool: Some(SessionPoolOptions::default()),
            shared_sessions: None,
            http_client: None,
            retry_status_codes: vec![408, 429],
            retry_server_errors: true,
            session_status_codes: vec![401, 403],
            navigation_timeout: Duration::from_secs(30),
            wait_until: WaitUntil::Load,
        }
    }

    /// Replaces all browser-pool options.
    pub fn pool_options(mut self, options: BrowserPoolOptions<P::LaunchOptions>) -> Self {
        self.pool_options = options;
        self
    }

    /// Replaces provider-specific launch options.
    pub fn launch_options(mut self, options: P::LaunchOptions) -> Self {
        self.pool_options.launch_options = options;
        self
    }

    /// Sets the maximum simultaneously open pages in one browser.
    pub fn max_open_pages_per_browser(mut self, value: usize) -> Self {
        self.pool_options.max_open_pages_per_browser = value;
        self
    }

    /// Sets the created-page count after which a browser retires.
    pub fn retire_browser_after_page_count(mut self, value: u64) -> Self {
        self.pool_options.retire_browser_after_page_count = value;
        self
    }

    /// Sets the maximum number of live or launching browsers.
    pub fn max_browsers(mut self, value: usize) -> Self {
        self.pool_options.max_browsers = Some(value);
        self
    }

    /// Sets the proxy configuration applied once per browser launch.
    pub fn proxy(mut self, proxy: ProxyConfiguration) -> Self {
        self.pool_options.proxy = Some(proxy);
        self
    }

    /// Replaces the browser-pool hooks.
    ///
    /// This replaces the default session cookie synchronization hooks too. Add them explicitly
    /// with [`BrowserHooks::with_session_cookie_sync`] when custom hooks still need that behavior.
    pub fn hooks(mut self, hooks: BrowserHooks) -> Self {
        self.pool_options.hooks = hooks;
        self
    }

    /// Enables an owned session pool with the supplied options.
    pub fn session_pool(mut self, options: SessionPoolOptions) -> Self {
        self.session_pool = Some(options);
        self.shared_sessions = None;
        self
    }

    /// Disables browser sessions and cookie synchronization.
    pub fn disable_sessions(mut self) -> Self {
        self.session_pool = None;
        self.shared_sessions = None;
        self
    }

    /// Uses a session pool whose persistence lifecycle is managed by its sharing owner.
    pub fn shared_session_pool(mut self, pool: Arc<SessionPool>) -> Self {
        self.shared_sessions = Some(pool);
        self
    }

    /// Injects the HTTP transport exposed as [`BrowserContext::send_request`].
    pub fn http_client(mut self, client: Arc<dyn HttpClient>) -> Self {
        self.http_client = Some(client);
        self
    }

    /// Replaces the exact status codes classified as ordinary retries.
    pub fn retry_status_codes(mut self, codes: impl IntoIterator<Item = u16>) -> Self {
        self.retry_status_codes = codes.into_iter().collect();
        self
    }

    /// Controls whether every 5xx response is retried.
    pub fn retry_server_errors(mut self, enabled: bool) -> Self {
        self.retry_server_errors = enabled;
        self
    }

    /// Replaces statuses that mark and rotate a session.
    pub fn session_status_codes(mut self, codes: impl IntoIterator<Item = u16>) -> Self {
        self.session_status_codes = codes.into_iter().collect();
        self
    }

    /// Sets the browser navigation deadline.
    pub fn navigation_timeout(mut self, timeout: Duration) -> Self {
        self.navigation_timeout = timeout;
        self
    }

    /// Sets the browser lifecycle event awaited after navigation.
    pub fn wait_until(mut self, wait_until: WaitUntil) -> Self {
        self.wait_until = wait_until;
        self
    }

    /// Builds the kind, constructing a typed-error HTTP client when none was injected.
    pub fn build(self) -> Result<BrowserKind<P>, HttpClientError> {
        let send_request = match self.http_client {
            Some(client) => client,
            None => Arc::new(millipede_http::ReqwestClient::new()?),
        };
        let sessions = if let Some(pool) = self.shared_sessions {
            SessionMode::Shared(pool)
        } else if let Some(options) = self.session_pool {
            SessionMode::Owned(Arc::new(SessionPool::new(options)))
        } else {
            SessionMode::Disabled
        };
        Ok(BrowserKind {
            pool: BrowserPool::new(self.provider, self.pool_options),
            sessions,
            send_request,
            retry_status_codes: self.retry_status_codes,
            retry_server_errors: self.retry_server_errors,
            session_status_codes: self.session_status_codes,
            goto: GotoOptions::default()
                .timeout(self.navigation_timeout)
                .wait_until(self.wait_until),
            storage: OnceLock::new(),
            persist_task: Mutex::new(None),
        })
    }
}

/// A crawler using [`BrowserKind`] to render requests in browser pages.
pub type BrowserCrawler<P> = Crawler<BrowserKind<P>>;

impl<P: BrowserProvider> CrawlerKind for BrowserKind<P> {
    type Context = BrowserContext;

    fn start<'a>(&'a self, env: &'a CrawlerEnv) -> BoxFuture<'a, Result<(), CrawlError>> {
        Box::pin(async move {
            let client = env.storage_client().cloned().ok_or_else(|| {
                CrawlError::non_retryable(anyhow!("BrowserKind requires a storage client"))
            })?;
            let kvs = match env.kvs() {
                Some(kvs) => kvs.clone(),
                None => client
                    .open_key_value_store(Some(env.config().default_key_value_store_id()))
                    .await
                    .map_err(|error| CrawlError::retry(anyhow!(error)))?,
            };
            let dataset = client
                .open_dataset(Some(env.config().default_dataset_id()))
                .await
                .map_err(|error| CrawlError::retry(anyhow!(error)))?;
            let queue = env.request_queue().clone();
            let _ = self
                .storage
                .set(StorageHandle::new(client, dataset, kvs.clone(), queue));

            if let SessionMode::Owned(pool) = &self.sessions {
                pool.attach_persistence(kvs);
                pool.restore().await?;
                let pool = Arc::clone(pool);
                let mut events = env.events().subscribe();
                let task = tokio::spawn(async move {
                    loop {
                        match events.recv().await {
                            Ok(CrawlerEvent::PersistState { .. }) => {
                                if let Err(error) = pool.persist().await {
                                    tracing::warn!(%error, "session pool persistence failed");
                                }
                            }
                            Ok(CrawlerEvent::Exiting | CrawlerEvent::Aborting) => break,
                            Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                });
                *self
                    .persist_task
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()) = Some(task);
            }
            Ok(())
        })
    }

    fn execute<'a>(
        &'a self,
        env: RequestEnv<'a>,
    ) -> BoxFuture<'a, Result<Self::Context, CrawlError>> {
        Box::pin(async move {
            let session = if let Some(pool) = self.session_pool() {
                Some(pool.get_session(None).await)
            } else {
                None
            };
            self.execute_with_session(env, session).await
        })
    }

    fn observe(&self, ctx: &Self::Context) -> AttemptObservation {
        let mut observation = AttemptObservation::default();
        observation.status = ctx.response.as_ref().and_then(|response| response.status);
        observation.loaded_url = ctx
            .response
            .as_ref()
            .and_then(|response| response.url.clone())
            .or_else(|| Some(ctx.request.url.clone()));
        observation.session_id = ctx.session.as_ref().map(|session| session.id().clone());
        observation.proxy_info = ctx.proxy_info.clone();
        observation.response_bytes = None;
        observation
    }

    fn cleanup(
        &self,
        outcome: RequestOutcome<Self::Context>,
    ) -> BoxFuture<'_, Result<(), CrawlError>> {
        Box::pin(async move {
            match outcome {
                RequestOutcome::Handled(ctx) => {
                    if let Err(error) = ctx.page.close().await {
                        tracing::warn!(%error, "failed to close handled browser page");
                    }
                }
                RequestOutcome::HandlerFailed { ctx, error } => {
                    if error.rotates_session() {
                        if let Some(session) = &ctx.session {
                            session.mark_bad().await;
                        }
                    }
                    if let Err(error) = ctx.page.close().await {
                        tracing::warn!(%error, "failed to close browser page after handler error");
                    }
                }
                RequestOutcome::ExecuteFailed { .. } => {
                    // Ordinary execute failures close their page before returning. If execute is
                    // cancelled, the last PageHandle drop schedules the pool close worker, whose
                    // strong command reference completes cleanup independently of this future.
                }
            }
            Ok(())
        })
    }

    fn stop<'a>(&'a self, _env: &'a CrawlerEnv) -> BoxFuture<'a, Result<(), CrawlError>> {
        Box::pin(async move {
            if let Some(task) = self
                .persist_task
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                task.abort();
            }
            if let SessionMode::Owned(pool) = &self.sessions {
                if let Err(error) = pool.persist().await {
                    tracing::warn!(%error, "final session pool persistence failed");
                }
            }
            if let Err(error) = self.pool.shutdown().await {
                tracing::warn!(%error, "browser pool shutdown failed");
            }
            Ok(())
        })
    }
}
