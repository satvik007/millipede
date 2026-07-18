//! HTTP-first crawler kind with selective browser promotion.

use std::{
    collections::HashSet,
    fmt,
    sync::{Arc, Mutex},
};

use futures_util::future::BoxFuture;
use millipede_core::{
    crawler::{
        AttemptObservation, Crawler, CrawlerEnv, CrawlerHandle, CrawlerKind, RequestEnv,
        RequestOutcome,
    },
    enqueue::EnqueueLinker,
    errors::CrawlError,
    events::CrawlerEvent,
    http_client::HttpClientError,
    request::Request,
    router::HasRequest,
    session::{SessionPool, SessionPoolOptions},
    storage::StorageHandle,
};
use millipede_html::{HtmlContext, HtmlKind, HtmlKindBuilder};

use crate::{
    BrowserContext, BrowserKind, BrowserKindBuilder, BrowserProvider,
    detect::{
        BrowserPromotionDetector, DefaultPromotionDetector, HttpAttemptSnapshot, PromotionReason,
    },
};

/// A context produced by either the HTTP/HTML or browser execution path.
#[derive(Clone)]
#[non_exhaustive]
pub enum SmartContext {
    /// Successful HTTP response with parsed HTML.
    Http(HtmlContext),
    /// Browser-rendered page.
    Browser(BrowserContext),
}

impl SmartContext {
    /// Returns the request that produced this context.
    pub fn request(&self) -> &Request {
        HasRequest::request(self)
    }

    /// Returns this context's URL-enqueue helper.
    pub fn enqueue(&self) -> &EnqueueLinker {
        match self {
            Self::Http(ctx) => &ctx.enqueue,
            Self::Browser(ctx) => &ctx.enqueue,
        }
    }

    /// Returns this context's open storage handles.
    pub fn storage(&self) -> &StorageHandle {
        match self {
            Self::Http(ctx) => &ctx.storage,
            Self::Browser(ctx) => &ctx.storage,
        }
    }

    /// Returns the weak handle to the running crawler.
    pub fn crawler(&self) -> &CrawlerHandle {
        match self {
            Self::Http(ctx) => &ctx.crawler,
            Self::Browser(ctx) => &ctx.crawler,
        }
    }

    /// Borrows the HTTP context when this request stayed on the HTTP path.
    pub fn as_http(&self) -> Option<&HtmlContext> {
        match self {
            Self::Http(ctx) => Some(ctx),
            Self::Browser(_) => None,
        }
    }

    /// Borrows the browser context when this request was promoted.
    pub fn as_browser(&self) -> Option<&BrowserContext> {
        match self {
            Self::Http(_) => None,
            Self::Browser(ctx) => Some(ctx),
        }
    }

    /// Returns whether this request executed through a browser.
    pub fn is_browser(&self) -> bool {
        matches!(self, Self::Browser(_))
    }
}

impl HasRequest for SmartContext {
    fn request(&self) -> &Request {
        match self {
            Self::Http(ctx) => ctx.request(),
            Self::Browser(ctx) => ctx.request(),
        }
    }
}

impl fmt::Debug for SmartContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(ctx) => formatter
                .debug_tuple("SmartContext::Http")
                .field(ctx)
                .finish(),
            Self::Browser(ctx) => formatter
                .debug_tuple("SmartContext::Browser")
                .field(ctx)
                .finish(),
        }
    }
}

/// HTTP-first execution with conservative browser promotion.
///
/// On the success path the detector inspects the response body and never promotes on status
/// alone. On the error path `HttpKind` has already classified statuses such as 403 and 503 before
/// an HTML context exists, so [`CrawlError::http_status`] is the only available signal. The
/// default status list is deliberately small (`[403, 503]`): 429 means rate limiting, which a
/// browser does not fix. The list is configurable, and sticky per-host promotion bounds repeated
/// HTTP-first costs.
pub struct SmartKind<P: BrowserProvider> {
    html: HtmlKind,
    browser: BrowserKind<P>,
    detector: Arc<dyn BrowserPromotionDetector>,
    promote_status_codes: Vec<u16>,
    sticky: bool,
    promoted_hosts: Mutex<HashSet<String>>,
    sessions: Option<Arc<SessionPool>>,
    persist_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl<P: BrowserProvider> SmartKind<P> {
    /// Starts configuring smart HTTP-first crawling backed by `provider`.
    pub fn builder(provider: P) -> SmartKindBuilder<P> {
        SmartKindBuilder::new(provider)
    }

    fn record_promoted_host(&self, host: Option<String>) {
        if self.sticky {
            if let Some(host) = host {
                self.promoted_hosts
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .insert(host);
            }
        }
    }
}

/// Configures [`SmartKind`].
#[must_use = "builders do nothing unless consumed by build"]
pub struct SmartKindBuilder<P: BrowserProvider> {
    html: HtmlKindBuilder,
    browser: BrowserKindBuilder<P>,
    detector: Arc<dyn BrowserPromotionDetector>,
    promote_status_codes: Vec<u16>,
    sticky: bool,
    session_pool: Option<SessionPoolOptions>,
}

impl<P: BrowserProvider> SmartKindBuilder<P> {
    fn new(provider: P) -> Self {
        Self {
            html: HtmlKind::builder(),
            browser: BrowserKind::builder(provider),
            detector: Arc::new(DefaultPromotionDetector::default()),
            promote_status_codes: vec![403, 503],
            sticky: true,
            session_pool: Some(SessionPoolOptions::default()),
        }
    }

    /// Replaces the complete HTML-kind configuration.
    pub fn html_kind(mut self, builder: HtmlKindBuilder) -> Self {
        self.html = builder;
        self
    }

    /// Replaces the complete browser-kind configuration.
    pub fn browser_kind(mut self, builder: BrowserKindBuilder<P>) -> Self {
        self.browser = builder;
        self
    }

    /// Replaces the browser-promotion detector.
    pub fn detector<D: BrowserPromotionDetector>(mut self, detector: D) -> Self {
        self.detector = Arc::new(detector);
        self
    }

    /// Replaces HTTP error statuses that trigger browser promotion.
    pub fn promote_status_codes(mut self, codes: impl IntoIterator<Item = u16>) -> Self {
        self.promote_status_codes = codes.into_iter().collect();
        self
    }

    /// Controls whether one promotion makes subsequent requests to that host browser-first.
    pub fn sticky_promotion(mut self, enabled: bool) -> Self {
        self.sticky = enabled;
        self
    }

    /// Enables the one shared session pool with the supplied options.
    pub fn session_pool(mut self, options: SessionPoolOptions) -> Self {
        self.session_pool = Some(options);
        self
    }

    /// Disables sessions on both execution paths.
    pub fn disable_sessions(mut self) -> Self {
        self.session_pool = None;
        self
    }

    /// Builds the smart kind and unifies session state across both execution paths.
    pub fn build(self) -> Result<SmartKind<P>, HttpClientError> {
        let (html, browser, sessions) = if let Some(options) = self.session_pool {
            let pool = Arc::new(SessionPool::new(options));
            let html = self.html.shared_session_pool(Arc::clone(&pool)).build()?;
            let browser = self
                .browser
                .shared_session_pool(Arc::clone(&pool))
                .build()?;
            (html, browser, Some(pool))
        } else {
            let html = self.html.disable_sessions().build()?;
            let browser = self.browser.disable_sessions().build()?;
            (html, browser, None)
        };
        Ok(SmartKind {
            html,
            browser,
            detector: self.detector,
            promote_status_codes: self.promote_status_codes,
            sticky: self.sticky,
            promoted_hosts: Mutex::new(HashSet::new()),
            sessions,
            persist_task: Mutex::new(None),
        })
    }
}

/// A crawler using [`SmartKind`] for HTTP-first browser promotion.
pub type SmartCrawler<P> = Crawler<SmartKind<P>>;

impl<P: BrowserProvider> CrawlerKind for SmartKind<P> {
    type Context = SmartContext;

    fn start<'a>(&'a self, env: &'a CrawlerEnv) -> BoxFuture<'a, Result<(), CrawlError>> {
        Box::pin(async move {
            self.html.start(env).await?;
            self.browser.start(env).await?;
            if let Some(pool) = &self.sessions {
                let kvs = env.kvs().cloned().ok_or_else(|| {
                    CrawlError::critical(anyhow::anyhow!(
                        "SmartKind requires an initialized key-value store"
                    ))
                })?;
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
            let host = env.request.url.host_str().map(str::to_owned);
            let sticky_promoted = if self.sticky {
                if let Some(host) = &host {
                    self.promoted_hosts
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .contains(host)
                } else {
                    false
                }
            } else {
                false
            };
            if sticky_promoted {
                // Sticky and error-path promotions intentionally use a fresh
                // session (`execute` re-checks out): there is no successful HTTP
                // attempt whose session state is worth carrying over. Only the
                // success-path promotion below reuses the HTTP attempt's session.
                return self.browser.execute(env).await.map(SmartContext::Browser);
            }

            match self.html.execute(env.duplicate()).await {
                Ok(html_ctx) => {
                    let snapshot = HttpAttemptSnapshot::new(
                        &html_ctx.request,
                        html_ctx.response.status,
                        &html_ctx.response.headers,
                        &html_ctx.response.body,
                        &html_ctx.response.url,
                        Some(&html_ctx.html),
                    );
                    if let Some(reason) = self.detector.should_promote(&snapshot) {
                        tracing::info!(%reason, url = %env.request.url, "promoting request to browser");
                        self.record_promoted_host(host);
                        self.browser
                            .execute_with_session(env, html_ctx.session.clone())
                            .await
                            .map(SmartContext::Browser)
                    } else {
                        Ok(SmartContext::Http(html_ctx))
                    }
                }
                Err(error) => {
                    let promoted_status = error
                        .http_status()
                        .filter(|status| self.promote_status_codes.contains(&status.as_u16()));
                    if let Some(status) = promoted_status {
                        let reason = PromotionReason::StatusPromoted {
                            status: status.as_u16(),
                        };
                        tracing::info!(%reason, url = %env.request.url, "promoting request to browser");
                        self.record_promoted_host(host);
                        self.browser.execute(env).await.map(SmartContext::Browser)
                    } else {
                        Err(error)
                    }
                }
            }
        })
    }

    fn observe(&self, ctx: &Self::Context) -> AttemptObservation {
        match ctx {
            SmartContext::Http(ctx) => self.html.observe(ctx),
            SmartContext::Browser(ctx) => self.browser.observe(ctx),
        }
    }

    fn after_success<'a>(
        &'a self,
        ctx: &'a mut Self::Context,
    ) -> BoxFuture<'a, Result<(), CrawlError>> {
        match ctx {
            SmartContext::Http(ctx) => self.html.after_success(ctx),
            SmartContext::Browser(ctx) => self.browser.after_success(ctx),
        }
    }

    fn cleanup(
        &self,
        outcome: RequestOutcome<Self::Context>,
    ) -> BoxFuture<'_, Result<(), CrawlError>> {
        match outcome {
            RequestOutcome::Handled(SmartContext::Http(ctx)) => {
                self.html.cleanup(RequestOutcome::Handled(ctx))
            }
            RequestOutcome::Handled(SmartContext::Browser(ctx)) => {
                self.browser.cleanup(RequestOutcome::Handled(ctx))
            }
            RequestOutcome::HandlerFailed {
                ctx: SmartContext::Http(ctx),
                error,
            } => self
                .html
                .cleanup(RequestOutcome::HandlerFailed { ctx, error }),
            RequestOutcome::HandlerFailed {
                ctx: SmartContext::Browser(ctx),
                error,
            } => self
                .browser
                .cleanup(RequestOutcome::HandlerFailed { ctx, error }),
            RequestOutcome::ExecuteFailed { request, error } => {
                // Both inner execute-failure cleanups are no-ops; route through browser cleanup.
                self.browser
                    .cleanup(RequestOutcome::ExecuteFailed { request, error })
            }
        }
    }

    fn stop<'a>(&'a self, env: &'a CrawlerEnv) -> BoxFuture<'a, Result<(), CrawlError>> {
        Box::pin(async move {
            if let Some(task) = self
                .persist_task
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                task.abort();
            }
            if let Some(pool) = &self.sessions {
                if let Err(error) = pool.persist().await {
                    tracing::warn!(%error, "final session pool persistence failed");
                }
            }
            let html_result = self.html.stop(env).await;
            let browser_result = self.browser.stop(env).await;
            html_result?;
            browser_result
        })
    }
}
