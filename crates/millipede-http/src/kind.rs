use std::{
    collections::hash_map::DefaultHasher,
    fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::anyhow;
use futures_util::future::BoxFuture;
use http::{HeaderValue, StatusCode, header::USER_AGENT};
use millipede_core::{
    antibot::{AntiBotDetector, AntiBotSignals, DefaultAntiBotDetector},
    crawler::{
        AttemptObservation, Crawler, CrawlerEnv, CrawlerHandle, CrawlerKind, RequestEnv,
        RequestOutcome,
    },
    enqueue::EnqueueLinker,
    errors::CrawlError,
    events::CrawlerEvent,
    http_client::{HttpClient, HttpClientError, HttpRequest, HttpResponse, HttpStatusError},
    proxy::{
        ProxyBuckets, ProxyConfiguration, ProxyInfo, ProxyKind, ProxyResolveContext,
        ProxyRouteContext, ProxyStrategy,
    },
    request::Request,
    router::HasRequest,
    session::{Session, SessionPool, SessionPoolOptions},
    storage::StorageHandle,
};

use crate::{CoalescingClient, ReqwestClient};

/// Parses `Retry-After`, capped at ten minutes as the header-trust ceiling.
///
/// The core-side 429 penalty has its own separate five-minute cap.
fn parse_retry_after(headers: &http::HeaderMap, now: time::OffsetDateTime) -> Option<Duration> {
    const MAX_RETRY_AFTER: Duration = Duration::from_secs(600);

    let value = headers
        .get(http::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        let seconds = value.parse::<u64>().unwrap_or(u64::MAX);
        return Some(Duration::from_secs(seconds.min(MAX_RETRY_AFTER.as_secs())));
    }

    let date_value = if let Some(prefix) = value.strip_suffix(" GMT") {
        format!("{prefix} +0000")
    } else {
        value.to_owned()
    };
    let date =
        time::OffsetDateTime::parse(&date_value, &time::format_description::well_known::Rfc2822)
            .ok()?;
    if date <= now {
        return Some(Duration::ZERO);
    }
    let duration = Duration::try_from(date - now).ok()?;
    Some(duration.min(MAX_RETRY_AFTER))
}

/// Per-request context produced by [`HttpKind`].
///
/// This intentionally differs from INTERFACE §4.2 in two ways. The response is an
/// `Arc<HttpResponse>` because [`CrawlerKind::Context`] must be a cheap aliasing clone and the
/// engine clones a context for each attempt. The proposed `log: Log` field is omitted because no
/// `Log` type exists yet, logging is not scheduled by the roadmap, and Phase 2's `BasicContext`
/// likewise omits it; use `tracing` macros in the meantime.
///
/// The response cookie headers from every redirect hop have already been stored in the session's
/// cookie jar by [`ReqwestClient`] during the send. No separate cookie extraction is needed.
#[derive(Clone)]
#[non_exhaustive]
pub struct HttpContext {
    /// Crawl request that produced this context.
    pub request: Arc<Request>,
    /// Final buffered response, shared cheaply across context clones.
    pub response: Arc<HttpResponse>,
    /// Session used for this attempt, if sessions are enabled.
    pub session: Option<Arc<Session>>,
    /// Proxy selected for this attempt.
    pub proxy_info: Option<ProxyInfo>,
    /// URL enqueue helper linked to the running crawler.
    pub enqueue: EnqueueLinker,
    /// Open default storage resources. For example, `ctx.storage.dataset().push(&item)` works
    /// when [`millipede_core::storage::DatasetExt`] is in scope.
    pub storage: StorageHandle,
    /// Weak handle back to the running crawler.
    pub crawler: CrawlerHandle,
}

impl HasRequest for HttpContext {
    fn request(&self) -> &Request {
        &self.request
    }
}

impl fmt::Debug for HttpContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpContext")
            .field("request", &self.request)
            .field("response_url", &self.response.url)
            .field("response_status", &self.response.status)
            .field("response_headers", &self.response.headers)
            .field("response_body_bytes", &self.response.body.len())
            .field("redirect_chain", &self.response.redirect_chain)
            .field("session", &self.session)
            .field("proxy_info", &self.proxy_info)
            .field("enqueue", &self.enqueue)
            .field("storage", &self.storage)
            .field("crawler", &self.crawler)
            .finish()
    }
}

/// HTTP fetching behavior used by [`HttpCrawler`].
///
/// # End-to-end example
///
/// ```no_run
/// use std::sync::Arc;
/// use millipede_core::{crawler::Crawler, request::Request};
/// use millipede_http::{HttpContext, HttpKind};
/// use millipede_storage_memory::MemoryStorageClient;
///
/// # async fn crawl() -> Result<(), Box<dyn std::error::Error>> {
/// let crawler = Crawler::builder(HttpKind::builder().build()?)
///     .request_handler(|ctx: HttpContext| async move {
///         println!("{} {}", ctx.response.status, ctx.request.url);
///         Ok(())
///     })
///     .storage_client(Arc::new(MemoryStorageClient::new()))
///     .build()
///     .await?;
///
/// let stats = crawler.run([Request::get("https://example.com").build()?]).await?;
/// assert_eq!(stats.requests_finished, 1);
/// # Ok(())
/// # }
/// ```
pub struct HttpKind {
    client: Arc<dyn HttpClient>,
    sessions: SessionMode,
    proxies: ProxyBuckets,
    proxy_strategy: Option<Arc<dyn ProxyStrategy>>,
    user_agents: Vec<String>,
    ua_cursor: AtomicUsize,
    retry_status_codes: Vec<u16>,
    retry_server_errors: bool,
    session_status_codes: Vec<u16>,
    request_timeout: Duration,
    max_redirects: u32,
    detect_anti_bot: Option<Arc<dyn AntiBotDetector>>,
    header_generator: bool,
    snapshot_errors: bool,
    pre_hooks: Vec<crate::nav::HttpPreNavigationHook>,
    post_hooks: Vec<crate::nav::HttpPostNavigationHook>,
    storage: OnceLock<StorageHandle>,
    persist_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

enum SessionMode {
    Disabled,
    Owned(Arc<SessionPool>),
    Shared(Arc<SessionPool>),
}

impl HttpKind {
    /// Starts configuring an HTTP crawler kind.
    pub fn builder() -> HttpKindBuilder {
        HttpKindBuilder::default()
    }

    /// Creates an HTTP kind with all defaults.
    pub fn new() -> Result<Self, HttpClientError> {
        Self::builder().build()
    }

    fn session_pool(&self) -> Option<&Arc<SessionPool>> {
        match &self.sessions {
            SessionMode::Owned(pool) | SessionMode::Shared(pool) => Some(pool),
            SessionMode::Disabled => None,
        }
    }

    fn classify_client_error(error: HttpClientError) -> CrawlError {
        match error {
            HttpClientError::Connect(_) | HttpClientError::Timeout(_) | HttpClientError::Io(_) => {
                CrawlError::retry(error)
            }
            HttpClientError::Build(_)
            | HttpClientError::InvalidRequest(_)
            | HttpClientError::Redirect(_)
            | HttpClientError::Decode(_)
            | HttpClientError::Other(_) => CrawlError::non_retryable(error),
            _ => CrawlError::non_retryable(error),
        }
    }

    async fn classify_status(
        &self,
        status: StatusCode,
        retry_after: Option<Duration>,
        session: Option<&Arc<Session>>,
        proxy: Option<&ProxyConfiguration>,
        target: &url::Url,
    ) -> Result<(), CrawlError> {
        let status_error = |status: StatusCode| {
            let error = HttpStatusError::new(status);
            match retry_after {
                Some(duration) => error.with_retry_after(duration),
                None => error,
            }
        };
        let code = status.as_u16();
        if self.session_status_codes.contains(&code) {
            if let Some(session) = session {
                session.mark_bad().await;
            }
            if let Some(proxy) = proxy {
                proxy.report_blocked(target);
            }
            return Err(CrawlError::session(status_error(status)));
        }
        if self.retry_status_codes.contains(&code)
            || (self.retry_server_errors && status.is_server_error())
        {
            return Err(CrawlError::retry(status_error(status)));
        }
        if !status.is_success() && !status.is_redirection() {
            return Err(CrawlError::non_retryable(status_error(status)));
        }
        if let Some(session) = session {
            session.mark_good().await;
        }
        if let Some(proxy) = proxy {
            proxy.report_success(target);
        }
        Ok(())
    }
}

/// Configures [`HttpKind`].
pub struct HttpKindBuilder {
    http_client: Option<Arc<dyn HttpClient>>,
    coalesce_in_flight: bool,
    session_pool: Option<SessionPoolOptions>,
    shared_session_pool: Option<Arc<SessionPool>>,
    proxies: ProxyBuckets,
    proxy_strategy: Option<Arc<dyn ProxyStrategy>>,
    user_agents: Vec<String>,
    retry_status_codes: Vec<u16>,
    retry_server_errors: bool,
    session_status_codes: Vec<u16>,
    request_timeout: Duration,
    max_redirects: u32,
    detect_anti_bot: Option<Arc<dyn AntiBotDetector>>,
    header_generator: bool,
    snapshot_errors: bool,
    pre_hooks: Vec<crate::nav::HttpPreNavigationHook>,
    post_hooks: Vec<crate::nav::HttpPostNavigationHook>,
}

impl Default for HttpKindBuilder {
    fn default() -> Self {
        Self {
            http_client: None,
            coalesce_in_flight: false,
            session_pool: Some(SessionPoolOptions::default()),
            shared_session_pool: None,
            proxies: ProxyBuckets::default(),
            proxy_strategy: None,
            user_agents: vec!["millipede/0.1 (+https://github.com/satvik007/millipede)".to_owned()],
            retry_status_codes: vec![408, 429],
            retry_server_errors: true,
            session_status_codes: vec![401, 403],
            request_timeout: Duration::from_secs(30),
            max_redirects: 10,
            detect_anti_bot: None,
            header_generator: false,
            snapshot_errors: false,
            pre_hooks: Vec::new(),
            post_hooks: Vec::new(),
        }
    }
}

impl HttpKindBuilder {
    /// Injects the HTTP transport.
    pub fn http_client(mut self, client: Arc<dyn HttpClient>) -> Self {
        self.http_client = Some(client);
        self
    }

    /// Enables or disables optional in-flight request coalescing. It is disabled by default.
    pub fn coalesce_in_flight(mut self, enabled: bool) -> Self {
        self.coalesce_in_flight = enabled;
        self
    }

    /// Enables sessions with the supplied pool options.
    pub fn session_pool(mut self, options: SessionPoolOptions) -> Self {
        self.session_pool = Some(options);
        self
    }

    /// Uses an existing session pool without managing its persistence lifecycle.
    ///
    /// With a shared pool, the component that owns the sharing (for example, the Phase 6 smart
    /// kind) attaches persistence exactly once. Otherwise, two kinds would double-restore and
    /// double-persist the same pool.
    pub fn shared_session_pool(mut self, pool: Arc<SessionPool>) -> Self {
        self.shared_session_pool = Some(pool);
        self
    }

    /// Disables session selection, cookie persistence, and session rotation.
    pub fn disable_sessions(mut self) -> Self {
        self.session_pool = None;
        self
    }

    /// Sets the default proxy bucket.
    pub fn proxy(mut self, proxy: ProxyConfiguration) -> Self {
        self.proxies = std::mem::take(&mut self.proxies).with_default(proxy);
        self
    }

    /// Replaces all logical proxy buckets.
    pub fn proxy_buckets(mut self, proxies: ProxyBuckets) -> Self {
        self.proxies = proxies;
        self
    }

    /// Sets the policy that selects a proxy bucket per attempt.
    pub fn proxy_strategy<S: ProxyStrategy>(mut self, strategy: S) -> Self {
        self.proxy_strategy = Some(Arc::new(strategy));
        self
    }

    /// Replaces the rotating User-Agent set.
    pub fn user_agents<I, U>(mut self, user_agents: I) -> Self
    where
        I: IntoIterator<Item = U>,
        U: Into<String>,
    {
        self.user_agents = user_agents.into_iter().map(Into::into).collect();
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

    /// Replaces statuses that trigger the `retry_on_blocked` session-rotation behavior.
    ///
    /// The default is 401 and 403. An empty list disables status-driven session rotation.
    pub fn session_status_codes(mut self, codes: impl IntoIterator<Item = u16>) -> Self {
        self.session_status_codes = codes.into_iter().collect();
        self
    }

    /// Sets the HTTP request deadline corresponding to §20.5's `navigation_timeout` option.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Sets the maximum number of redirects followed for one request.
    pub fn max_redirects(mut self, maximum: u32) -> Self {
        self.max_redirects = maximum;
        self
    }

    /// Enables response inspection with a caller-supplied anti-bot detector.
    pub fn detect_anti_bot(mut self, detector: Arc<dyn AntiBotDetector>) -> Self {
        self.detect_anti_bot = Some(detector);
        self
    }

    /// Opts into the default anti-bot detector.
    ///
    /// Detection is off by default so SmartKind promotion and default crawls are unaffected.
    pub fn detect_anti_bot_default(self) -> Self {
        self.detect_anti_bot(Arc::new(DefaultAntiBotDetector::new()))
    }

    /// Enables or disables deterministic browser-like request headers.
    pub fn header_generator(mut self, enabled: bool) -> Self {
        self.header_generator = enabled;
        self
    }

    /// Enables or disables response-body snapshots when handlers fail.
    pub fn snapshot_errors_on_failure(mut self, enabled: bool) -> Self {
        self.snapshot_errors = enabled;
        self
    }

    /// Registers an HTTP hook that runs immediately before navigation.
    pub fn pre_navigation_hook<F>(mut self, hook: F) -> Self
    where
        F: for<'a> Fn(
                crate::nav::HttpPreHookCtx<'a>,
            ) -> futures_util::future::BoxFuture<'a, Result<(), CrawlError>>
            + Send
            + Sync
            + 'static,
    {
        self.pre_hooks.push(Arc::new(hook));
        self
    }

    /// Registers an HTTP hook that runs after navigation.
    pub fn post_navigation_hook<F>(mut self, hook: F) -> Self
    where
        F: for<'a> Fn(
                crate::nav::HttpPostHookCtx<'a>,
            ) -> futures_util::future::BoxFuture<'a, Result<(), CrawlError>>
            + Send
            + Sync
            + 'static,
    {
        self.post_hooks.push(Arc::new(hook));
        self
    }

    /// Builds the kind, constructing a typed-error [`ReqwestClient`] when none was injected.
    pub fn build(self) -> Result<HttpKind, HttpClientError> {
        let client = match self.http_client {
            Some(client) => client,
            None => Arc::new(ReqwestClient::new()?),
        };
        let client: Arc<dyn HttpClient> = if self.coalesce_in_flight {
            Arc::new(CoalescingClient::new(client))
        } else {
            client
        };
        let sessions = if let Some(pool) = self.shared_session_pool {
            SessionMode::Shared(pool)
        } else if let Some(options) = self.session_pool {
            SessionMode::Owned(Arc::new(SessionPool::new(options)))
        } else {
            SessionMode::Disabled
        };
        Ok(HttpKind {
            client,
            sessions,
            proxies: self.proxies,
            proxy_strategy: self.proxy_strategy,
            user_agents: self.user_agents,
            ua_cursor: AtomicUsize::new(0),
            retry_status_codes: self.retry_status_codes,
            retry_server_errors: self.retry_server_errors,
            session_status_codes: self.session_status_codes,
            request_timeout: self.request_timeout,
            max_redirects: self.max_redirects,
            detect_anti_bot: self.detect_anti_bot,
            header_generator: self.header_generator,
            snapshot_errors: self.snapshot_errors,
            pre_hooks: self.pre_hooks,
            post_hooks: self.post_hooks,
            storage: OnceLock::new(),
            persist_task: Mutex::new(None),
        })
    }
}

/// A crawler using [`HttpKind`] to fetch raw HTTP responses.
pub type HttpCrawler = Crawler<HttpKind>;

impl CrawlerKind for HttpKind {
    type Context = HttpContext;

    fn start<'a>(&'a self, env: &'a CrawlerEnv) -> BoxFuture<'a, Result<(), CrawlError>> {
        Box::pin(async move {
            let client = env.storage_client().cloned().ok_or_else(|| {
                CrawlError::non_retryable(anyhow!("HttpKind requires a storage client"))
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
            let attempt = env.request.retry_count;
            let kind = if let Some(kind) = env.overrides.proxy_kind.clone() {
                kind
            } else if let Some(strategy) = &self.proxy_strategy {
                let mut context = ProxyRouteContext::new(&env.request, attempt);
                if let Some(profile) = env.overrides.user_agent_profile.as_deref() {
                    context = context.previous_profile_key(profile);
                }
                strategy.route(&context)
            } else {
                ProxyKind::Default
            };
            let proxy_cfg = self.proxies.for_kind(&kind);
            let resolved = if let Some(proxy_cfg) = proxy_cfg {
                let mut context = ProxyResolveContext::new()
                    .request(&env.request)
                    .attempt(attempt);
                if let Some(session) = &session {
                    context = context.session_id(session.id());
                }
                proxy_cfg.new_proxy_info(context).await?
            } else {
                None
            };
            let (proxy_info, proxy_url) = match resolved {
                Some(info) => (Some(info.clone()), Some(info.url)),
                None => (None, None),
            };

            let mut http_request = HttpRequest::from_request(&env.request)
                .timeout(self.request_timeout)
                .max_redirects(self.max_redirects);
            if let Some(session) = &session {
                http_request = http_request.cookie_jar(session.cookie_jar().clone());
            }
            if let Some(proxy_url) = proxy_url {
                http_request = http_request.proxy(proxy_url);
            }

            // A retry directive is an explicit per-attempt identity change and therefore wins even
            // over a User-Agent header supplied by the original request.
            if let Some(user_agent) = &env.overrides.user_agent_profile {
                let value = HeaderValue::from_str(user_agent).map_err(|error| {
                    CrawlError::non_retryable(HttpClientError::invalid_request(error))
                })?;
                http_request.headers.insert(USER_AGENT, value);
            } else if !http_request.headers.contains_key(USER_AGENT) && !self.user_agents.is_empty()
            {
                let index = if let Some(session) = &session {
                    let mut hasher = DefaultHasher::new();
                    session.id().as_str().as_bytes().hash(&mut hasher);
                    hasher.finish() as usize % self.user_agents.len()
                } else {
                    self.ua_cursor.fetch_add(1, Ordering::Relaxed) % self.user_agents.len()
                };
                let value = HeaderValue::from_str(&self.user_agents[index]).map_err(|error| {
                    CrawlError::non_retryable(HttpClientError::invalid_request(error))
                })?;
                http_request.headers.insert(USER_AGENT, value);
            }

            if self.header_generator {
                let token = if let Some(session) = &session {
                    millipede_core::session::SessionToken::from(session.id())
                } else {
                    millipede_core::session::SessionToken::new(env.request.unique_key.clone())
                };
                http_request = http_request.use_header_generator(true).session_token(token);
            }

            for hook in &self.pre_hooks {
                hook(crate::nav::HttpPreHookCtx {
                    request: &env.request,
                    http_request: &mut http_request,
                    session: session.as_deref(),
                    proxy: proxy_info.as_ref(),
                })
                .await?;
            }

            let response = self
                .client
                .send(http_request)
                .await
                .map_err(Self::classify_client_error)?;
            if let Some(detector) = &self.detect_anti_bot {
                let signals = AntiBotSignals::new(
                    response.status,
                    &response.headers,
                    &response.body,
                    &response.url,
                );
                if let Some(tech) = detector.detect(&signals) {
                    if let Some(session) = &session {
                        session.mark_bad().await;
                    }
                    return Err(CrawlError::AntiBotDetected {
                        tech,
                        source: anyhow!("response body matched a known anti-bot challenge marker"),
                    });
                }
            }
            for hook in &self.post_hooks {
                hook(crate::nav::HttpPostHookCtx {
                    request: &env.request,
                    response: &response,
                    session: session.as_deref(),
                    proxy: proxy_info.as_ref(),
                })
                .await?;
            }
            let retry_after = parse_retry_after(&response.headers, time::OffsetDateTime::now_utc());
            self.classify_status(
                response.status,
                retry_after,
                session.as_ref(),
                proxy_cfg,
                &env.request.url,
            )
            .await?;
            let storage =
                self.storage.get().cloned().ok_or_else(|| {
                    CrawlError::critical(anyhow!("HttpKind::execute before start"))
                })?;
            Ok(HttpContext {
                request: env.request.clone(),
                response: Arc::new(response),
                session,
                proxy_info,
                enqueue: EnqueueLinker::new(env.crawler.clone(), &env.request),
                storage,
                crawler: env.crawler,
            })
        })
    }

    fn observe(&self, ctx: &Self::Context) -> AttemptObservation {
        let mut observation = AttemptObservation::default();
        observation.status = Some(ctx.response.status);
        observation.loaded_url = Some(ctx.response.url.clone());
        observation.session_id = ctx.session.as_ref().map(|session| session.id().clone());
        observation.proxy_info = ctx.proxy_info.clone();
        observation.response_bytes = Some(ctx.response.body.len());
        observation
    }

    fn cleanup(
        &self,
        outcome: RequestOutcome<Self::Context>,
    ) -> BoxFuture<'_, Result<(), CrawlError>> {
        Box::pin(async move {
            if let RequestOutcome::HandlerFailed { ctx, error } = outcome {
                if error.rotates_session() {
                    if let Some(session) = &ctx.session {
                        session.mark_bad().await;
                    }
                }
                if self.snapshot_errors {
                    let snapshotter = millipede_core::snapshot::ErrorSnapshotter::new(
                        ctx.storage.key_value_store().clone(),
                    );
                    let content_type = ctx
                        .response
                        .headers
                        .get(http::header::CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("application/octet-stream")
                        .to_owned();
                    if let Err(snapshot_error) = snapshotter
                        .capture(
                            &ctx.request,
                            "body",
                            ctx.response.body.clone(),
                            &content_type,
                        )
                        .await
                    {
                        tracing::warn!(%snapshot_error, "error snapshot capture failed");
                    }
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
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn only_success_and_redirection_statuses_are_successful() {
        let kind = HttpKind::builder()
            .disable_sessions()
            .retry_status_codes([])
            .retry_server_errors(false)
            .build()
            .expect("default HTTP client must build");
        let target = url::Url::parse("https://example.com/").expect("test URL must parse");

        for status in [
            StatusCode::CONTINUE,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::from_u16(700).expect("nonstandard status must parse"),
        ] {
            let error = kind
                .classify_status(status, None, None, None, &target)
                .await
                .expect_err("non-2xx/3xx status must fail");
            assert!(matches!(error, CrawlError::NonRetryable(_)));
        }

        for status in [StatusCode::OK, StatusCode::FOUND] {
            kind.classify_status(status, None, None, None, &target)
                .await
                .expect("2xx/3xx status must succeed");
        }
    }

    #[test]
    fn retry_after_delta_seconds() {
        let headers = http::HeaderMap::from_iter([(
            http::header::RETRY_AFTER,
            HeaderValue::from_static("120"),
        )]);

        assert_eq!(
            parse_retry_after(&headers, time::OffsetDateTime::UNIX_EPOCH),
            Some(Duration::from_secs(120))
        );
    }

    #[test]
    fn retry_after_overflow_is_capped() {
        let headers = http::HeaderMap::from_iter([(
            http::header::RETRY_AFTER,
            HeaderValue::from_static("9999999999999999999999999999999999999999"),
        )]);

        assert_eq!(
            parse_retry_after(&headers, time::OffsetDateTime::UNIX_EPOCH),
            Some(Duration::from_secs(600))
        );
    }

    #[test]
    fn retry_after_http_date() {
        let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000)
            .expect("fixed timestamp must be valid");
        let formatted = (now + time::Duration::seconds(90))
            .format(&time::format_description::well_known::Rfc2822)
            .expect("HTTP date must format");
        let value = formatted
            .strip_suffix(" +0000")
            .map(|prefix| format!("{prefix} GMT"))
            .expect("UTC RFC 2822 date must have a numeric zone");
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::RETRY_AFTER,
            HeaderValue::from_str(&value).expect("HTTP date must be a header value"),
        );

        let parsed = parse_retry_after(&headers, now).expect("HTTP date must parse");
        assert!(parsed.abs_diff(Duration::from_secs(90)) <= Duration::from_secs(1));
    }

    #[test]
    fn retry_after_past_date_is_zero() {
        let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000)
            .expect("fixed timestamp must be valid");
        let formatted = (now - time::Duration::seconds(90))
            .format(&time::format_description::well_known::Rfc2822)
            .expect("HTTP date must format");
        let value = formatted
            .strip_suffix(" +0000")
            .map(|prefix| format!("{prefix} GMT"))
            .expect("UTC RFC 2822 date must have a numeric zone");
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::RETRY_AFTER,
            HeaderValue::from_str(&value).expect("HTTP date must be a header value"),
        );

        assert_eq!(parse_retry_after(&headers, now), Some(Duration::ZERO));
    }

    #[test]
    fn retry_after_garbage_is_ignored() {
        for value in [
            HeaderValue::from_static("soon"),
            HeaderValue::from_static(""),
            HeaderValue::from_bytes(b"\xff").expect("opaque header bytes must be accepted"),
        ] {
            let headers = http::HeaderMap::from_iter([(http::header::RETRY_AFTER, value)]);
            assert_eq!(
                parse_retry_after(&headers, time::OffsetDateTime::UNIX_EPOCH),
                None
            );
        }
    }

    #[test]
    fn retry_after_absent_is_ignored() {
        assert_eq!(
            parse_retry_after(&http::HeaderMap::new(), time::OffsetDateTime::UNIX_EPOCH),
            None
        );
    }
}
