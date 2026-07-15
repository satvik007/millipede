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
    sessions: Option<Arc<SessionPool>>,
    proxies: ProxyBuckets,
    proxy_strategy: Option<Arc<dyn ProxyStrategy>>,
    user_agents: Vec<String>,
    ua_cursor: AtomicUsize,
    retry_status_codes: Vec<u16>,
    retry_server_errors: bool,
    session_status_codes: Vec<u16>,
    request_timeout: Duration,
    max_redirects: u32,
    storage: OnceLock<StorageHandle>,
    persist_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
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
        session: Option<&Arc<Session>>,
        proxy: Option<&ProxyConfiguration>,
        target: &url::Url,
    ) -> Result<(), CrawlError> {
        let code = status.as_u16();
        if self.session_status_codes.contains(&code) {
            if let Some(session) = session {
                session.mark_bad().await;
            }
            if let Some(proxy) = proxy {
                proxy.report_blocked(target);
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
    proxies: ProxyBuckets,
    proxy_strategy: Option<Arc<dyn ProxyStrategy>>,
    user_agents: Vec<String>,
    retry_status_codes: Vec<u16>,
    retry_server_errors: bool,
    session_status_codes: Vec<u16>,
    request_timeout: Duration,
    max_redirects: u32,
}

impl Default for HttpKindBuilder {
    fn default() -> Self {
        Self {
            http_client: None,
            coalesce_in_flight: false,
            session_pool: Some(SessionPoolOptions::default()),
            proxies: ProxyBuckets::default(),
            proxy_strategy: None,
            user_agents: vec!["millipede/0.1 (+https://github.com/satvik007/millipede)".to_owned()],
            retry_status_codes: vec![408, 429],
            retry_server_errors: true,
            session_status_codes: vec![401, 403],
            request_timeout: Duration::from_secs(30),
            max_redirects: 10,
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
        Ok(HttpKind {
            client,
            sessions: self
                .session_pool
                .map(|options| Arc::new(SessionPool::new(options))),
            proxies: self.proxies,
            proxy_strategy: self.proxy_strategy,
            user_agents: self.user_agents,
            ua_cursor: AtomicUsize::new(0),
            retry_status_codes: self.retry_status_codes,
            retry_server_errors: self.retry_server_errors,
            session_status_codes: self.session_status_codes,
            request_timeout: self.request_timeout,
            max_redirects: self.max_redirects,
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

            if let Some(pool) = &self.sessions {
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
            let session = if let Some(pool) = &self.sessions {
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

            let response = self
                .client
                .send(http_request)
                .await
                .map_err(Self::classify_client_error)?;
            self.classify_status(
                response.status,
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
            if let Some(pool) = &self.sessions {
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
                .classify_status(status, None, None, &target)
                .await
                .expect_err("non-2xx/3xx status must fail");
            assert!(matches!(error, CrawlError::NonRetryable(_)));
        }

        for status in [StatusCode::OK, StatusCode::FOUND] {
            kind.classify_status(status, None, None, &target)
                .await
                .expect("2xx/3xx status must succeed");
        }
    }
}
