use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::future::BoxFuture;
use millipede_core::{
    crawler::{
        AttemptObservation, Crawler, CrawlerEnv, CrawlerHandle, CrawlerKind, RequestEnv,
        RequestOutcome, RequestPrep,
    },
    enqueue::EnqueueLinker,
    errors::CrawlError,
    http_client::{HttpClient, HttpClientError, HttpResponse},
    proxy::{ProxyBuckets, ProxyConfiguration, ProxyInfo, ProxyStrategy},
    request::Request,
    router::HasRequest,
    session::{Session, SessionPoolOptions},
    storage::StorageHandle,
};
use millipede_http::{HttpContext, HttpKind, HttpKindBuilder};

/// A parsed HTML document with the synchronization required for shared handler access.
///
/// This is an explicit deviation from `INTERFACE.md` §4.2 and the Phase 5 roadmap, which specify
/// `Arc<scraper::Html>`. In scraper 0.24.0, the `atomic` feature makes `scraper::Html` `Send`, but
/// not `Sync`: `scraper/src/node.rs` stores element `id` and `classes` caches in
/// `std::cell::OnceCell` and populates them through `&self`; scraper's atomic-gated
/// `html_is_send` test consequently asserts only `Send`. Tendril 0.4.3 likewise provides an
/// `unsafe impl Send` for atomic tendrils, not `Sync`. The specifications need amending before
/// this synchronization boundary can be considered ratified.
///
/// This type owns the required mutex rather than exposing a guard to handlers. Its query methods
/// require owned results, so the lock is always released before a handler can reach an `.await`
/// point. Adding an unsafe `Sync` implementation for direct shared access would be unsound (and
/// the workspace forbids unsafe code in any case).
///
/// `Arc<scraper::Html>` must not accidentally be treated as sendable shared state. This
/// compile-fail guard complements the positive assertions for `SynchronizedHtml`:
///
/// ```compile_fail
/// fn assert_send_sync<T: Send + Sync>() {}
/// assert_send_sync::<std::sync::Arc<scraper::Html>>();
/// ```
pub struct SynchronizedHtml {
    html: Mutex<scraper::Html>,
}

impl SynchronizedHtml {
    fn parse_document(document: &str) -> Self {
        Self {
            html: Mutex::new(scraper::Html::parse_document(document)),
        }
    }

    /// Runs a synchronous query against the parsed document and returns its owned result.
    ///
    /// The callback cannot return data borrowed from the document. Complete the query before an
    /// `.await`, then move the returned value into subsequent asynchronous work.
    ///
    /// # Panics
    ///
    /// Panics if an earlier query callback panicked while holding the document lock.
    pub fn with_html<R>(&self, query: impl FnOnce(&scraper::Html) -> R) -> R {
        let html = self.html.lock().expect("HTML document mutex poisoned");
        query(&html)
    }

    /// Maps every element matching `selector` to an owned value.
    ///
    /// The document lock is released before the returned vector is available to the caller.
    pub fn select<T, F>(&self, selector: &scraper::Selector, mut map: F) -> Vec<T>
    where
        F: for<'a> FnMut(scraper::ElementRef<'a>) -> T,
    {
        self.with_html(|html| html.select(selector).map(&mut map).collect())
    }

    /// Maps the first element matching `selector` to an owned value.
    ///
    /// The document lock is released before the returned option is available to the caller.
    pub fn select_first<T, F>(&self, selector: &scraper::Selector, map: F) -> Option<T>
    where
        F: for<'a> FnOnce(scraper::ElementRef<'a>) -> T,
    {
        self.with_html(|html| html.select(selector).next().map(map))
    }
}

impl fmt::Debug for SynchronizedHtml {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SynchronizedHtml")
            .field(&"<scraper::Html>")
            .finish()
    }
}

/// Per-request context produced by [`HtmlKind`].
///
/// This intentionally differs from INTERFACE §4.2 in three ways. The response is an
/// `Arc<HttpResponse>` because [`CrawlerKind::Context`] must be a cheap aliasing clone and the
/// engine clones a context for each attempt. The HTML field uses [`SynchronizedHtml`] because
/// `scraper::Html` is not `Sync`, as detailed on that type. The proposed `log: Log` field is
/// omitted because no `Log` type exists yet, logging is not scheduled by the roadmap, and Phase
/// 2's `BasicContext` likewise omits it; use `tracing` macros in the meantime.
#[derive(Clone)]
#[non_exhaustive]
pub struct HtmlContext {
    /// Crawl request that produced this context.
    pub request: Arc<Request>,
    /// Final buffered response, shared cheaply across context clones.
    pub response: Arc<HttpResponse>,
    /// Parsed HTML document with guard-free query helpers, shared cheaply across context clones.
    ///
    /// **Unratified API drift:** `INTERFACE.md` §4.2 specifies `Arc<scraper::Html>` here, assuming
    /// scraper's `atomic` feature makes `scraper::Html: Sync`. This does not hold for scraper
    /// 0.24.0 and tendril 0.4.3 as resolved by this workspace: scraper's element caches use
    /// `std::cell::OnceCell`, and atomic tendrils implement `Send` but not `Sync`. Consequently,
    /// `Arc<scraper::Html>` is not `Send` and cannot satisfy [`CrawlerKind::Context`]. This
    /// synchronization boundary requires an `INTERFACE.md` amendment or ADR before dependent
    /// Phase 5 work such as selector-based enqueue extraction and the `scrape_books` example
    /// proceeds.
    pub html: Arc<SynchronizedHtml>,
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
    http: HttpContext,
}

impl HasRequest for HtmlContext {
    fn request(&self) -> &Request {
        &self.request
    }
}

impl fmt::Debug for HtmlContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HtmlContext")
            .field("request", &self.request)
            .field("response_url", &self.response.url)
            .field("response_status", &self.response.status)
            .field("response_headers", &self.response.headers)
            .field("response_body_bytes", &self.response.body.len())
            .field("redirect_chain", &self.response.redirect_chain)
            .field("html", &"<scraper::Html>")
            .field("session", &self.session)
            .field("proxy_info", &self.proxy_info)
            .field("enqueue", &self.enqueue)
            .field("storage", &self.storage)
            .field("crawler", &self.crawler)
            .finish()
    }
}

/// Errors specific to HTML response processing.
#[derive(Debug, thiserror::Error)]
pub enum HtmlError {
    /// The response declares a media type that cannot be parsed as HTML.
    #[error("unsupported content type for HTML parsing: {content_type}")]
    UnsupportedContentType {
        /// Media type declared by the response.
        content_type: String,
    },
}

/// HTML fetching behavior that delegates transport concerns to [`HttpKind`].
pub struct HtmlKind {
    http: HttpKind,
}

impl HtmlKind {
    /// Starts configuring an HTML crawler kind.
    pub fn builder() -> HtmlKindBuilder {
        HtmlKindBuilder::default()
    }

    /// Creates an HTML kind with all defaults.
    pub fn new() -> Result<Self, HttpClientError> {
        Self::builder().build()
    }

    /// Wraps an already configured HTTP kind.
    pub fn from_http(http: HttpKind) -> Self {
        Self { http }
    }
}

/// Configures [`HtmlKind`] by delegating HTTP settings to [`HttpKindBuilder`].
pub struct HtmlKindBuilder {
    http: HttpKindBuilder,
}

impl Default for HtmlKindBuilder {
    fn default() -> Self {
        Self {
            http: HttpKind::builder(),
        }
    }
}

impl HtmlKindBuilder {
    /// Injects the HTTP transport.
    pub fn http_client(mut self, client: Arc<dyn HttpClient>) -> Self {
        self.http = self.http.http_client(client);
        self
    }

    /// Enables or disables optional in-flight request coalescing. It is disabled by default.
    pub fn coalesce_in_flight(mut self, enabled: bool) -> Self {
        self.http = self.http.coalesce_in_flight(enabled);
        self
    }

    /// Enables sessions with the supplied pool options.
    pub fn session_pool(mut self, options: SessionPoolOptions) -> Self {
        self.http = self.http.session_pool(options);
        self
    }

    /// Disables session selection, cookie persistence, and session rotation.
    pub fn disable_sessions(mut self) -> Self {
        self.http = self.http.disable_sessions();
        self
    }

    /// Sets the default proxy bucket.
    pub fn proxy(mut self, proxy: ProxyConfiguration) -> Self {
        self.http = self.http.proxy(proxy);
        self
    }

    /// Replaces all logical proxy buckets.
    pub fn proxy_buckets(mut self, proxies: ProxyBuckets) -> Self {
        self.http = self.http.proxy_buckets(proxies);
        self
    }

    /// Sets the policy that selects a proxy bucket per attempt.
    pub fn proxy_strategy<S: ProxyStrategy>(mut self, strategy: S) -> Self {
        self.http = self.http.proxy_strategy(strategy);
        self
    }

    /// Replaces the rotating User-Agent set.
    pub fn user_agents<I, U>(mut self, user_agents: I) -> Self
    where
        I: IntoIterator<Item = U>,
        U: Into<String>,
    {
        self.http = self.http.user_agents(user_agents);
        self
    }

    /// Replaces the exact status codes classified as ordinary retries.
    pub fn retry_status_codes(mut self, codes: impl IntoIterator<Item = u16>) -> Self {
        self.http = self.http.retry_status_codes(codes);
        self
    }

    /// Controls whether every 5xx response is retried.
    pub fn retry_server_errors(mut self, enabled: bool) -> Self {
        self.http = self.http.retry_server_errors(enabled);
        self
    }

    /// Replaces statuses that trigger the `retry_on_blocked` session-rotation behavior.
    pub fn session_status_codes(mut self, codes: impl IntoIterator<Item = u16>) -> Self {
        self.http = self.http.session_status_codes(codes);
        self
    }

    /// Sets the HTTP request deadline.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.http = self.http.request_timeout(timeout);
        self
    }

    /// Sets the maximum number of redirects followed for one request.
    pub fn max_redirects(mut self, maximum: u32) -> Self {
        self.http = self.http.max_redirects(maximum);
        self
    }

    /// Builds the kind, constructing the default HTTP client when none was injected.
    pub fn build(self) -> Result<HtmlKind, HttpClientError> {
        self.http.build().map(HtmlKind::from_http)
    }
}

/// A crawler using [`HtmlKind`] to fetch and parse HTML documents.
pub type HtmlCrawler = Crawler<HtmlKind>;

impl CrawlerKind for HtmlKind {
    type Context = HtmlContext;

    fn start<'a>(&'a self, env: &'a CrawlerEnv) -> BoxFuture<'a, Result<(), CrawlError>> {
        self.http.start(env)
    }

    fn before_request<'a>(
        &'a self,
        prep: &'a mut RequestPrep,
    ) -> BoxFuture<'a, Result<(), CrawlError>> {
        self.http.before_request(prep)
    }

    fn execute<'a>(
        &'a self,
        env: RequestEnv<'a>,
    ) -> BoxFuture<'a, Result<Self::Context, CrawlError>> {
        Box::pin(async move {
            let http_ctx = self.http.execute(env).await?;
            if let Some(value) = http_ctx.response.headers.get(http::header::CONTENT_TYPE) {
                let content_type = String::from_utf8_lossy(value.as_bytes()).into_owned();
                let media_type = content_type.split(';').next().unwrap_or_default().trim();
                if !media_type.eq_ignore_ascii_case("text/html")
                    && !media_type.eq_ignore_ascii_case("application/xhtml+xml")
                {
                    return Err(CrawlError::non_retryable(
                        HtmlError::UnsupportedContentType { content_type },
                    ));
                }
            }

            // Parse once for handler queries; Phase 5 benchmarks may choose a separate streaming
            // extractor for engine-owned link discovery without changing this handler API.
            let html = Arc::new(SynchronizedHtml::parse_document(&http_ctx.response.text()));
            Ok(HtmlContext {
                request: http_ctx.request.clone(),
                response: http_ctx.response.clone(),
                html,
                session: http_ctx.session.clone(),
                proxy_info: http_ctx.proxy_info.clone(),
                enqueue: http_ctx.enqueue.clone(),
                storage: http_ctx.storage.clone(),
                crawler: http_ctx.crawler.clone(),
                http: http_ctx,
            })
        })
    }

    fn observe(&self, ctx: &Self::Context) -> AttemptObservation {
        self.http.observe(&ctx.http)
    }

    fn after_success<'a>(
        &'a self,
        ctx: &'a mut Self::Context,
    ) -> BoxFuture<'a, Result<(), CrawlError>> {
        self.http.after_success(&mut ctx.http)
    }

    fn cleanup(
        &self,
        outcome: RequestOutcome<Self::Context>,
    ) -> BoxFuture<'_, Result<(), CrawlError>> {
        let outcome = match outcome {
            RequestOutcome::Handled(ctx) => RequestOutcome::Handled(ctx.http),
            RequestOutcome::HandlerFailed { ctx, error } => RequestOutcome::HandlerFailed {
                ctx: ctx.http,
                error,
            },
            RequestOutcome::ExecuteFailed { request, error } => {
                RequestOutcome::ExecuteFailed { request, error }
            }
        };
        self.http.cleanup(outcome)
    }

    fn stop<'a>(&'a self, env: &'a CrawlerEnv) -> BoxFuture<'a, Result<(), CrawlError>> {
        self.http.stop(env)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>() {}

    fn assert_send_sync<T: Send + Sync>() {}

    fn assert_ctx<T: Send + Clone + 'static>() {}

    #[test]
    fn context_types_satisfy_engine_bounds() {
        assert_send::<scraper::Html>();
        assert_send_sync::<SynchronizedHtml>();
        assert_send_sync::<Arc<SynchronizedHtml>>();
        assert_ctx::<HtmlContext>();
    }
}
