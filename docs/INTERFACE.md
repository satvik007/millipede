# Millipede — Idiomatic Rust Interface Design

> A web crawling & scraping library for Rust, inspired by [Apify's Crawlee](https://github.com/apify/crawlee) but rebuilt around Rust's ownership, async, and trait-based polymorphism.

This document is a **design spec for the public API and core abstractions**. Implementation details (concrete data structures, locking strategies, schedulers) live in `ROADMAP.md`.

---

## 1. Design Principles

The Rust port is not a one-to-one transliteration of Crawlee. The TypeScript design relies heavily on JS-only idioms — class inheritance with widening generic contexts, dual-protocol return values (Promise + AsyncIterable), `this`-bound closures, module-level singletons, EventEmitter back-channels. Each of these maps awkwardly to Rust. The principles below state how we resolve those tensions.

1. **Composition over inheritance.** Crawlee's `BasicCrawler → HttpCrawler → CheerioCrawler` chain becomes one generic `Crawler<Kind>` engine plus orthogonal traits (`Fetcher`, `Parser`, `BrowserProvider`). Crawler "flavors" are type aliases or thin builders, not subclasses.
2. **Static dispatch by default, dynamic at the boundary.** Hot-path types (the request handler, fetcher, parser) are generic. Pluggable backends crossing crate or user boundaries (storage clients, proxy resolvers, browser providers) are `Arc<dyn Trait>`.
3. **No global mutable state.** Crawlee's `Configuration.getGlobalConfig()` and `AsyncLocalStorage` are replaced by explicit dependency injection via the builder. There is no "ambient" crawler.
4. **Errors are typed, retries are explicit.** A single `CrawlError` enum encodes retry semantics (`Retry`, `Session`, `NonRetryable`, `Critical`). Handlers return `Result<(), CrawlError>`; the engine never inspects message strings to decide what to do.
5. **Lifetimes are minimal in user-facing APIs.** Handler signatures use `'static` futures with owned context; sharing is via `Arc`. Internal modules may use borrowed lifetimes where it's safe.
6. **Tokio-only for v1.** A `runtime` feature flag may later allow `async-std` or `smol`, but the v1 API is `tokio::spawn`-shaped (`Send + 'static` futures).
7. **`enqueue_links`, routing, and link filtering are first-class** — these are the daily-driver ergonomics that make Crawlee pleasant. Anything that compromises them is wrong.
8. **Streaming results are first-class.** A crawler that only writes through handlers is awkward for pipelines. Users must be able to subscribe to completed-request snapshots as they arrive, without blocking the crawl on a slow consumer.
9. **Operational policies are explicit types.** Depth limits, robots, budgets, frontier ordering, domain throttling, retry strategy, and proxy routing are not hidden in callbacks or globals. They are builder-owned policies with testable behavior.

### 1.1 Adjacent Rust crawler lessons (`spider-rs/spider`)

Spider validates several Millipede choices by contrast: a single all-purpose crawler struct grows quickly, synthetic HTTP status codes are a poor substitute for typed errors, and broad default features make dependency control hard. Millipede keeps the multi-crate split, explicit configuration, typed `CrawlError`, and minimal defaults.

The useful patterns to lift are operational, not architectural: real-time result streaming, a small happy-path builder, all-atomic AIMD concurrency as the first autoscaling mode, borrowed retry/proxy strategy contexts, domain-round-robin frontier policy, in-flight request coalescing, streaming link extraction for the engine hot path, and RAII browser page cleanup.

The anti-patterns to avoid are equally important: global env-driven semaphores, shared tuple task contexts accessed by numeric index, unsafe client-build shortcuts, and a large feature matrix in the user-facing crate.

---

## 2. Workspace & Crate Layout

```
millipede/                          # repo root (this directory)
├── crates/
│   ├── millipede-core/              # Engine: Request, queues, autoscale, sessions, proxy,
│   │                                 # storage traits, router, events, errors, statistics.
│   ├── millipede-storage-memory/    # Default in-memory StorageClient.
│   ├── millipede-storage-fs/        # File-system StorageClient (parity w/ MemoryStorage on disk).
│   ├── millipede-http/              # HttpCrawler — reqwest-based fetcher.
│   ├── millipede-html/              # HtmlCrawler — adds `scraper` (Cheerio-equivalent) parsing.
│   ├── millipede-browser/           # BrowserCrawler core + BrowserProvider trait, BrowserPool.
│   ├── millipede-browser-chromiumoxide/  # Chromium driver via `chromiumoxide`.
│   ├── millipede-browser-playwright/     # Optional: Playwright via `playwright-rust`.
│   ├── millipede-fingerprint/       # Browser-like header generator, TLS fingerprinting hooks.
│   └── millipede-cli/               # `millipede create` scaffolder (optional, post-MVP).
├── millipede/                        # Umbrella crate — re-exports a curated public API.
├── examples/
└── docs/
```

`millipede` is the user-facing crate. The split crates exist so projects can avoid pulling in browser/CDP dependencies when only HTTP crawling is needed. Default features in `millipede` pull `http` + `html` + `storage-memory`; `browser`, `browser-chromiumoxide`, and `storage-fs` are opt-in.

---

## 3. The Request Model

```rust
// crates/millipede-core/src/request.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub id: RequestId,
    pub url: Url,
    pub loaded_url: Option<Url>,
    pub unique_key: String,
    pub method: Method,                  // re-exported http::Method
    pub headers: HeaderMap,
    pub body: Option<RequestBody>,       // bytes | form | json
    pub user_data: UserData,             // typed wrapper around serde_json::Value
    pub label: Option<String>,
    pub retry_count: u32,
    pub session_rotation_count: u32,
    pub max_retries: Option<u32>,
    pub no_retry: bool,
    pub error_messages: Vec<String>,
    pub handled_at: Option<OffsetDateTime>,
    pub state: RequestState,
    pub crawl_depth: u32,
    pub skip_navigation: bool,
    pub enqueue_strategy: Option<EnqueueStrategy>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestState {
    Unprocessed, BeforeNav, AfterNav, RequestHandler,
    Done, ErrorHandler, Error, Skipped,
}

pub struct RequestBuilder { /* … */ }

impl Request {
    pub fn get(url: impl IntoUrl) -> RequestBuilder { … }
    pub fn post(url: impl IntoUrl) -> RequestBuilder { … }
    pub fn builder() -> RequestBuilder { … }
}

// RequestBuilder: url, method, headers, body, label, user_data, max_retries,
// no_retry, skip_navigation, unique_key (override), forefront (queue position).
//   .build() -> Request
//   .enqueue(&queue) -> Result<RequestQueueOperationInfo>  // convenience
```

`UserData` is a thin wrapper to enforce that label-routing semantics work:

```rust
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UserData(pub serde_json::Map<String, serde_json::Value>);

impl UserData {
    pub fn get_typed<T: DeserializeOwned>(&self, key: &str) -> Option<Result<T, serde_json::Error>>;
    pub fn set_typed<T: Serialize>(&mut self, key: &str, value: &T) -> Result<()>;
}
```

For most users, `Request::get(url).label("listing").user_data(("page", 3)).build()` is sufficient.

---

## 4. The Crawler Engine

### 4.1 Top-level: `Crawler<K>` and the `CrawlerKind` lifecycle

A single private engine drives every flavor of crawl. Flavors plug in via the `CrawlerKind` trait, which owns the *full per-request lifecycle*, not just context construction. (Codex review #2: `make_context` is too narrow — HTTP and browser crawlers do substantial pre/post work around the fetch.)

```rust
// crates/millipede-core/src/crawler.rs

pub struct Crawler<K: CrawlerKind> {
    kind: Arc<K>,
    inner: Arc<CrawlerInner>,   // queue, sessions, proxy, autoscaler, stats, events, config
}

pub trait CrawlerKind: Send + Sync + 'static {
    /// The fully-typed context passed to user handlers.
    type Context: Send + 'static;

    /// Called once at `Crawler::run()` start, before any request is fetched.
    /// Useful for warming a browser pool, opening shared connections, etc.
    fn start(&self, env: &CrawlerEnv) -> BoxFuture<'_, Result<(), CrawlError>> {
        let _ = env; Box::pin(async { Ok(()) })
    }

    /// Mutate the `Request` before the fetch (apply headers, set session,
    /// choose proxy). Engine-supplied default: no-op.
    fn before_request<'a>(&'a self, prep: &'a mut RequestPrep)
        -> BoxFuture<'a, Result<(), CrawlError>>
    {
        let _ = prep; Box::pin(async { Ok(()) })
    }

    /// Execute the fetch/navigation and build the user-visible context.
    /// This is the core: HTTP performs the GET, HTML adds parsing, Browser
    /// performs the goto and exposes a Page.
    fn execute<'a>(&'a self, env: RequestEnv<'a>)
        -> BoxFuture<'a, Result<Self::Context, CrawlError>>;

    /// Called after the user handler returns `Ok(())`. The kind can extract
    /// cookies from the page back into the session, save response snapshots, etc.
    fn after_success<'a>(&'a self, ctx: &'a mut Self::Context)
        -> BoxFuture<'a, Result<(), CrawlError>>
    {
        let _ = ctx; Box::pin(async { Ok(()) })
    }

    /// Always called after a request finishes, regardless of outcome.
    /// Used for releasing browser pages, closing streams, retiring sessions.
    fn cleanup(&self, outcome: RequestOutcome<Self::Context>)
        -> BoxFuture<'_, Result<(), CrawlError>>;

    /// Called once at `Crawler::run()` shutdown.
    fn stop(&self, env: &CrawlerEnv) -> BoxFuture<'_, Result<(), CrawlError>> {
        let _ = env; Box::pin(async { Ok(()) })
    }
}

/// Engine-supplied scratch space for `before_request`.
pub struct RequestPrep {
    pub request: Request,
    pub session: Option<Arc<Session>>,
    pub proxy: Option<ProxyInfo>,
    pub headers_to_add: HeaderMap,
}

/// Everything the engine has at `execute` time. Borrowed for the duration
/// of one fetch.
pub struct RequestEnv<'a> {
    pub request: Arc<Request>,
    pub session: Option<Arc<Session>>,
    pub proxy: Option<ProxyInfo>,
    pub http: &'a Arc<dyn HttpClient>,
    pub storage: &'a StorageHandle,
    pub enqueue: EnqueueLinker,
    pub log: Log,
    pub crawler: CrawlerHandle,   // weak handle for ctx.crawler — see §4.4
}

/// What the engine hands to `cleanup`.
pub enum RequestOutcome<C> {
    Handled(C),
    HandlerFailed { ctx: C, error: CrawlError },
    ExecuteFailed { request: Arc<Request>, error: CrawlError },
}

/// Process-global engine-state view (queue depth, stats, events, config).
pub struct CrawlerEnv { /* … */ }
```

The default `start`/`before_request`/`after_success`/`stop` impls are no-ops; a kind only overrides the steps it cares about. `execute` and `cleanup` are required.

### 4.2 Three concrete crawler kinds

The naming convention is: the **Kind** is `XxxKind`, the **Crawler** is `XxxCrawler = Crawler<XxxKind>`. Codex review #3 flagged the earlier ambiguity (`HtmlCrawler` doubling as both).

```rust
// HTTP — fetches raw bytes, gives them to the handler.
pub struct HttpKind { http_client: Arc<dyn HttpClient> }
impl CrawlerKind for HttpKind {
    type Context = HttpContext;
    fn execute<'a>(&'a self, env: RequestEnv<'a>) -> BoxFuture<'a, Result<HttpContext, CrawlError>> { … }
    fn cleanup(&self, _: RequestOutcome<HttpContext>) -> BoxFuture<'_, Result<(), CrawlError>> { … }
}
pub type HttpCrawler = Crawler<HttpKind>;

pub struct HttpContext {
    pub request: Arc<Request>,
    pub response: HttpResponse,        // status, headers, body bytes, redirect chain
    pub session: Option<Arc<Session>>,
    pub proxy_info: Option<ProxyInfo>,
    pub enqueue: EnqueueLinker,
    pub storage: StorageHandle,
    pub crawler: CrawlerHandle,
    pub log: Log,
}

// HTML — wraps HttpKind and adds `scraper`-based parsing.
pub struct HtmlKind { http: HttpKind }
impl CrawlerKind for HtmlKind {
    type Context = HtmlContext;
    // `execute` calls `self.http.execute(...)` then parses the body once.
}
pub type HtmlCrawler = Crawler<HtmlKind>;

pub struct HtmlContext {
    pub request: Arc<Request>,
    pub response: HttpResponse,
    pub html: Arc<scraper::Html>,      // owned Arc so it can move into spawned tasks
    pub session: Option<Arc<Session>>,
    pub proxy_info: Option<ProxyInfo>,
    pub enqueue: EnqueueLinker,
    pub storage: StorageHandle,
    pub crawler: CrawlerHandle,
    pub log: Log,
}

// Browser — drives a real browser. Provider generic stays inside the kind;
// users see a stable `PageHandle` (Codex review #5).
pub struct BrowserKind<P: BrowserProvider> { pool: Arc<BrowserPool<P>> }
impl<P: BrowserProvider> CrawlerKind for BrowserKind<P> {
    type Context = BrowserContext;
}
pub type BrowserCrawler<P> = Crawler<BrowserKind<P>>;

pub struct BrowserContext {
    pub request: Arc<Request>,
    pub page: PageHandle,              // provider-erased; see §12.2
    pub response: Option<BrowserResponse>,
    pub session: Option<Arc<Session>>,
    pub proxy_info: Option<ProxyInfo>,
    pub enqueue: EnqueueLinker,
    pub storage: StorageHandle,
    pub send_request: Arc<dyn HttpClient>,
    pub crawler: CrawlerHandle,
    pub log: Log,
}
```

Why three concrete `Context`s rather than one widening trait? Each carries different *owned* state (parsed HTML, browser page), and downstream code wants to pattern-match or call concrete methods on it. Trying to express this with a single trait would force `Box<dyn Any>` for the parsed body or a sum type — both worse than three structs.

### 4.3 `CrawlerHandle` — back-reference without self-borrow

Some user code legitimately needs to enqueue extra requests from a handler, query live stats, or stop the crawler from inside. Rather than embedding `&self` into the context (which would force lifetimes everywhere), we hand out a cheaply-cloned weak handle:

```rust
#[derive(Clone)]
pub struct CrawlerHandle { inner: Weak<CrawlerInner> }
impl CrawlerHandle {
    pub async fn add_requests<I: IntoIterator<Item = Request>>(&self, reqs: I)
        -> Result<BatchAddHandle>;
    pub fn stats(&self) -> Option<StatisticsSnapshot>;
    pub async fn stop(&self) -> Result<()>;
    pub fn events(&self) -> Option<EventStream>;
    pub fn results(&self) -> Option<ResultStream>;
}
```

`Weak` means dropping the crawler can't be blocked by a handle outliving a handler.

### 4.3 Building & running

```rust
// User code:

let crawler = HtmlCrawler::builder()
    .max_concurrency(20)
    .max_request_retries(3)
    .request_handler(router)         // see §6
    .failed_request_handler(|ctx| async move { … })
    .pre_navigation_hook(|req, sess| async move { … })
    .session_pool(SessionPoolOptions::default().max_pool_size(100))
    .proxy(ProxyConfiguration::round_robin(proxy_urls))
    .build()?;

let stats = crawler.run(["https://crawlee.dev"]).await?;
println!("{} requests handled", stats.requests_finished);
```

`run()` accepts anything implementing `IntoStartRequests`: `&str`, `Url`, iterators thereof, `Vec<Request>`, or a `RequestSource` (sitemap URL, URL list file).

```rust
impl<K: CrawlerKind> Crawler<K> {
    pub async fn run(&self, start: impl IntoStartRequests) -> Result<FinalStatistics, CrawlError>;
    pub fn results(&self) -> ResultStream;         // completed-request snapshots
    pub async fn add_requests(&self, reqs: impl IntoIterator<Item = Request>) -> Result<()>;
    pub async fn stop(&self) -> Result<()>;       // graceful drain
    pub async fn abort(&self) -> Result<()>;      // immediate shutdown
    pub fn stats(&self) -> StatisticsHandle;      // live snapshot
    pub fn events(&self) -> EventStream;          // tokio broadcast subscriber
}

pub type ResultStream = tokio::sync::broadcast::Receiver<HandledRequest>;

/// Lightweight snapshot emitted after handler + cleanup. This intentionally
/// does not expose the full `Context`: browser pages may already be closed,
/// and handlers consume their owned context.
#[derive(Debug, Clone)]
pub struct HandledRequest {
    pub request: Arc<Request>,
    pub loaded_url: Option<Url>,
    pub outcome: RequestFinalState,
    pub response_status: Option<StatusCode>,
    pub retry_count: u32,
    pub duration: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestFinalState { Succeeded, Failed, Skipped }
```

`results()` is the Rust replacement for Spider's `subscribe()` ergonomics without making `run()` itself a dual-protocol API. Handlers remain the primary place to extract data; `ResultStream` is for pipelines, progress UIs, queues, metrics sidecars, and tests that need to observe completed work as it lands.

---

## 5. Request Handlers

```rust
pub trait RequestHandler<C>: Send + Sync + 'static {
    fn handle(&self, ctx: C) -> BoxFuture<'static, Result<(), CrawlError>>;
}

// Blanket impl for closures returning a future.
impl<C, F, Fut> RequestHandler<C> for F
where
    F: Fn(C) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), CrawlError>> + Send + 'static,
{ … }
```

This is the only handler shape. No `this`-bound callbacks, no shared mutable closures. Handlers that need shared state capture an `Arc<Mutex<…>>`.

```rust
// Example
crawler.request_handler(|ctx: HtmlContext| async move {
    let title = ctx.html.select(&Selector::parse("title").unwrap())
                       .next()
                       .map(|el| el.text().collect::<String>())
                       .unwrap_or_default();
    ctx.storage.dataset().push(serde_json::json!({
        "url": ctx.request.url.to_string(),
        "title": title,
    })).await?;
    ctx.enqueue.same_origin().await?;
    Ok(())
});
```

---

## 6. Router

```rust
// crates/millipede-core/src/router.rs

pub struct Router<C> {
    routes: Vec<Route<C>>,                       // ordered; first match wins
    default: Option<Arc<dyn RequestHandler<C>>>,
    middleware: Vec<Arc<dyn Middleware<C>>>,
}

struct Route<C> {
    label: Option<String>,             // None = match any label
    methods: MethodFilter,             // see below
    handler: Arc<dyn RequestHandler<C>>,
}

pub enum MethodFilter {
    Any,
    Only(SmallVec<[Method; 4]>),
}

impl<C: HasRequest + Send + 'static> Router<C> {
    pub fn new() -> Self;

    /// Match every request with this label, any method.
    pub fn route<H: RequestHandler<C>>(self, label: impl Into<String>, h: H) -> Self;

    /// Match a (label, single method) tuple. Equivalent to
    /// `.route_methods(label, [method], h)`.
    pub fn route_method<H: RequestHandler<C>>(
        self, label: impl Into<String>, method: Method, h: H
    ) -> Self;

    /// Match a label only when the request uses one of the listed methods.
    /// Mirrors Crawlee's `router.addHandler('detail', 'POST', …)`.
    pub fn route_methods<H, I>(self, label: impl Into<String>, methods: I, h: H) -> Self
    where
        H: RequestHandler<C>,
        I: IntoIterator<Item = Method>;

    /// Fallback handler used when no route matches.
    pub fn default<H: RequestHandler<C>>(self, h: H) -> Self;

    pub fn middleware<M: Middleware<C>>(self, m: M) -> Self;
}

// The router IS a handler.
impl<C: HasRequest + Send + 'static> RequestHandler<C> for Router<C> {
    fn handle(&self, ctx: C) -> BoxFuture<'static, Result<(), CrawlError>> { … }
}

// HasRequest exposes label + method lookup without making `Router` know each
// context's type. Implemented by HttpContext / HtmlContext / BrowserContext.
pub trait HasRequest {
    fn request(&self) -> &Request;
}
```

Usage:

```rust
let router = Router::<HtmlContext>::new()
    .route("listing", listing_page)
    .route_method("detail", Method::GET, detail_page)
    .route_method("detail", Method::POST, detail_submit)
    .route_methods("api", [Method::GET, Method::HEAD], api_probe)
    .default(|ctx| async move { /* fallback */ Ok(()) });

crawler.request_handler(router);
```

Dispatch order:
1. Walk `routes` in registration order. For each, the label must match (or be `None`) AND the method must match the filter.
2. If no route matches and a `default` is set, run it.
3. Otherwise, return `CrawlError::MissingRoute { label, method }` (matches Crawlee's `MissingRouteError` semantics).

Registration order matters: more specific (method-filtered) routes should be registered *before* the catch-all label route to avoid being shadowed. The builder logs a warning at `build()` time when a later route is unreachable.

---

## 7. Link Extraction, `EnqueueLinker`, and `CrawlPolicy`

Crawlee's `enqueueLinks()` is the API users touch most often, and it carries *more* than just URL extraction: crawl-depth tracking, robots.txt filtering, `maxRequestsPerCrawl`, `maxCrawlDepth`, `respectRobotsTxtFile`, redirect-skip behavior, and skipped-request callbacks. We model this as a long-lived `CrawlPolicy` owned by the crawler plus a per-context `EnqueueLinker` that consults it.

### 7.1 `CrawlPolicy` — set on the builder, read on every enqueue

```rust
pub struct CrawlPolicy {
    pub strategy: EnqueueStrategy,                  // default: SameHostname
    pub max_crawl_depth: Option<u32>,               // None = unbounded
    pub max_requests_per_crawl: Option<u64>,
    pub robots: RobotsPolicy,
    pub on_skipped: Option<Arc<dyn SkippedHandler>>,
}

pub enum RobotsPolicy {
    Ignore,
    Respect { user_agent: String, cache: RobotsCache },
}

pub enum EnqueueStrategy { All, SameHostname, SameDomain, SameOrigin }

#[async_trait]
pub trait SkippedHandler: Send + Sync + 'static {
    async fn on_skip(&self, request: Request, reason: SkipReason);
}

#[derive(Debug, Clone)]
pub enum SkipReason {
    MaxDepthExceeded { depth: u32, limit: u32 },
    MaxRequestsReached { limit: u64 },
    RobotsDisallowed,
    StrategyExcluded { strategy: EnqueueStrategy },
    GlobExcluded,
    RegexExcluded,
    TransformRejected { reason: String },
    DuplicateUniqueKey,
}
```

Set on the crawler builder:

```rust
HtmlCrawler::builder()
    .crawl_policy(CrawlPolicy::new()
        .strategy(EnqueueStrategy::SameDomain)
        .max_crawl_depth(5)
        .max_requests_per_crawl(10_000)
        .respect_robots("MillipedeBot/0.1"))
    .build()?;
```

The engine reads `max_requests_per_crawl` as part of its `is_finished` check; `max_crawl_depth` and `robots` are read inside `EnqueueLinker::send()`.

### 7.2 `EnqueueLinker` — per-context

```rust
// Available on every Context as `ctx.enqueue`
pub struct EnqueueLinker {
    queue: Arc<dyn RequestQueue>,
    policy: Arc<CrawlPolicy>,
    parent: RequestMeta,                  // url, depth, label of the page we're on
    extractor: Option<Arc<dyn LinkExtractor>>,
}

impl EnqueueLinker {
    pub fn options(&self) -> EnqueueLinksOptions<'_>;
    pub async fn all(&self) -> Result<EnqueueResult>;
    pub async fn same_origin(&self) -> Result<EnqueueResult>;
    pub async fn same_hostname(&self) -> Result<EnqueueResult>;
    pub async fn same_domain(&self) -> Result<EnqueueResult>;
    pub async fn urls<U: IntoIterator<Item = Url>>(&self, urls: U) -> Result<EnqueueResult>;
}

pub struct EnqueueResult {
    pub added: Vec<ProcessedRequest>,
    pub skipped: Vec<(Url, SkipReason)>,
}

pub struct EnqueueLinksOptions<'a> { /* fluent builder */ }
impl<'a> EnqueueLinksOptions<'a> {
    pub fn selector(self, css: impl Into<String>) -> Self;       // HtmlContext / BrowserContext
    pub fn strategy(self, s: EnqueueStrategy) -> Self;           // overrides policy.strategy
    pub fn globs<G: Into<GlobPattern>>(self, g: impl IntoIterator<Item = G>) -> Self;
    pub fn regex(self, patterns: impl IntoIterator<Item = Regex>) -> Self;
    pub fn exclude<P: Into<UrlPattern>>(self, p: impl IntoIterator<Item = P>) -> Self;
    pub fn label(self, label: impl Into<String>) -> Self;
    pub fn user_data(self, ud: UserData) -> Self;

    /// Async transform with a typed result — can mutate, reject (with reason),
    /// or rewrite the URL after a DB lookup. Returning `Skip` calls the
    /// policy's `on_skipped` handler.
    pub fn transform<F>(self, f: F) -> Self
    where
        F: for<'r> Fn(&'r mut Request) -> BoxFuture<'r, TransformResult> + Send + Sync + 'static;

    pub fn limit(self, n: usize) -> Self;            // per-call cap (independent of policy)
    pub fn forefront(self, b: bool) -> Self;
    pub fn base_url(self, base: Url) -> Self;
    pub async fn send(self) -> Result<EnqueueResult>;
}

pub enum TransformResult {
    Enqueue,                          // keep the request as-is (possibly mutated by `&mut`)
    Skip { reason: String },          // becomes SkipReason::TransformRejected
}
```

Inside `EnqueueLinker::send()` (in order, short-circuiting on first reject):

1. Extract candidate URLs (via `selector` or `<a href>` walk).
2. Apply `EnqueueStrategy` (same-domain/hostname/origin filter).
3. Apply `globs` / `regex` includes and `exclude` patterns.
4. For each surviving URL, build a `Request` with `crawl_depth = parent.depth + 1`.
5. Check `policy.max_crawl_depth` — if exceeded, emit `SkipReason::MaxDepthExceeded`.
6. Check robots.txt (cached per host) — if disallowed, emit `SkipReason::RobotsDisallowed`.
7. Run the async `transform` if any.
8. Enqueue via `RequestQueue::add_batch`; the queue itself detects duplicate `unique_key`s and emits `SkipReason::DuplicateUniqueKey` for them.

`SkippedHandler` is invoked for every skip — users can wire it into a counter, a CSV log, or a Slack alert without inventing their own machinery.

### 7.3 Patterns and per-pattern overrides

`GlobPattern` accepts either a `&str` minimatch-style glob, a `Regex`, or a structured `UrlMatch { pattern, label, user_data, method, headers }` — matching Crawlee's per-pattern overrides. Patterns surviving the strategy filter inherit any structured override before reaching the transform step.

For `HttpContext` (no DOM), only `EnqueueLinker::urls(...)` is available; selector-based extraction returns a compile-time error because the extractor is `None`.

Implementation note: `HtmlContext` exposes `Arc<scraper::Html>` for ergonomic handler queries, but the engine is not required to use `scraper` for its own link-discovery pass. Phase 5 benchmarks `scraper` against a streaming `lol_html` extractor with precompiled selectors; if streaming extraction materially reduces memory or latency, `EnqueueLinker` uses it internally while preserving the public `scraper::Html` handler API.

---

## 8. Storage

### 8.1 `StorageClient` trait

All three storage traits are exposed as `Arc<dyn Trait>` for backend pluggability. That forces them to be **object-safe**: no generic methods, no `Self`-returning methods, no `impl Trait` returns. The user-friendly typed methods (`push::<T>`, `get::<T>`) live on blanket extension traits that work for every `T` against any object-safe core. (Codex review #1 was that the original trait shapes were not object-safe.)

```rust
// crates/millipede-core/src/storage/mod.rs

#[async_trait]
pub trait StorageClient: Send + Sync + 'static {
    async fn open_dataset(&self, name: Option<&str>) -> Result<Arc<dyn Dataset>>;
    async fn open_key_value_store(&self, name: Option<&str>) -> Result<Arc<dyn KeyValueStore>>;
    async fn open_request_queue(&self, name: Option<&str>) -> Result<Arc<dyn RequestQueue>>;
    async fn purge(&self) -> Result<()>;
}

// --- Dataset: object-safe core ---

#[async_trait]
pub trait Dataset: Send + Sync {
    async fn push_json(&self, item: serde_json::Value) -> Result<()>;
    async fn push_json_batch(&self, items: Vec<serde_json::Value>) -> Result<()>;
    async fn list_raw(&self, opts: ListOptions) -> Result<Page<serde_json::Value>>;
    fn stream_raw(&self, opts: ListOptions) -> BoxStream<'_, Result<serde_json::Value>>;
    async fn export_json(&self, path: &Path) -> Result<()>;
    async fn export_csv(&self, path: &Path) -> Result<()>;
    async fn info(&self) -> Result<DatasetInfo>;
}

// --- Dataset: typed conveniences via blanket extension ---

#[async_trait]
pub trait DatasetExt: Dataset {
    async fn push<T: Serialize + Send + Sync>(&self, item: &T) -> Result<()> {
        self.push_json(serde_json::to_value(item)?).await
    }
    async fn push_batch<T: Serialize + Send + Sync>(&self, items: &[T]) -> Result<()> {
        let json = items.iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?;
        self.push_json_batch(json).await
    }
    async fn list<T: DeserializeOwned>(&self, opts: ListOptions) -> Result<Page<T>> { … }
    fn stream<T: DeserializeOwned + 'static>(&self, opts: ListOptions)
        -> BoxStream<'_, Result<T>> { … }
}
impl<D: Dataset + ?Sized> DatasetExt for D {}

// --- KeyValueStore: same pattern ---

#[async_trait]
pub trait KeyValueStore: Send + Sync {
    async fn get_bytes(&self, key: &str) -> Result<Option<KvEntry>>;
    async fn set_bytes(&self, key: &str, bytes: Bytes, content_type: &str) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn list_keys(&self, opts: ListKeysOptions) -> Result<KeyList>;
}

#[async_trait]
pub trait KeyValueStoreExt: KeyValueStore {
    async fn get<T: DeserializeOwned + 'static>(&self, key: &str) -> Result<Option<T>> { … }
    async fn set<T: Serialize + Send + Sync>(&self, key: &str, value: &T) -> Result<()> { … }
}
impl<K: KeyValueStore + ?Sized> KeyValueStoreExt for K {}

/// Auto-persisting cell: snapshotted to disk on every PersistState event.
/// Lives as its own typed wrapper, NOT a method on `dyn KeyValueStore` —
/// generic methods would break object-safety. Construct it with a borrowed
/// store handle that it captures internally.
pub struct AutoSaved<T> { /* … */ }

impl<T> AutoSaved<T>
where T: Serialize + DeserializeOwned + Send + Sync + 'static {
    pub async fn open(store: Arc<dyn KeyValueStore>, key: impl Into<String>, default: T)
        -> Result<Self>;
    pub async fn get(&self) -> T where T: Clone;
    pub async fn set(&self, value: T);
    pub async fn update<F: FnOnce(&mut T) + Send>(&self, f: F);
    pub async fn persist(&self) -> Result<()>;     // called on PersistState
}

// --- RequestQueue: object-safe + lease semantics ---

/// A lease represents temporary ownership of a request by one worker.
/// Returned by `fetch_next`; the worker must call `mark_handled` or `reclaim`
/// before the lease expires, otherwise the queue will hand the request to
/// another worker. (Distributed queues will eventually need this; designing
/// for it now avoids a breaking change.)
pub struct Lease {
    pub request: Request,
    pub lease_id: LeaseId,
    pub expires_at: Instant,
}

#[async_trait]
pub trait RequestQueue: Send + Sync {
    async fn add(&self, req: Request, opts: AddOptions) -> Result<QueueOpInfo>;

    /// Returns immediately with the requests that were directly added (i.e.,
    /// inline URLs). Requests sourced from `requests_from_url` are added
    /// asynchronously — completion is observable via the returned handle.
    async fn add_batch(&self, reqs: Vec<RequestSource>, opts: AddOptions) -> Result<BatchAddHandle>;

    async fn fetch_next(&self) -> Result<Option<Lease>>;

    /// Mark a leased request as successfully processed. Consumes the lease.
    async fn mark_handled(&self, lease: Lease) -> Result<()>;

    /// Return the request to the queue (e.g., after a retryable error).
    /// Increments retry count by default; controlled via `ReclaimOptions`.
    async fn reclaim(&self, lease: Lease, opts: ReclaimOptions) -> Result<()>;

    /// Extend the lease deadline without releasing the request. Long-running
    /// browser tasks should call this periodically.
    async fn renew(&self, lease_id: &LeaseId, extend_by: Duration) -> Result<()>;

    /// Acknowledge that a lease will not be completed (e.g., worker shutdown).
    /// Re-queues immediately rather than waiting for expiration.
    async fn abandon(&self, lease: Lease) -> Result<()>;

    async fn is_empty(&self) -> Result<bool>;
    async fn is_finished(&self) -> Result<bool>;
    async fn handled_count(&self) -> Result<u64>;
    async fn pending_count(&self) -> Result<u64>;
}

/// Returned by add_batch — completes when all requests are added, including
/// requests sourced from a `requests_from_url`.
pub struct BatchAddHandle {
    pub added: Vec<ProcessedRequest>,
    completion: JoinHandle<Result<AddRequestsBatchedResult>>,
}
impl BatchAddHandle {
    pub async fn wait(self) -> Result<AddRequestsBatchedResult>;
}
```

For the in-memory backend, lease expiry is a no-op (single process, no networking — leases never time out in practice). The contract still holds: `mark_handled` consumes the lease, `reclaim` increments retry count and re-queues, `abandon` re-queues without incrementing. FS, Redis, and Apify backends can enforce real expiry without an API break.

Queue ordering is also policy, not a storage accident. The v1 in-memory queue supports FIFO plus `forefront`; the trait leaves room for priority frontier ordering, domain round-robin fairness, and per-path budgets without changing `fetch_next()`. A distributed backend may implement the same policy with a sorted set or shard-aware frontier.

### 8.2 Provided implementations

- `millipede-storage-memory::MemoryStorageClient` — in-process, no I/O. Default. Used in tests.
- `millipede-storage-fs::FsStorageClient` — mirrors Crawlee's `MemoryStorage` on-disk layout (`./storage/datasets/<id>/`, `./storage/key_value_stores/<id>/`, `./storage/request_queues/<id>/`). Wire-compatible enough that a Crawlee project's `./storage` directory can be inspected by a millipede crawler.
- (Post-MVP) `millipede-storage-apify` — talks to the Apify platform API.

### 8.3 `StorageHandle` — what users get on `ctx`

```rust
pub struct StorageHandle { /* Arc to the client + cached default ids */ }
impl StorageHandle {
    pub fn dataset(&self) -> &Arc<dyn Dataset>;          // default dataset
    pub async fn dataset_named(&self, name: &str) -> Result<Arc<dyn Dataset>>;
    pub fn key_value_store(&self) -> &Arc<dyn KeyValueStore>;
    pub async fn kvs_named(&self, name: &str) -> Result<Arc<dyn KeyValueStore>>;
    pub fn request_queue(&self) -> &Arc<dyn RequestQueue>;
}
```

`ctx.storage.dataset().push(&item).await?` is the everyday call.

---

## 9. HTTP Client Abstraction

```rust
// crates/millipede-core/src/http_client.rs

#[async_trait]
pub trait HttpClient: Send + Sync + 'static {
    async fn send(&self, req: HttpRequest) -> Result<HttpResponse, HttpClientError>;
    async fn stream(&self, req: HttpRequest) -> Result<StreamingResponse, HttpClientError>;
}

pub struct HttpRequest {
    pub url: Url,
    pub method: Method,
    pub headers: HeaderMap,
    pub body: Option<RequestBody>,
    pub cookie_jar: Option<Arc<CookieJar>>,
    pub proxy: Option<Url>,
    pub timeout: Option<Duration>,
    pub max_redirects: u32,
    pub use_header_generator: bool,
    pub session_token: Option<SessionToken>,  // for fingerprint consistency per session
}

pub struct HttpResponse {
    pub url: Url,                              // final URL after redirects
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub redirect_chain: Vec<Url>,
}
```

The default impl, `ReqwestClient`, wraps `reqwest` and integrates with `millipede-fingerprint` for header generation. True TLS fingerprint impersonation is not promised by a custom `rustls` verifier. The extension point is the `HttpClient` trait: a future `WreqClient`/`ImpitClient`-style backend can provide JA3/JA4-level behavior without breaking crawler APIs. Until that exists, `millipede-fingerprint` means header and browser-context consistency, not full TLS impersonation.

The `HttpClient` trait is stable across crawler kinds: `HttpCrawler` uses it as its primary fetcher; `BrowserCrawler` uses it for `ctx.send_request` (out-of-band requests during browser flows).

In-flight request coalescing sits above `HttpClient`: if two active tasks attempt the same `Request::unique_key`, the second task may await the first task's fetch result rather than duplicate the network request. This is distinct from queue deduplication, which prevents future duplicate work but cannot see races already in flight.

---

## 10. Sessions & Session Pool

Codex review #9 flagged two bugs in the earlier draft: (1) `AtomicU32 error_score` is incompatible with `f32 error_score_decrement` (Crawlee's default decrement is 0.5); (2) returning `RwLockReadGuard` is a footgun because users can hold it across `.await`. Fixed below: error score lives in a `Mutex`-guarded state struct with fixed-point scaling, and user-data access is closure-based (no lock guard leaks).

```rust
pub struct Session {
    pub id: SessionId,
    cookies: Arc<CookieJar>,
    state: Mutex<SessionState>,            // tokio::sync::Mutex — never held across await
    expires_at: Instant,
    config: SessionConfig,
}

struct SessionState {
    user_data: UserData,
    /// Error score scaled by 1000. Decrements use the same scale so `0.5`
    /// decrement = subtract 500. Avoids the f32-on-atomic mismatch.
    error_score_scaled: u32,
    usage_count: u32,
    retired: bool,
}

pub struct SessionConfig {
    pub max_error_score_scaled: u32,       // default 3000 (= 3.0)
    pub max_usage_count: u32,               // default 50
    pub error_score_decrement_scaled: u32,  // default 500 (= 0.5)
}

impl Session {
    pub fn id(&self) -> &SessionId;
    pub fn cookie_jar(&self) -> &Arc<CookieJar>;

    /// Closure-based access — the lock is released as soon as `f` returns.
    /// `f` is sync because `Mutex` is held; for async work, clone the data first.
    pub async fn with_user_data<R>(&self, f: impl FnOnce(&UserData) -> R) -> R;
    pub async fn update_user_data(&self, f: impl FnOnce(&mut UserData));

    pub async fn error_score(&self) -> f32;     // returns scaled value / 1000.0
    pub async fn usage_count(&self) -> u32;
    pub async fn is_blocked(&self) -> bool;
    pub fn is_expired(&self) -> bool;            // expiry is fixed, no lock needed
    pub async fn is_usable(&self) -> bool;
    pub async fn mark_good(&self);               // decrement_scaled
    pub async fn mark_bad(&self);                // increment by 1000 (=1.0)
    pub async fn retire(&self);
    pub async fn set_cookies_from_response(&self, resp: &HttpResponse) -> Result<()>;
}

pub struct SessionPool {
    sessions: RwLock<Vec<Arc<Session>>>,
    max_pool_size: usize,
    persist_state_key: Option<String>,
    create_options: SessionOptions,
}

impl SessionPool {
    pub async fn new(opts: SessionPoolOptions, storage: StorageHandle) -> Result<Self>;
    pub async fn get_session(&self, sticky_id: Option<&SessionId>) -> Result<Arc<Session>>;
    pub async fn retire_session(&self, s: &Session);
    pub async fn persist(&self) -> Result<()>;     // wire to PersistState event
    pub async fn restore(&self) -> Result<()>;
}
```

The cookie jar is exposed as a single `CookieJar` newtype owned by the `Session` and threaded into both `reqwest` (via `reqwest::cookie::CookieStore`) and `chromiumoxide` (via the provider's `set_cookies` / `get_cookies` adapter). Users never touch the underlying store. This unified adapter is a hard requirement, not an optimization (ChatGPT Pro review #1): mixing two cookie representations across the HTTP and browser crawler paths is the single biggest source of state-divergence bugs in projects like Crawlee. The concrete inner type (`reqwest_cookie_store::CookieStoreMutex` vs. a custom `Arc<RwLock<…>>`) is the only variable, and it is resolved at Phase 3 close — see Open Question 1 in §22.

---

## 11. Proxy Configuration

```rust
pub struct ProxyConfiguration { inner: ProxyInner }

enum ProxyInner {
    Static { urls: Vec<Url>, rotation: RotationStrategy },
    Custom(Arc<dyn ProxyResolver>),
    Tiered { tiers: Vec<Vec<Option<Url>>>, state: Arc<TieredProxyState> },
}

#[async_trait]
pub trait ProxyResolver: Send + Sync + 'static {
    async fn resolve(&self, ctx: ProxyResolveContext<'_>) -> Result<Option<Url>>;
}

pub struct ProxyResolveContext<'a> {
    pub request: Option<&'a Request>,
    pub session_id: Option<&'a SessionId>,
    pub attempt: u32,
}

pub struct ProxyInfo {
    pub session_id: Option<SessionId>,
    pub url: Url,
    pub hostname: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tier: Option<u8>,
}

impl ProxyConfiguration {
    pub fn round_robin(urls: impl IntoIterator<Item = Url>) -> Self;
    pub fn tiered(tiers: Vec<Vec<Option<Url>>>) -> Self;
    pub fn custom<R: ProxyResolver>(r: R) -> Self;

    pub async fn new_url(&self, ctx: ProxyResolveContext<'_>) -> Result<Option<Url>>;
    pub async fn new_proxy_info(&self, ctx: ProxyResolveContext<'_>) -> Result<Option<ProxyInfo>>;
}
```

Tiered proxy state tracks per-domain blocking and probes lower tiers periodically — replicating Crawlee's `tieredProxyUrls` semantics. The `null` slot in a tier means "direct, no proxy" (this matches Crawlee).

Advanced routing uses a cheap, object-safe hook:

```rust
pub trait ProxyStrategy: Send + Sync + 'static {
    fn route(&self, ctx: &ProxyRouteContext<'_>) -> ProxyKind;
}

pub struct ProxyRouteContext<'a> {
    pub request: &'a Request,
    pub attempt: u32,
    pub previous_profile_key: Option<&'a str>,
}

pub enum ProxyKind { Default, MediaAsset, Custom(String) }
```

`ProxyStrategy` decides *which* proxy bucket to use for one request; `RetryStrategy` decides *when* to retry and may replace the available proxy configuration between attempts. Both receive borrowed context so hot-path dispatch is allocation-light.

---

## 12. Browser Pool & Provider Abstraction

```rust
// crates/millipede-browser/src/lib.rs

#[async_trait]
pub trait BrowserProvider: Send + Sync + 'static {
    type Browser: Send + Sync + 'static;
    type Page: Send + Sync + 'static;
    type LaunchOptions: Default + Clone + Send + Sync + 'static;

    async fn launch(&self, opts: Self::LaunchOptions, ctx: LaunchContext)
        -> Result<Self::Browser, BrowserError>;
    async fn new_page(&self, browser: &Self::Browser) -> Result<Self::Page, BrowserError>;
    async fn close_browser(&self, browser: Self::Browser) -> Result<(), BrowserError>;
    async fn close_page(&self, page: Self::Page) -> Result<(), BrowserError>;
    async fn get_cookies(&self, page: &Self::Page) -> Result<Vec<Cookie>, BrowserError>;
    async fn set_cookies(&self, page: &Self::Page, cookies: &[Cookie])
        -> Result<(), BrowserError>;
    async fn goto(&self, page: &Self::Page, url: &Url, opts: GotoOptions)
        -> Result<Option<BrowserResponse>, BrowserError>;
}

pub struct BrowserPool<P: BrowserProvider> {
    provider: Arc<P>,
    options: BrowserPoolOptions,
    state: RwLock<PoolState<P>>,
    hooks: BrowserHooks<P>,
}

impl<P: BrowserProvider> BrowserPool<P> {
    pub async fn new(provider: P, opts: BrowserPoolOptions) -> Result<Self>;
    pub async fn new_page(&self) -> Result<PageHandle<P>>;
    pub async fn shutdown(&self) -> Result<()>;
}

/// RAII handle — closes the page back to the pool on drop.
pub struct PageHandle<P: BrowserProvider> { … }

pub struct BrowserHooks<P: BrowserProvider> {
    pub pre_launch: Vec<Hook<LaunchContext, P>>,
    pub post_launch: Vec<Hook<(Arc<P::Browser>, LaunchContext), P>>,
    pub pre_page_create: Vec<Hook<(&Arc<P::Browser>, PageOpts<P>), P>>,
    pub post_page_create: Vec<Hook<&P::Page, P>>,
    pub pre_page_close: Vec<Hook<&P::Page, P>>,
    pub post_page_close: Vec<Hook<(), P>>,
}
```

Concrete providers live in their own crates so that pulling in `millipede-browser` does not force a Chromium dependency:

- `millipede-browser-chromiumoxide::ChromiumoxideProvider` — uses [`chromiumoxide`](https://crates.io/crates/chromiumoxide) (Rust CDP client).
- `millipede-browser-playwright::PlaywrightProvider` — optional, uses the `playwright` Rust binding.

Fingerprint injection runs as a `post_page_create` hook supplied by `millipede-fingerprint`.

### 12.2 `PageHandle` — provider-erased page wrapper

`BrowserContext` exposes a `PageHandle`, not `P::Page`. This keeps provider generics out of every handler and router signature, lets us write reusable browser utilities, and gives us a place to enforce explicit cleanup. (Codex review #5: don't push provider generics into user code; Drop can't `.await`.)

```rust
pub struct PageHandle {
    inner: Arc<dyn BrowserPage>,
    pool: Weak<dyn PoolControl>,
    page_id: PageId,
    _guard: PageReturnGuard,                 // see below
}

#[async_trait]
pub trait BrowserPage: Send + Sync {
    async fn goto(&self, url: &Url, opts: GotoOptions) -> Result<Option<BrowserResponse>>;
    async fn evaluate_js(&self, script: &str) -> Result<serde_json::Value>;
    async fn content(&self) -> Result<String>;
    async fn screenshot(&self, opts: ScreenshotOptions) -> Result<Bytes>;
    async fn cookies(&self) -> Result<Vec<Cookie>>;
    async fn set_cookies(&self, cookies: &[Cookie]) -> Result<()>;
    async fn set_extra_headers(&self, headers: &HeaderMap) -> Result<()>;
    async fn wait_for_selector(&self, selector: &str, timeout: Duration) -> Result<()>;
    async fn click(&self, selector: &str) -> Result<()>;
    async fn evaluate_anchors(&self, selector: Option<&str>) -> Result<Vec<Url>>;
}

impl PageHandle {
    /// Explicit, async cleanup. Always call this when done.
    /// If dropped without close(), a background task closes the page and
    /// emits a warning via `tracing`.
    pub async fn close(self) -> Result<()>;

    pub fn id(&self) -> PageId;
}

/// Drop fallback: sends a close command to a worker task. Logs a warning
/// because async drop is impossible and `close().await` is preferred.
struct PageReturnGuard { /* … */ }
impl Drop for PageReturnGuard { /* enqueue close on pool's worker task */ }
```

Each `BrowserProvider` impl is responsible for adapting its native page type to `BrowserPage`. `chromiumoxide::Page` `Send + Sync` behavior across `await` points is validated in the Phase 6 spike before the adapter API is locked.

### 12.3 Smart HTTP-first promotion

Browser crawling is expensive enough that Millipede should not pay the browser tax unless a page needs it. A smart crawler mode first attempts the request through the HTTP/HTML path, runs a cheap detector over status, headers, and body patterns, and promotes to `BrowserCrawler` only when JavaScript rendering or anti-bot challenge handling is likely required.

The detector is intentionally pluggable:

```rust
pub trait BrowserPromotionDetector: Send + Sync + 'static {
    fn should_promote(&self, attempt: &HttpAttemptSnapshot<'_>) -> Option<PromotionReason>;
}

pub enum PromotionReason {
    EmptyBodyLikelyJs,
    KnownAntiBot(AntiBotTech),
    SelectorMissing { selector: String },
    Custom(String),
}
```

The default implementation uses a static pattern set plus response metadata. It must be conservative: false negatives mean a page stays in HTTP mode and can be retried/promoted by user policy; false positives waste browser capacity.

---

## 13. Autoscaling

```rust
// crates/millipede-core/src/autoscale.rs

pub struct AutoscaledPool { /* … */ }

pub struct AutoscaledPoolOptions {
    /// `Some(n)` disables autoscaling entirely and pins concurrency at `n`.
    /// Equivalent to `min == max == n` but makes intent explicit in builder code
    /// and skips the snapshotter/system-status overhead. Useful for users who
    /// want predictable throughput or who run inside a container with a fixed
    /// resource budget.
    pub fixed_concurrency: Option<usize>,
    pub min_concurrency: usize,           // default 1
    pub max_concurrency: usize,           // default 200
    pub desired_concurrency: Option<usize>,
    pub scale_up_step_ratio: f32,         // 0.05
    pub scale_down_step_ratio: f32,       // 0.05
    pub desired_utilization_ratio: f32,   // 0.9
    pub task_timeout: Option<Duration>,
    pub max_tasks_per_minute: Option<u32>,
    pub maybe_run_interval: Duration,     // 500ms
    pub autoscale_interval: Duration,     // 10s
    pub mode: AutoscaleMode,
    pub snapshotter: SnapshotterOptions,
    pub system_status: SystemStatusOptions,
}

pub enum AutoscaleMode {
    /// Deterministic baseline: additive increase after sustained success,
    /// multiplicative decrease on retry/failure signals.
    Aimd {
        increase_after_successes: usize,
        decrease_factor: f32,
    },
    /// Crawlee-style load-signal pool using CPU/memory/runtime/client signals.
    LoadSignals,
}

#[async_trait]
pub trait TaskSource: Send + Sync {
    async fn run_task(&self) -> Result<(), CrawlError>;
    async fn is_task_ready(&self) -> bool;
    async fn is_finished(&self) -> bool;
}

#[async_trait]
pub trait LoadSignal: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn overload_threshold(&self) -> f32;
    async fn start(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;
    fn sample(&self, window: Duration) -> Vec<LoadSnapshot>;
}

pub struct LoadSnapshot { pub at: Instant, pub overloaded: bool }
```

Built-in load signals:

- `CpuLoadSignal` — `sysinfo::System::global_cpu_info().cpu_usage()` sampled periodically.
- `MemoryLoadSignal` — `sysinfo` process + system memory.
- `TokioRuntimeLoadSignal` — tokio metrics (`runtime::Handle::current().metrics().worker_local_queue_depth()` etc.), the Rust analog of Crawlee's event-loop-lag signal.
- `ClientLoadSignal` — fed by `StorageClient` rate-limit errors via an event channel.

Scaling decisions: every `autoscale_interval`, compute weighted overload across signals over a sliding window; if all signals are under threshold for `desired_utilization_ratio` of the window, scale up by `scale_up_step_ratio * desired`; if any signal exceeds threshold, scale down.

Implementation note for `AutoscaledPool`: tokio's `Semaphore` doesn't support shrinking, so we don't use it as the throttle. Instead a `dispatch_loop` maintains `current_tasks: AtomicUsize` and only spawns when `current_tasks < desired_concurrency`. Active tasks decrement on completion.

Domain politeness is handled by a cooperative token bucket keyed by host. `same_domain_delay` is the simple builder-facing knob; internally it maps to a per-domain limiter that can also be throttled by `Retry-After`, robots `Crawl-delay`, or repeated 429s.

---

## 14. Configuration

```rust
pub struct Configuration { /* … */ }

#[derive(Default, Clone)]
pub struct ConfigurationBuilder {
    pub storage_client: Option<Arc<dyn StorageClient>>,
    pub default_dataset_id: Option<String>,
    pub default_key_value_store_id: Option<String>,
    pub default_request_queue_id: Option<String>,
    pub max_used_cpu_ratio: Option<f32>,
    pub available_memory_ratio: Option<f32>,
    pub memory_bytes: Option<u64>,
    pub persist_state_interval: Option<Duration>,
    pub purge_on_start: Option<bool>,
    pub log_level: Option<LogLevel>,
}

impl Configuration {
    pub fn builder() -> ConfigurationBuilder;
    pub fn storage_client(&self) -> &Arc<dyn StorageClient>;
    pub fn events(&self) -> &EventBus;
    /* getters for every field */
}
```

`Configuration` is **passed explicitly** to every `Crawler::builder()` (with a default if omitted). There is no `Configuration::global()`. Environment-variable overrides are read into the builder at `build()` time, not via process-wide state.

Defaults read from env vars (matching Crawlee for ease of migration):
`CRAWLEE_PURGE_ON_START`, `CRAWLEE_STORAGE_DIR`, `CRAWLEE_LOG_LEVEL`, `CRAWLEE_AVAILABLE_MEMORY_RATIO`, `CRAWLEE_MEMORY_MBYTES`, `CRAWLEE_DEFAULT_DATASET_ID`, etc.

---

## 15. Events

```rust
#[derive(Debug, Clone)]
pub enum CrawlerEvent {
    PersistState { is_migrating: bool },
    RequestFinished(HandledRequest),
    RequestFailed { request: Arc<Request>, error: String },
    SystemInfo(SystemSnapshot),
    Aborting,
    Exiting,
}

pub struct EventBus { tx: tokio::sync::broadcast::Sender<CrawlerEvent> }
impl EventBus {
    pub fn subscribe(&self) -> EventStream;
    pub fn emit(&self, e: CrawlerEvent);
}

pub type EventStream = tokio::sync::broadcast::Receiver<CrawlerEvent>;
```

Internally the crawler fires `PersistState` every `persist_state_interval` and on SIGINT/SIGTERM (via tokio `signal`). `RequestFinished` is also mirrored to `ResultStream`; `EventStream` is for control-plane observers, while `ResultStream` is the stable data-plane feed. User code subscribes:

```rust
let mut events = crawler.events();
while let Ok(ev) = events.recv().await {
    if matches!(ev, CrawlerEvent::Aborting) { /* save extra state */ }
}
```

---

## 16. Errors & Retries

```rust
#[derive(thiserror::Error, Debug)]
pub enum CrawlError {
    /// Retried; counts against `max_request_retries`.
    #[error("retryable: {0}")]
    Retry(#[source] anyhow::Error),

    /// Retried; rotates the session and counts against `max_session_rotations`,
    /// *not* against `max_request_retries`. Use when you suspect IP/cookie block.
    #[error("session: {0}")]
    Session(#[source] anyhow::Error),

    /// Always retried; ignores `max_request_retries`. (Crawlee's RetryRequestError.)
    #[error("force-retry: {0}")]
    ForceRetry(#[source] anyhow::Error),

    /// Never retried; calls `failed_request_handler`.
    #[error("non-retryable: {0}")]
    NonRetryable(#[source] anyhow::Error),

    /// Aborts the entire crawler.
    #[error("critical: {0}")]
    Critical(#[source] anyhow::Error),

    /// Specifically: no route matches the request's label.
    #[error("missing route for label {0:?}")]
    MissingRoute(Option<String>),

    /// A known anti-bot or WAF page was detected.
    #[error("anti-bot detected: {tech:?}")]
    AntiBotDetected { tech: AntiBotTech, source: anyhow::Error },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AntiBotTech {
    Cloudflare,
    DataDome,
    PerimeterX,
    Kasada,
    Imperva,
    Akamai,
    Custom(String),
    Unknown,
}

impl CrawlError {
    pub fn retry<E: Into<anyhow::Error>>(e: E) -> Self;
    pub fn session<E: Into<anyhow::Error>>(e: E) -> Self;
    pub fn force_retry<E: Into<anyhow::Error>>(e: E) -> Self;
    pub fn fatal<E: Into<anyhow::Error>>(e: E) -> Self;
}

impl From<reqwest::Error> for CrawlError { /* heuristic: timeout/connect → Retry */ }
impl From<std::io::Error> for CrawlError { /* default → Retry */ }
```

Handlers return `Result<(), CrawlError>` and use `?` freely. Common errors (`reqwest::Error`, parsing errors) get a sensible default classification but can be remapped with `.map_err(CrawlError::session)` when context tells the handler the error is session-related.

Advanced retries are an opt-in strategy hook layered on top of this taxonomy:

```rust
pub struct AttemptOutcome<'a> {
    pub request: &'a Request,
    pub attempt: u32,
    pub status: Option<StatusCode>,
    pub error: Option<&'a CrawlError>,
    pub anti_bot: Option<AntiBotTech>,
    pub proxy_info: Option<&'a ProxyInfo>,
    pub session_id: Option<&'a SessionId>,
    pub response_bytes: Option<usize>,
}

pub struct RetryDirective {
    pub should_retry: bool,
    pub backoff: Option<Duration>,
    pub proxy_kind: Option<ProxyKind>,
    pub user_agent_profile: Option<String>,
    pub session_action: SessionRetryAction,
}

pub enum SessionRetryAction { Keep, Rotate, Retire }

pub trait RetryStrategy: Send + Sync + 'static {
    fn max_retries(&self) -> u32;
    fn on_retry(&self, outcome: &AttemptOutcome<'_>) -> RetryDirective;
}
```

The strategy receives borrowed context and returns an owned directive. It never mutates crawler internals directly.

---

## 17. Statistics

```rust
pub struct StatisticsHandle { inner: Arc<StatisticsInner> }

impl StatisticsHandle {
    pub fn snapshot(&self) -> StatisticsSnapshot;
    pub fn subscribe(&self) -> mpsc::Receiver<StatisticsSnapshot>;  // periodic updates
}

#[derive(Debug, Clone, Serialize)]
pub struct StatisticsSnapshot {
    pub requests_finished: u64,
    pub requests_failed: u64,
    pub requests_retries: u64,
    pub requests_finished_per_minute: f64,
    pub requests_failed_per_minute: f64,
    pub request_avg_duration: Duration,
    pub request_min_duration: Duration,
    pub request_max_duration: Duration,
    pub status_codes: BTreeMap<u16, u64>,
    pub crawler_runtime: Duration,
    pub retry_histogram: Vec<u64>,
    pub errors: BTreeMap<String, u64>,
    pub retry_errors: BTreeMap<String, u64>,
}

pub struct FinalStatistics { /* same fields */ }
```

Stats are persisted to the default `KeyValueStore` under key `SDK_CRAWLER_STATISTICS_0` (matching Crawlee key) on every `PersistState` event, enabling resume.

---

## 18. Hooks

Pre- and post-navigation hooks let users intercept before/after the fetch. Hooks are generic over the kind's hook context, so HTTP hooks see HTTP state and browser hooks see the page — no downcasts, no extension-trait grab bags. (Gemini review #1.1 flagged the original `NavCtx` "extras via downcast" design.)

```rust
// Engine-supplied state every hook can see.
pub trait HookCtx: Send {
    fn request(&mut self) -> &mut Request;
    fn session(&self) -> Option<&Session>;
    fn proxy(&self) -> Option<&ProxyInfo>;
    fn log(&self) -> &Log;
}

#[async_trait]
pub trait PreNavigationHook<K: CrawlerKind>: Send + Sync + 'static {
    type Ctx<'a>: HookCtx + 'a where K: 'a;
    async fn run<'a>(&'a self, ctx: Self::Ctx<'a>) -> Result<(), CrawlError>;
}

#[async_trait]
pub trait PostNavigationHook<K: CrawlerKind>: Send + Sync + 'static {
    type Ctx<'a>: HookCtx + 'a where K: 'a;
    async fn run<'a>(&'a self, ctx: Self::Ctx<'a>) -> Result<(), CrawlError>;
}

// Each kind picks its own concrete hook contexts.

pub struct HttpPreHookCtx<'a> {
    pub request: &'a mut Request,
    pub session: Option<&'a Session>,
    pub proxy: Option<&'a ProxyInfo>,
    pub http_request: &'a mut HttpRequest,    // can add headers, change timeout, etc.
    pub log: &'a Log,
}

pub struct HttpPostHookCtx<'a> {
    pub request: &'a Request,
    pub response: &'a HttpResponse,           // post-fetch: inspect status/headers
    pub session: Option<&'a Session>,
    pub proxy: Option<&'a ProxyInfo>,
    pub log: &'a Log,
}

pub struct BrowserPreHookCtx<'a> {
    pub request: &'a mut Request,
    pub page: &'a PageHandle,                 // hooks can call page.set_extra_headers etc.
    pub session: Option<&'a Session>,
    pub proxy: Option<&'a ProxyInfo>,
    pub log: &'a Log,
}

pub struct BrowserPostHookCtx<'a> {
    pub request: &'a Request,
    pub page: &'a PageHandle,
    pub response: Option<&'a BrowserResponse>,
    pub session: Option<&'a Session>,
    pub proxy: Option<&'a ProxyInfo>,
    pub log: &'a Log,
}

// Blanket impl so closures Just Work as hooks.
impl<K, F, Fut> PreNavigationHook<K> for F
where
    K: CrawlerKind,
    F: for<'a> Fn(<K as HasPreHook>::Ctx<'a>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), CrawlError>> + Send,
{ … }
```

Builder API:

```rust
HttpCrawler::builder()
    .pre_navigation_hook(|ctx: HttpPreHookCtx<'_>| async move {
        ctx.http_request.headers.insert("X-Crawl", "millipede".parse().unwrap());
        Ok(())
    })
    .post_navigation_hook(|ctx: HttpPostHookCtx<'_>| async move {
        if ctx.response.status == 503 {
            return Err(CrawlError::Session(anyhow!("503 — rotate session")));
        }
        Ok(())
    })
    .build()?;
```

Hooks run in registration order. Any `CrawlError` short-circuits the request (treated like a handler error of the same severity). The hook context borrows engine state for one fetch; nothing crosses `.await` boundaries that the engine cannot guarantee `Send`.

---

## 19. Logging & Observability

### 19.1 Logging

A thin `Log` newtype wraps `tracing`. Every context has `ctx.log`:

```rust
pub struct Log { span: tracing::Span }
impl Log {
    pub fn info(&self, msg: impl AsRef<str>);
    pub fn debug(&self, msg: impl AsRef<str>);
    pub fn warn(&self, msg: impl AsRef<str>);
    pub fn error(&self, msg: impl AsRef<str>);
    /// Returns a child log with extra structured fields.
    pub fn with_fields(&self, fields: impl IntoIterator<Item = (&str, &dyn Value)>) -> Self;
}
```

Internally everything emits `tracing` events; users hook into `tracing-subscriber` for output formatting. We do not invent our own logger.

### 19.2 Spans

The engine opens a span hierarchy that mirrors the crawl lifecycle:

- `crawler.run` — opened on `Crawler::run()`, closed on completion. Fields: `crawler_id`, `kind`.
- `request` — opened per request, child of `crawler.run`. Fields: `request_id`, `url`, `method`, `retry_count`, `label`, `crawl_depth`.
- `fetch` — opened around the actual fetch inside `execute`. Fields: `proxy_tier`, `session_id`, `status_code` (on close).
- `handler` — opened around the user's `RequestHandler::handle` call. Fields: `route_label` (the matched route).

Span names are stable: external tracing collectors (Jaeger, Datadog, Tempo) can rely on them.

### 19.3 Metrics

In addition to `tracing` events, the engine maintains counters and gauges via the [`metrics`](https://crates.io/crates/metrics) crate (an optional feature `metrics`). Users register any compatible exporter (Prometheus, StatsD, OTLP) and get the following series for free:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `millipede.requests.finished` | counter | `crawler`, `label` | Successful request handler completions. |
| `millipede.requests.failed` | counter | `crawler`, `label`, `error_kind` | Failed at handler or fetch. |
| `millipede.requests.retried` | counter | `crawler`, `retry_kind` | Retries by classification (`retry`/`session`/`force_retry`). |
| `millipede.requests.duration` | histogram | `crawler`, `label` | Per-request total time. |
| `millipede.fetch.duration` | histogram | `crawler` | Fetch time alone. |
| `millipede.queue.depth.pending` | gauge | `crawler` | Pending requests in queue. |
| `millipede.queue.depth.in_flight` | gauge | `crawler` | Leased + not yet completed. |
| `millipede.concurrency.current` | gauge | `crawler` | Tasks currently running. |
| `millipede.concurrency.desired` | gauge | `crawler` | Autoscaler target. |
| `millipede.session_pool.size` | gauge | `crawler` | Active sessions. |
| `millipede.session_pool.retired` | counter | `crawler` | Sessions retired this run. |
| `millipede.status_code` | counter | `crawler`, `code` | HTTP status codes seen. |

The `metrics` feature is off by default; turning it on adds ~50 KB and a `Send + Sync` recorder lookup per metric call. Users not on `metrics` still get all the same data via `tracing` events.

### 19.4 OpenTelemetry

`tracing-opentelemetry` works out of the box because we only emit standard `tracing` spans/events. No bespoke OTel integration crate is needed for v1.

---

## 20. End-to-End Example

```rust
use millipede::{HtmlCrawler, HtmlContext, Router, CrawlPolicy, EnqueueStrategy, Method};
use millipede::storage::FsStorageClient;
use millipede::{DatasetExt, Configuration};   // bring `dataset.push(&item)` into scope

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let storage = Arc::new(FsStorageClient::new("./storage")?);

    let router = Router::<HtmlContext>::new()
        .route("listing", |ctx: HtmlContext| async move {
            ctx.log.info(format!("listing: {}", ctx.request.url));
            ctx.enqueue.options()
                .selector("article a.detail")
                .label("detail")
                .send().await?;
            Ok(())
        })
        .route_method("detail", Method::GET, |ctx: HtmlContext| async move {
            let sel = scraper::Selector::parse("h1").unwrap();
            let title = ctx.html.select(&sel).next()
                .map(|el| el.text().collect::<String>())
                .unwrap_or_default();
            ctx.storage.dataset().push(&serde_json::json!({
                "url": ctx.request.url,
                "title": title,
            })).await?;
            Ok(())
        });

    let crawler = HtmlCrawler::builder()
        .configuration(Configuration::builder()
            .storage_client(storage)
            .purge_on_start(true)
            .build())
        .crawl_policy(CrawlPolicy::new()
            .strategy(EnqueueStrategy::SameDomain)
            .max_crawl_depth(5)
            .max_requests_per_crawl(10_000)
            .respect_robots("MillipedeBot/0.1"))
        .max_concurrency(20)
        .max_request_retries(3)
        .request_handler(router)
        .build()?;

    let stats = crawler.run(["https://example.com/products?label=listing"]).await?;
    println!("Finished: {} ok, {} failed", stats.requests_finished, stats.requests_failed);
    Ok(())
}
```

---

## 20.5 Timeouts, Limits, and `retry_on_blocked`

Following Codex's review, the following options are first-class on every crawler builder, not buried in nested configs:

```rust
HttpCrawler::builder()
    .request_handler_timeout(Duration::from_secs(60))   // user handler budget
    .navigation_timeout(Duration::from_secs(30))        // single fetch/navigate budget
    .internal_operation_timeout(Duration::from_secs(10)) // queue/storage ops
    .max_request_retries(3)
    .max_session_rotations(10)
    .same_domain_delay(Duration::from_millis(500))      // polite per-domain throttle
    .retry_on_blocked(true)                             // 401/403 → SessionError
    .build()?;
```

These map directly onto Crawlee's options and are wired into the engine, not surfaced via `Configuration`. They affect engine architecture (cancellation tokens around handler calls, per-domain rate buckets), so they need to be designed in early — they cannot be tacked on later.

## 21. What We Are Explicitly Not Porting

- **`AsyncLocalStorage` access checks.** Storage access is gated by *which `StorageHandle` was passed in*, not by ambient runtime state. There's no equivalent "you accessed a dataset outside a crawler" error — passing the handle around is the contract.
- **JS-style `RouterHandler` callable object.** We use a normal `Router` struct that implements `RequestHandler`. Builders set the handler on the crawler; the duality is gone.
- **`useState` magic.** Crawlee's `useState()` returns an auto-persisted reactive value bound to context identity. We expose `kvs.auto_saved("key", default)` explicitly. Users opt in.
- **`got-scraping` directly.** Header generation is reimplemented in `millipede-fingerprint`; we don't shell out to a Node library. True TLS fingerprinting is handled only by swapping the `HttpClient` backend (for example a future `wreq`/`impit`-style client), not by pretending a custom verifier is enough.
- **`PseudoUrl` legacy patterns.** Crawlee's `[regex]` bracketed pseudo-URLs are deprecated even there; we accept only globs and regexes.
- **A 1:1 port of `BrowserCrawler`'s 80+ context utility methods** (`infinite_scroll`, `save_snapshot`, `enqueue_links_by_click_elements`, …). We ship the obvious ones (`enqueue_links` works with CSS selectors on `BrowserContext`); the rest are user-space helper crates.

---

## 22. Open Questions & Resolved Decisions

**Resolved (via Codex/Gemini review):**

- ✅ **Storage trait object-safety.** Resolved in §8.1: object-safe core + blanket `*Ext` extension traits + standalone `AutoSaved<T>` wrapper.
- ✅ **`CrawlerKind` boundary.** Resolved in §4.1: full lifecycle trait (`start`/`before_request`/`execute`/`after_success`/`cleanup`/`stop`), not just context construction.
- ✅ **Naming clarity.** Resolved in §4.2: `XxxKind` for the trait impl, `XxxCrawler = Crawler<XxxKind>` for the public type.
- ✅ **Request queue future-proofing.** Resolved in §8.1: lease-based `fetch_next() -> Option<Lease>` with `mark_handled` / `reclaim` / `renew` / `abandon`.
- ✅ **Hook context downcasts.** Resolved in §18: per-kind `HookCtx` associated types; no `Any`, no extras-via-extension.
- ✅ **Router method dispatch.** Resolved in §6: `route_method` / `route_methods` for HTTP-method-specific handlers.
- ✅ **Crawl policy (depth, robots, max requests).** Resolved in §7.1: `CrawlPolicy` set on the builder, enforced inside `EnqueueLinker::send()`.
- ✅ **Session atomics + lock-guard footguns.** Resolved in §10: scaled-integer error score under a `Mutex`, closure-based user-data access.
- ✅ **Browser provider generics in user code.** Resolved in §12.2: `PageHandle` is provider-erased; provider generics stay inside `BrowserPool<P>`.
- ✅ **Async-trait policy.** Object-safe traits (`StorageClient`, `Dataset`, `KeyValueStore`, `RequestQueue`, `HttpClient`, `ProxyResolver`, `BrowserPage`, `SkippedHandler`) use `#[async_trait]`. Internal generic traits and `RequestHandler<C>` use native async fn or boxed-future returns. Decision documented per-crate in an ADR.
- ✅ **Result streaming shape.** Resolved in §4.3 and §15: `run()` returns final stats, `results()` exposes completed-request snapshots, and `events()` remains the control-plane feed. We do not make `run()` return a stream because the owned handler context is consumed and browser resources may already be cleaned up.

**Still open:**

1. **Cookie jar concretion (ChatGPT Pro #1).** The *abstraction* is locked in §10: one `CookieJar` newtype, threaded into `reqwest` and `chromiumoxide` via an internal adapter. The *inner* concrete is the open question: `reqwest_cookie_store::CookieStoreMutex` (uses a sync `parking_lot` mutex — never held across `.await`, but adds a non-tokio dependency to the lock surface) or a custom `Arc<tokio::sync::RwLock<cookie_store::CookieStore>>` (tokio-native, but we re-implement the `reqwest::cookie::CookieStore` impl by hand). Resolution target: **end of Phase 3**, recorded as an ADR. Decision criteria, in order: (a) round-trips cookies correctly through a `wiremock` redirect chain; (b) survives the chromiumoxide `set_cookies` → handler → `get_cookies` cycle without lost state; (c) does not require holding a lock across any `.await`.
2. **`scraper` ergonomics and engine extraction path (ChatGPT Pro #2 + Spider review).** `HtmlContext::html: Arc<scraper::Html>` is fixed for handler ergonomics and spawn-friendliness, but `Selector::parse` is fallible and cheap-to-repeat, and engine-owned link discovery may need streaming extraction. Four candidate layers/paths are evaluated in **Phase 5** under Criterion before any of them lands:
   - A `selectors!("a.detail", "h1 > span", …)` macro that expands to `static` items behind `OnceLock<Selector>`. Compile-time validation, zero runtime parsing cost.
   - A `Selectors` registry on `HtmlContext` (`ctx.selectors.parse_or_get("a.detail")`) for handlers that compute selector strings dynamically.
   - Status quo: users call `Selector::parse(…).unwrap()` inline.
   - A streaming `lol_html` extractor used only by `EnqueueLinker` for the engine hot path, while handlers still see `scraper::Html`.
   We will not pick one in the abstract — we'll measure on a 100k-page crawl whether selector-parse or full-body parsing dominates handler time. If it doesn't, we ship the macro only (zero-cost when used, no runtime API surface to support forever).
3. **Dynamic configuration reload (Gemini #3.2, ChatGPT Pro #7).** Currently a built crawler's `Configuration` is immutable. Allow `set_log_level` / `set_proxy_strategy` post-build via an `Arc<RwLock<RuntimeConfig>>` for the small subset that's safe to mutate live? Decision: defer to **post-1.0** unless a use case appears. The candidate mutable surface — log level, proxy strategy choice (not the proxy URLs themselves), autoscaler ceiling — is documented here so we don't have to re-derive it later.
4. **Scaffolding (ChatGPT Pro #4).** Crawlee ships a CLI scaffolder. Two-stage plan:
   - **Pre-0.2:** publish a `cargo-generate` template (`millipede-template`) under the same org. Cheap to maintain — it's just a starter `Cargo.toml` + `main.rs` for each crawler kind. Lowers the adoption barrier without committing to a CLI surface we'd have to support.
   - **Post-1.0:** the full `millipede-cli` crate (`millipede new`, `millipede run`, future `millipede serve` for the inspector UI). Tracked as an issue, not a phase.
5. **Distributed crawl support.** `RequestQueue` lease semantics open the door; a Redis-backed impl is the obvious post-1.0 add. Apify platform integration is a longer arc — see also the Crawlee parity & migration notes below.
6. **`CrawlError` source granularity (Gemini #1.3).** Variants currently wrap `anyhow::Error`. Consider replacing with typed `source` chains (`NetworkError { source: reqwest::Error }`, `ParseError { source: scraper::Error }`) before 1.0 to give users structured matching power. Open until we collect feedback on what failure-handling patterns users actually want.
7. **`UserData` typing (Gemini #1.2).** `serde_json::Map`-backed is the practical choice. Worth exploring a derive macro (`#[derive(UserData)]`) that generates typed accessors against an underlying map. Post-1.0.

**Ecosystem & migration (ChatGPT Pro #5, #6):**

- **`millipede-extras` (community crate).** Crawlee's `infinite_scroll`, `save_snapshot`, `enqueue_links_by_click_elements`, login helpers, etc. are deliberately out of `millipede-core` (see §21). Rather than absorb that surface, we will publish an opt-in `millipede-extras` crate as a venue for these — same org, looser API stability bar than core, semver-independent of `millipede` itself. Helpers ship there until they earn a place in core. Tracked as a post-1.0 effort; the policy doc is what we want in place by 0.1.0 so contributors know where to send PRs.
- **Crawlee → Millipede migration guide.** Phase 8 (the release candidate) must include a side-by-side guide covering: error-handling semantics (typed `CrawlError` vs. string sniffing), router semantics (label + method matching, no `this`-binding), `enqueue_links` API differences (typed builder vs. options object), storage layout (FS layer is wire-compatible — see §8.2), and the `useState` ⇒ `kvs.auto_saved` mapping. This is a hard exit criterion for 0.1.0, not a stretch goal.
- **Apify platform deployment.** Out of scope until the `millipede-storage-apify` crate exists (post-1.0). When it lands, we ship: a `Dockerfile.example` per crawler kind, the `APIFY_*` environment variable mapping, and a "running on Apify" section in the guide. Tracked so we don't lose context, but no work in 0.1.0.
