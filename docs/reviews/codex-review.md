**Top Issues**

1. **The storage traits are not object-safe, despite being used as `Arc<dyn Trait>`.**  
This is the biggest hard break. [INTERFACE.md](/Users/apple/Github/satvik007/millipede/docs/INTERFACE.md:358) returns `Arc<dyn Dataset>`, `Arc<dyn KeyValueStore>`, and `Arc<dyn RequestQueue>`, but `Dataset::push<T>`, `push_batch<T>`, `KeyValueStore::get<T>`, `set<T>`, and `auto_saved<T>` make those traits non-object-safe. `async_trait` does not fix generic methods on trait objects.

Suggested split:

```rust
#[async_trait]
pub trait Dataset: Send + Sync {
    async fn push_json(&self, item: serde_json::Value) -> Result<()>;
    async fn push_bytes(&self, item: Bytes, content_type: ContentType) -> Result<()>;
}

pub trait DatasetExt: Dataset {
    async fn push<T: Serialize + Send + Sync>(&self, item: &T) -> Result<()> {
        self.push_json(serde_json::to_value(item)?).await
    }
}
impl<T: Dataset + ?Sized> DatasetExt for T {}
```

Same for KVS: object-safe core methods plus extension helpers. `auto_saved<T>` probably belongs on a typed wrapper, not on `dyn KeyValueStore`.

2. **`CrawlerKind::make_context` is underspecified and probably the wrong boundary.**  
The design says the engine has a fetched request and asks `CrawlerKind` to make a context. But HTTP and browser crawlers do not just “make context”; they own navigation/fetch, hook ordering, response parsing, blocked-request detection, cookie extraction, `loaded_url`, redirect strategy checks, and skip semantics. Crawlee’s `HttpCrawler` does substantial lifecycle work around `_handleNavigation`, response parsing, blocked detection, and post-navigation hooks before handler execution.

Make the trait model lifecycle, not just context construction:

```rust
#[async_trait]
pub trait CrawlerKind: Send + Sync + 'static {
    type Context: Send + 'static;

    async fn before_request(&self, env: &mut RequestEnv) -> Result<(), CrawlError> { Ok(()) }
    async fn execute(&self, env: RequestEnv) -> Result<Self::Context, CrawlError>;
    async fn after_success(&self, ctx: &mut Self::Context) -> Result<(), CrawlError> { Ok(()) }
    async fn cleanup(&self, result: RequestResult<Self::Context>) -> Result<(), CrawlError>;
}
```

Or more simply: keep one private engine loop, but each kind supplies a `RequestProcessor` that owns fetch/navigation. `make_context` alone will collapse under HTTP/browser special cases.

3. **`Crawler<Kind>` plus `HtmlCrawler` naming is confusing and likely to produce awkward builders.**  
The docs use `pub struct HttpCrawler { ... }` as a kind, then call `HtmlCrawler::builder()` and return something with `run()`. Is `HtmlCrawler` the kind or the crawler? This ambiguity will leak everywhere.

Prefer separate names:

```rust
pub struct Crawler<K> { ... }

pub struct HttpKind { ... }
pub type HttpCrawler = Crawler<HttpKind>;

impl HttpCrawler {
    pub fn builder() -> HttpCrawlerBuilder { ... }
}
```

For browser:

```rust
pub type BrowserCrawler<P> = Crawler<BrowserKind<P>>;
```

Do not make users reason about whether `HtmlCrawler` is a flavor, a kind, or the engine.

4. **The handler API forces owned context and `'static` futures everywhere. That is safe, but expensive and less ergonomic than needed.**  
`RequestHandler<C>::handle(ctx: C) -> BoxFuture<'static, ...>` means every context must be fully owned and spawn-compatible. That is reasonable for `tokio::spawn`, but it prevents borrowing large immutable state like parsed HTML or response bytes within the engine task. It also means browser pages must be `Send + Sync + 'static`, which may not hold for CDP clients.

You probably want two layers:

```rust
pub trait RequestHandler<C>: Send + Sync + 'static {
    fn handle(&self, ctx: C) -> BoxFuture<'static, Result<(), CrawlError>>;
}
```

for public spawned handlers, but internally contexts should carry handles like `Arc<ResponseBody>` / `PageHandle`, not raw provider pages. For browser, do not expose `P::Page` directly unless you have proven `chromiumoxide::Page: Send + 'static` across awaits.

5. **`BrowserProvider` is too generic in the wrong places and too concrete in the user context.**  
`BrowserContext<Page> { page: Page }` pushes provider generics into every handler, router, middleware, and example. That creates noisy types and makes it hard to write reusable browser utilities.

Expose a stable `PageHandle` trait/object or wrapper:

```rust
pub struct BrowserContext {
    pub page: PageHandle,
    ...
}

pub struct PageHandle {
    inner: Arc<dyn BrowserPage>,
}
```

Keep provider generics inside `BrowserPool<P>`. Crawlee’s browser pool has page IDs, controllers, page close hooks, retirement rules, page-to-browser lookup, plugin ordering, and launch/page option mutation. The current provider trait mostly models “launch/new_page/goto/close”; it misses the controller lifecycle that makes a browser pool reliable.

Also, “RAII closes page on drop” is not enough in async Rust. `Drop` cannot `.await`. You need explicit `close().await`, background close task, or an owned guard whose drop sends a close command to a worker.

6. **The autoscaler design is plausible, but it underspecifies wakeups, fairness, and cancellation.**  
Avoiding a shrinking semaphore is fine. Crawlee effectively uses a dispatch loop with current/desired concurrency, periodic maybe-run, and “maybe run when a task finishes.” The docs mention `AtomicUsize` but not the important parts: a `Notify` on task completion/new request, gating concurrent `is_task_ready` calls, task timeout cancellation semantics, pause/resume, and what happens when `run_task` panics.

Use a single scheduler actor, not scattered atomics:

```rust
struct Scheduler {
    desired: usize,
    in_flight: FuturesUnordered<JoinHandle<TaskResult>>,
    notify: Arc<Notify>,
    shutdown: CancellationToken,
}
```

Atomics are fine for snapshots, but the scheduling state should have one owner. Otherwise you will get duplicate spawns, missed wakeups, or incorrect finish detection when the queue is temporarily empty.

7. **`EnqueueLinker` as a per-context object hides too much mutable crawl policy.**  
Crawlee’s `enqueueLinks` injects crawl depth, max crawl depth, robots.txt filtering, per-request enqueue limit, max requests per crawl, enqueue strategy, redirect skip behavior, and skipped-request callbacks. The current linker is mostly a link extractor plus queue adder. That will miss subtle behavior.

Make enqueue a service tied to the crawler run:

```rust
pub struct Enqueuer {
    queue: Arc<dyn RequestQueue>,
    policy: Arc<CrawlPolicy>,
    parent: RequestMeta,
    extractor: Option<Arc<dyn LinkExtractor>>,
}
```

Also, `transform<F: Fn(&mut Request) -> bool>` is too weak. Crawlee transform can mutate or reject, but Rust should return a typed enum:

```rust
enum TransformResult {
    Enqueue(Request),
    Skip { reason: SkipReason },
}
```

And it may need to be async for URL normalization, DB lookups, or robots/policy decisions.

8. **Request queue semantics are too local-memory-shaped.**  
`fetch_next() -> Option<Request>` is not enough for a queue that may later support FS, Redis, or Apify-style distributed locking. Crawlee queues lock fetched requests, reclaim them, delete locks as a safety net, distinguish “empty right now” from “finished,” and track forefront ordering. The roadmap’s `VecDeque + HashMap` memory queue will not exercise those semantics.

Suggested core shape:

```rust
pub struct Lease {
    pub request: Request,
    pub lease_id: LeaseId,
    pub expires_at: Instant,
}

#[async_trait]
pub trait RequestQueue: Send + Sync {
    async fn add(&self, req: Request, opts: AddOptions) -> Result<QueueOpInfo>;
    async fn fetch_next(&self) -> Result<Option<Lease>>;
    async fn mark_handled(&self, lease: Lease) -> Result<()>;
    async fn reclaim(&self, lease: Lease, opts: ReclaimOptions) -> Result<()>;
    async fn renew(&self, lease_id: &LeaseId) -> Result<()>;
}
```

Even if memory storage ignores lease expiry, the API should not need a breaking redesign when distributed queues arrive.

9. **Session modeling uses atomics for fractional scoring and exposes sync lock guards across async code.**  
`error_score_decrement: f32` conflicts with `AtomicU32 error_score`. Crawlee’s score decrements by `0.5` by default. Either store scaled integer points or use a mutex-protected state struct.

Also, `user_data(&self) -> RwLockReadGuard<'_, UserData>` is a trap: users can accidentally hold a lock guard across `.await`. Prefer closure-based sync access or async methods that clone/replace:

```rust
pub async fn user_data<T>(&self, f: impl FnOnce(&UserData) -> T) -> T;
pub async fn update_user_data(&self, f: impl FnOnce(&mut UserData));
```

If this uses `std::sync::RwLock`, document that no guard crosses await; if using `tokio::sync::RwLock`, the guard still should not be part of the everyday API.

10. **Roadmap scope is optimistic and front-loads unstable abstractions.**  
Phase 1 includes all storage traits, request queue, KVS, dataset, events, config, and full error taxonomy. But the storage traits are currently invalid, and queue semantics depend on the engine. Phase 2 includes autoscaler, engine, statistics persistence, router, and retries in two weeks. That is too much for a foundational phase.

Rescope around vertical slices:

- Phase 1: request model, object-safe storage traits, memory queue with lease semantics, minimal dataset/KVS.
- Phase 2: fixed-concurrency engine only, no autoscaling. Prove retries, leases, failure handler, graceful shutdown.
- Phase 3: HTTP crawler, sessions, proxy, hooks, blocked status handling.
- Phase 4: autoscaler after real HTTP timings exist.
- Phase 5: HTML routing/enqueue links.
- Phase 6+: FS storage, sitemap, browser.

Browser should remain experimental until provider Send/lifetime behavior is proven with a spike. `chromiumoxide` may be fine, but the API should not be stabilized around it first.

**Other Misses**

Robots.txt is only visible in Crawlee comparison, not really specified as a first-class policy. Same-domain delay is missing. `max_requests_per_crawl`, `max_crawl_depth`, request handler timeout, navigation timeout, internal operation timeout, and `retry_on_blocked` need public options early because they affect engine architecture.

Observability is underdeveloped. `tracing` is good, but crawlers need per-request spans, queue depth gauges, concurrency gauges, retry counters, status-code counters, and optionally `metrics`/OpenTelemetry integration. This should not wait until polish.

The CLI can wait, but migration tooling and storage compatibility should not be promised casually. “Wire-compatible enough” with Crawlee storage is a test-suite-sized commitment. Either explicitly make it best-effort import-only, or defer the claim.