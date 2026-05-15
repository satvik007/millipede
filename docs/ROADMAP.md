# Millipede — Implementation Roadmap

> This is a phased plan for building [Millipede](./INTERFACE.md) from an empty repository to a 1.0 release. Each phase ends in a usable, tested artifact. Phases are not strictly time-boxed — they are *scope-boxed*.

The companion `INTERFACE.md` describes the target API. This document describes *the order in which we build it, what we depend on, and what we ship at each step*.

---

## Guiding Constraints

1. **Every phase produces a runnable example.** No phase ends "the engine compiles but nothing crawls."
2. **Tests are written alongside code, not after.** The bar is: each public trait has at least one integration test exercising it against the in-memory storage and a `wiremock`-backed HTTP server.
3. **The public API is permitted to break until 1.0.** We use `0.x.y` semver — `0.x.0` may break; `0.x.y` does not.
4. **No premature browser work.** A user crawling 90% of the modern web from Rust will use HTTP + HTML parsing. Browser support is real work and must not block earlier value.
5. **Reference Crawlee, do not transliterate it.** When in doubt, ask "what would a Rust user expect?" before "what does Crawlee do?".
6. **MSRV: Rust 1.75 stable** (for stable `async fn` in traits, where used). Re-evaluate at each phase.

---

## Dependency Choices (Locked at Phase 0)

| Concern | Crate | Notes |
|---|---|---|
| Async runtime | `tokio` (full) | Only runtime supported in v1. |
| HTTP client | `reqwest` | rustls, cookies, gzip, brotli, deflate features. |
| HTML parser | `scraper` | Cheerio analog. `html5ever` underneath. |
| CSS selectors | `scraper::Selector` | Built-in. |
| URL handling | `url` | |
| Cookies | `reqwest_cookie_store` | Persistable, `Arc<CookieStoreMutex>`-friendly. |
| Serde | `serde`, `serde_json` | |
| Errors | `thiserror` + `anyhow` | Library boundary: `thiserror`; user-facing: `anyhow`. |
| Logging | `tracing` + `tracing-subscriber` | |
| Time | `time` (`OffsetDateTime`) | Not `chrono` — fewer deps. |
| System info | `sysinfo` | CPU/memory snapshots. |
| Glob matching | `globset` | For URL pattern matching. |
| Regex | `regex` | |
| HTTP test mock | `wiremock` | Test-only. |
| Browser CDP | `chromiumoxide` | Phase 5+. |
| Sitemap parsing | `quick-xml` or `sitemap` | Phase 4+. |
| Async traits (object-safe) | `async-trait` | Where stable native isn't enough. |
| LRU cache | `lru` | RequestQueue dedup cache. |
| Concurrent maps | `dashmap` | For high-fanout structures. |
| Streams | `futures-util` | `Stream`, `StreamExt`. |
| TLS fingerprinting (later) | `rustls` config + custom verifier | Phase 6 polish. |

We will not depend on `serde_yaml`, `chrono`, or non-tokio runtime crates.

---

## Phase 0 — Project Skeleton & Conventions  (target: ~1 week)

### Scope
- `Cargo.toml` workspace with the crate layout from `INTERFACE.md` §2.
- `rust-toolchain.toml` pinning a recent stable + `rustfmt`, `clippy`.
- `.github/workflows/ci.yml`: build, test, clippy, fmt, doc, MSRV check, all on Linux/macOS.
- License: MIT or Apache-2.0 (TBD — pick before first publish).
- `docs/` already has `INTERFACE.md` and this `ROADMAP.md`; add `CONTRIBUTING.md`, `RELEASE.md`.
- Empty crates with their public `lib.rs` set up. Each crate exposes only `#[doc = include_str!("../README.md")]` and a stubbed prelude.
- `examples/` directory exists with a placeholder.
- `cargo deny` config (advisories, license allowlist).
- Codecov integration.

### Exit Criteria
- `cargo build --workspace` succeeds.
- `cargo test --workspace` runs (zero tests, but the test infra is in place).
- `cargo doc --workspace --no-deps` produces docs.
- CI green on PR.

### Out of Scope
Any real types or APIs. Just infrastructure.

---

## Phase 1 — Request Model, Object-Safe Storage, Lease-Based Memory Queue  (target: 1–2 weeks)

### Scope
Land the foundational data model and an in-process queue that the engine can drive in later phases. Object-safety and lease semantics are *required* in this phase — getting them right now avoids a forced API break once the engine and a second backend exist.

- `millipede-core::request` — `Request`, `RequestBuilder`, `RequestId`, `RequestState`, `UserData`, `Method`, `RequestBody`. Round-trip serde.
- `millipede-core::storage` traits per INTERFACE.md §8.1: object-safe `Dataset`/`KeyValueStore`/`RequestQueue` cores + blanket `DatasetExt`/`KeyValueStoreExt`. Standalone `AutoSaved<T>`.
- Lease-based queue API: `Lease`, `LeaseId`, `mark_handled` / `reclaim` / `renew` / `abandon`.
- `millipede-storage-memory::MemoryStorageClient`:
  - `MemoryRequestQueue`: dedup `HashMap<unique_key, RequestId>` + `VecDeque` of pending + `HashMap<LeaseId, LeasedRequest>` for in-flight. Lease expiry is a no-op (single-process), but the API surface enforces the contract.
  - `MemoryDataset`: `Mutex<Vec<serde_json::Value>>`.
  - `MemoryKeyValueStore`: `Mutex<HashMap<String, KvEntry>>`.
- `millipede-core::errors::CrawlError` (full enum, classification helpers, `From` impls).
- `millipede-core::events::{EventBus, CrawlerEvent}` (broadcast).
- `millipede-core::config::{Configuration, ConfigurationBuilder}`.

### Tests
- Property test for `Request::unique_key` (same URL + method + body ⇒ same key).
- Roundtrip serde for `Request`.
- Concurrent add/fetch/mark_handled on `MemoryRequestQueue` (100 concurrent producers + 10 consumers).
- `reclaim` on a lease increments `retry_count`; `abandon` does not.
- Object-safety smoke test: `let _: Arc<dyn Dataset> = ...;` compiles; same for KVS and queue.
- `DatasetExt::push::<MyStruct>` round-trips through a `dyn Dataset`.
- KVS list / delete / `AutoSaved::persist` round-trip.

### Exit Criteria
- `cargo test -p millipede-core -p millipede-storage-memory` passes.
- `cargo doc` lands the trait docs.
- `cargo public-api` baseline captured (will diff against this in every subsequent phase — see "Release Discipline" below).
- A `cargo run --example phase1_queue_demo` exists: spawns 8 tasks, all pulling leases from a shared queue, marking handled, and demonstrating dedup.

---

## Phase 2 — Fixed-Concurrency Engine Loop  (target: 1–2 weeks)

**Autoscaling is deferred to Phase 4.** A fixed-concurrency engine lets us prove lease handoff, retry classification, panic isolation, graceful shutdown, and statistics correctness *without* the additional variable of autoscaler decisions. This was Codex review item #10: the original Phase 2 bundled too much.

### Scope
A `BasicCrawler` equivalent: drives the queue, invokes a user `RequestHandler`, but does no actual HTTP. The "fetcher" at this phase is the identity (the engine just hands the request to the handler).

- `millipede-core::engine::Engine` (private) — single dispatch task with `FuturesUnordered` + `Notify` + `CancellationToken`. Hard-coded `max_concurrency` (no scaling yet).
- `millipede-core::CrawlerKind` lifecycle trait per INTERFACE.md §4.1 (`start`/`before_request`/`execute`/`after_success`/`cleanup`/`stop`).
- `BasicKind` / `BasicCrawler = Crawler<BasicKind>` whose `execute` is the identity.
- `millipede-core::handler::{RequestHandler, blanket Fn impl}`.
- `millipede-core::router::Router` with `route` / `route_method` / `route_methods` / `default` / `middleware`. `HasRequest` trait.
- `millipede-core::statistics::{StatisticsHandle, StatisticsSnapshot, FinalStatistics}` — sliding-window counters + persistence to KVS.
- `CrawlerHandle` (weak back-reference) per §4.3.
- Timeout enforcement: `request_handler_timeout`, `internal_operation_timeout` (no `navigation_timeout` yet — no fetch).

### Tests
- Engine respects `max_concurrency` (no more than N tasks in flight, observed via barriers).
- Retry classification: `Retry` increments `retry_count`, `Session` increments `session_rotation_count`, `ForceRetry` ignores `max_retries`, `NonRetryable` calls the failure handler, `Critical` halts.
- Handler panic does not poison the engine — the request is reclaimed with an error.
- Graceful shutdown: `Crawler::stop()` drains in-flight tasks; `abort()` cancels them.
- Statistics persistence: emit `PersistState`, read back from KVS, confirm fields.
- `Router::route_method` correctly routes by `(label, method)`; missing route returns `CrawlError::MissingRoute`.

### Exit Criteria
- Example: `examples/basic_engine.rs` enqueues 1000 synthetic requests with random handler delays + occasional `CrawlError::Retry` / `CrawlError::NonRetryable` and verifies `requests_finished + requests_failed == 1000` with `retries > 0`.
- `cargo public-api` diff is reviewed and accepted on PR.

---

## Phase 3 — HttpCrawler with Sessions and Proxy  (target: 2–3 weeks)

### Scope
A complete `HttpCrawler`: real HTTP fetches via `reqwest`, session pool, proxy rotation, cookie jars, retries on network errors.

- `millipede-core::http_client::{HttpClient, HttpRequest, HttpResponse, StreamingResponse}` traits.
- Concrete `ReqwestClient` impl in `millipede-http`:
  - Header injection (user-agent rotation; static set in this phase).
  - Cookie jar threading from `Session`.
  - Proxy URL application per request.
  - Timeout / redirect handling.
- `millipede-core::session::{Session, SessionPool, SessionOptions}`.
  - Cookie store: `reqwest_cookie_store::CookieStoreMutex` shared via `Arc`.
  - Error scoring, retirement, rotation.
  - Persistence to KVS via `auto_saved`.
- `millipede-core::proxy::{ProxyConfiguration, ProxyResolver, ProxyInfo, RotationStrategy}` + tiered support.
- `HttpCrawler` (in `millipede-http`): the engine kind that performs the fetch and produces `HttpContext`. Routes errors:
  - `reqwest::Error::is_connect` / `is_timeout` ⇒ `Retry`.
  - HTTP 408/429/5xx (configurable list) ⇒ `Retry`. 401/403 ⇒ `Session`.
- `EnqueueLinker` for `HttpContext` (URLs-only mode; no DOM).

### Tests
- `wiremock`-backed HTTP server fixtures.
- Cookie roundtrip across requests in the same session.
- 429 triggers retry, 404 does not, 403 triggers session rotation.
- Proxy round-robin: 3 proxies, 10 requests ⇒ even distribution within tolerance.
- Tiered proxy: domain blocks at tier 0 ⇒ engine probes tier 1 ⇒ recovery probe at lower tier.

### Exit Criteria
- `examples/http_crawl.rs` crawls a mock 100-page site against `wiremock`.
- Each public type has a rustdoc example that compiles.

---

## Phase 4 — Autoscaler  (target: 1–2 weeks)

With Phase 3 producing real HTTP-shaped request timings, the autoscaler now has realistic input to tune against. Splitting this out of Phase 2 (the original plan) means we don't bake autoscaler decisions into an engine that hasn't seen a real workload.

### Scope
- `millipede-core::autoscale::AutoscaledPool` + `LoadSignal` trait + `Snapshotter` + `SystemStatus`.
- Implementations: `CpuLoadSignal`, `MemoryLoadSignal`, `TokioRuntimeLoadSignal`, `ClientLoadSignal`.
- Replace the fixed-concurrency dispatch from Phase 2 with the dynamic dispatch scheduler. Keep the `FuturesUnordered + Notify + CancellationToken` shape — a *single* scheduler actor owning all scheduling state (Codex review #6: don't scatter scheduling across atomics).
- Rate limiting: `max_tasks_per_minute`, `same_domain_delay`.

### Tests
- Engine respects autoscaled concurrency under deterministic fake `LoadSignal`s; `tokio::time::pause` controls clock.
- Property test: random sequence of load signals → desired concurrency is monotonic with respect to signal direction; never overshoots `max_concurrency` or undershoots `min_concurrency`.
- Pause/resume across autoscale ticks does not leak tasks.
- `max_tasks_per_minute` rate-limits without starving any specific domain.

### Exit Criteria
- `examples/autoscale_demo.rs`: crawl 5000 mock pages against a `wiremock` server that occasionally returns 500s. Verify the autoscaler converged below the 200-task ceiling but above 8.
- `cargo public-api` diff accepted.

### Risk
Highest risk in the entire roadmap (Codex #6). Budget 30% extra time. Plan a half-day to write a chaos-style test harness that simulates clock-paused signal sequences.

---

## Phase 5 — HtmlCrawler, Routing, `enqueue_links`, Sitemap & File-System Storage  (target: 2–3 weeks)

### Scope
The phase that makes Millipede usable for "real" scraping projects.

- `millipede-html::HtmlCrawler` — wraps `HttpCrawler`, parses body with `scraper::Html`, exposes `HtmlContext`.
- `millipede-core::link_extraction` — link extractor over `scraper::Html`:
  - `<a href>` enumeration with selector override.
  - Glob/regex/exclude filtering via `globset` + `regex`.
  - Per-pattern overrides (method/headers/label/user_data).
  - `EnqueueStrategy` filter (same-origin/hostname/domain/all).
- `EnqueueLinker` complete API (`.options().selector(…).strategy(…).send()`).
- `Router` fully wired into all three context types.
- `SitemapRequestList`: streams XML sitemap (gzip-aware), emits `Request`s lazily, persists progress.
- `RequestQueueWithSitemap` (tandem): drains sitemap into queue.
- `millipede-storage-fs::FsStorageClient`:
  - Layout: `./storage/datasets/<id>/<seq>.json`, `./storage/key_value_stores/<id>/<key>.<ext>`, `./storage/request_queues/<id>/{requests/, state.json}`.
  - Wire-compatible enough with Crawlee's MemoryStorage on-disk format to inspect a crawl in either.
  - `purge_on_start` honored.

### Tests
- Realistic `wiremock` server with sitemap.xml + nested category pages + product pages.
- Verify same-domain strategy excludes external links.
- Glob `**/products/*` includes only product URLs.
- `transform` callback can mutate or reject requests.
- `FsStorageClient` survives crash mid-run (kill + restart picks up where it left off via persisted state).

### Exit Criteria
- `examples/scrape_books.rs` (against [books.toscrape.com](https://books.toscrape.com), an OSS test site) — yes, this hits real network in `--ignored` tests; CI runs the local-mock version.
- Migration note: a Crawlee user's `./storage/` directory can be opened by `FsStorageClient` and re-crawled.

---

## Phase 6 — BrowserCrawler with `chromiumoxide`  (target: 3–4 weeks)

### Scope
- `millipede-browser`: `BrowserProvider` trait, `BrowserPool`, `BrowserHooks`, `PageHandle`.
- `millipede-browser-chromiumoxide::ChromiumoxideProvider`: launches Chromium, manages pages, exposes cookies, performs `goto`.
- `BrowserCrawler<P>` (engine kind): drives `BrowserPool`, builds `BrowserContext<P::Page>`.
- DOM-level `enqueue_links` for `BrowserContext` (evaluate JS to enumerate `<a>` selectors).
- Default hooks:
  - `pre_launch`: apply proxy + launch args.
  - `post_page_create`: install cookies from session.
  - `pre_page_close`: extract cookies back into session.

### Tests
- Headless browser smoke test against a local static fixture (`tiny_http`-served HTML).
- Verifies cookie persistence across page recycles.
- Verifies `maxOpenPagesPerBrowser` and `retireBrowserAfterPageCount`.

### Exit Criteria
- `examples/browser_crawl.rs` runs a headless Chromium crawl over a local site.
- CI runs the browser tests only on Linux + macOS with Chromium pre-installed (cache the download); skip on Windows for now.

### Risks
- Chromiumoxide's Page lifecycle and `await`-friendliness — confirm `Send` boundaries early.
- Resource leaks on panic — every `PageHandle` must close on drop.

---

## Phase 7 — Fingerprinting, Polish, Error Snapshotter  (target: 2 weeks)

### Scope
- `millipede-fingerprint`:
  - Header generator (user-agent + Accept-* combinations) — port a curated subset of the Apify `header-generator` dataset.
  - Browser fingerprint generator stub for use in the `post_page_create` hook.
- `ErrorSnapshotter`: on `failed_request_handler`, capture page HTML + screenshot (browser) / response body (HTTP) into KVS at a hashed key.
- `ClientLoadSignal` wired to `StorageClient` rate-limit errors via a channel.
- `max_tasks_per_minute` rate limiting in the autoscaler.
- Pre/post navigation hooks fully implemented and tested.
- `Statistics::error_tracker` grouping (Crawlee's `name + stack-prefix` normalisation).

### Tests
- Fingerprint determinism: same `session_token` ⇒ same header set.
- Error snapshot files written to KVS on failure and reloadable.

### Exit Criteria
- A real-world target (one we don't control, with `--ignored` test) crawls without trivial bot detection blocks at default settings.

---

## Phase 8 — `0.1.0` Release Candidate  (target: 1–2 weeks)

### Scope
- Audit public API for consistency (naming, async signature uniformity).
- Run `cargo public-api` and lock the surface; document everything.
- README per crate, with examples.
- One canonical book-style guide in `docs/guide/`.
- Migrate `INTERFACE.md` "Open Questions" to resolved decisions or filed issues.
- Run `cargo semver-checks` baseline.
- Examples directory: at least four (basic, http, html-with-routing, browser).
- Benchmark suite (`criterion`) for queue ops, link extraction, and per-request overhead. Establish baseline numbers.
- Publish to crates.io as `0.1.0`.

### Exit Criteria
- All crates publish.
- Example projects' `cargo run` works against published versions.
- README badges green.
- A 1-paragraph announcement post is drafted.

---

## Post-1.0 / Future Work (Tracked as Issues, Not Roadmap)

- Redis-backed `RequestQueue` for distributed crawls.
- `millipede-storage-apify`: Apify platform client.
- Playwright provider (`millipede-browser-playwright`).
- TLS-level fingerprinting (`impit`-style) once a stable Rust dependency exists.
- `millipede-cli`: project scaffolder.
- WASM crawlers (long-tail; needs runtime work).
- Stealth/anti-detection plugin system.
- Per-request rate limiting (domain-aware).
- `tower`-style middleware ecosystem.

---

## Testing Strategy Summary

| Layer | Test type | Tools |
|---|---|---|
| Pure data (`Request`, `UserData`, builders) | Unit + property | `proptest` |
| Storage backends | Integration | `tokio::test`, `tempfile` for FS, real I/O |
| HTTP client + crawlers | Integration | `wiremock`, `tokio::test` |
| Browser crawler | Integration (gated) | `chromiumoxide` headless, local `tiny_http` |
| Autoscaler decisions | Simulation + property | Fake `LoadSignal`, deterministic time via `tokio::time::pause` |
| API stability | Snapshot | `cargo public-api`, `cargo semver-checks` |
| Bench | Microbench | `criterion` |
| Examples | Smoke | `cargo run --example` in CI |

A full CI run executes: build, clippy `-D warnings`, fmt check, test, doc, MSRV check, examples smoke, public-api diff. Browser tests run on a separate matrix entry.

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Tokio's lack of resizable semaphore makes autoscaling awkward | Medium | Medium | Single scheduler actor with `FuturesUnordered` + `Notify` + `CancellationToken`; never scatter state across atomics. Decision baked into Phase 4. |
| Autoscaler subtle bugs (missed wakeups, fairness, panic isolation) | High | High | Phase 4 budgets 30% slack and a chaos test harness using `tokio::time::pause`. |
| `scraper`/`html5ever` performance on large pages | Medium | Medium | Bench early in Phase 5; fall back to `lol_html` for streaming if needed. |
| `chromiumoxide` lifecycle complexity, `Send + 'static` across awaits | Medium | High | Phase 6 starts with a spike branch to validate `Page` `Send`-across-await *before* committing the `BrowserPage` adapter API. |
| `PageHandle::Drop` cannot `.await` cleanup | High | Medium | Drop posts a close command to a background worker and emits `tracing::warn!` if `close().await` wasn't called. Document the explicit-close idiom. |
| Cookie sharing between `reqwest` and `chromiumoxide` is awkward | Medium | Medium | Build a thin adapter type owned by `Session`; never expose raw cookie store. |
| Crawlee parity on `enqueue_links` semantics is subtle | High | Medium | Port Crawlee's test corpus for link extraction verbatim. `CrawlPolicy` covers depth, robots, max-requests; `SkippedHandler` makes failures observable. |
| TLS fingerprinting requires unsafe / custom rustls | Medium | Medium | Defer to Phase 7; provide hook for users to swap `HttpClient` impls in the meantime. |
| Lease semantics in memory queue diverge from FS/Redis impls | Medium | Medium | Single trait + shared test suite per backend (Phase 5 onward, Phase 1 for memory). Memory backend's "no-op expiry" is documented as such. |
| MSRV churn from async traits | Low | Low | Pin to stable; review every phase. |
| API drift discovered late | Medium | High | `cargo public-api` runs on every PR from Phase 1 onward (Gemini #4.2). |

---

## Cadence & Milestones

| Milestone | Phases | Duration estimate (single full-time dev) | Outcome |
|---|---|---|---|
| **M1: Foundations** | Phase 0–2 | ~4–5 weeks | Fixed-concurrency engine + lease-based memory queue + routing; no HTTP yet. |
| **M2: HTTP & sessions** | Phase 3 | ~3 weeks | Real HTTP crawling with sessions and proxy (no autoscale). |
| **M3: Autoscaler** | Phase 4 | ~1–2 weeks | Dynamic concurrency on real workloads. |
| **M4: Real scraping** | Phase 5 | ~3 weeks | HTML parsing, routing, link extraction with `CrawlPolicy`, sitemap, FS storage. |
| **M5: Browser** | Phase 6 | ~4 weeks | Chromium-based crawling. |
| **M6: Polish** | Phase 7 | ~2 weeks | Fingerprinting, snapshots, rate limiting. |
| **M7: 0.1.0** | Phase 8 | ~2 weeks | Published crates. |

Total estimate: ~19–21 weeks of focused work to `0.1.0`. With part-time effort or contributions, scale accordingly.

## Release Discipline

`cargo public-api` and `cargo semver-checks` ship in CI starting from **Phase 1** (baseline at end of Phase 1). Every PR that touches a public API runs the diff; a non-additive change must either edit the baseline (with a brief PR note explaining why) or be reworked. Gemini review #4.2 flagged the original plan (only at Phase 7) as too late — by then, drifting API decisions are buried under months of work.

`cargo deny` and `cargo audit` run on every PR from Phase 0. License allowlist and advisory database are checked nightly via a scheduled workflow.

---

## Decision Log Hooks

A `docs/decisions/` directory will hold short ADR-style notes for irreversible choices (storage layout, error taxonomy, dependency picks). Created lazily — first ADR is "ADR-0001: MSRV and async-fn-in-trait policy" at Phase 0 close.
