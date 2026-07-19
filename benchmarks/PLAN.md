# Millipede vs Spider — Final Head-to-Head Benchmark Plan

**Status:** approved for implementation (merged from two independent draft plans + review-finding resolution; see §12).
**Repo:** `/Users/apple/Github/satvik007/millipede` · **Comparison target:** `spider` crate.

## Verified facts (checked 2026-07-19)

- `spider` latest release is **2.52.9** (published 2026-07-09), license **MIT** (passes this repo's MIT/Apache-2.0-style allowlist in `deny.toml`), **no declared `rust-version`** (crates.io API).
- spider 2.52.9 `Website` API confirmed on docs.rs: `with_limit`, `with_concurrency_limit(Option<usize>)`, `with_delay(u64)`, `with_respect_robots_txt(bool)`, `with_subdomains`, `with_tld`, `with_depth`, `with_user_agent(Option<&str>)`, `with_request_timeout(Option<Duration>)`, `with_retry(u8)`, `with_redirect_limit`, `with_normalize`, `with_return_page_links`, `with_shared_queue`, `with_no_control_thread(bool)`, `with_modify_headers(bool)`, `with_full_resources`, `subscribe`/`unsubscribe` (needs `sync` feature), `crawl()`, `crawl_raw()`, `scrape()`, `crawl_smart()` (`smart` feature), `get_links()`. `subscribe`'s exact signature (capacity → tokio broadcast receiver of `Page`) must be re-confirmed against the pinned version at implementation time; prior source inspection places it in `spider/src/website.rs` returning a `tokio::sync::broadcast::Receiver<Page>`.
- spider 2.52.9 default features: `basic` (which activates disk storage + native TLS + reqwest native roots), `io_uring`, `numa`, `splice`, `tcp_fastopen`, `zero_copy` — several Linux-only. `sync` enables the tokio broadcast subscription. spider depends on **reqwest ^0.13** and **lol_html ^2** (its native link parser).
- Millipede API confirmed in-repo: `HttpKind::builder()` with `disable_sessions`, `coalesce_in_flight`, `header_generator`, `user_agents`, `retry_status_codes`, `retry_server_errors`, `session_status_codes`, `request_timeout`, `max_redirects` (`crates/millipede-http/src/kind.rs`); `HtmlKind::from_http(HttpKind)` (`crates/millipede-html/src/kind.rs`); `Crawler::builder` with `min_concurrency`/`max_concurrency`/`desired_concurrency`, `max_request_retries`, `max_session_rotations`, `same_domain_delay`, `crawl_policy`, `storage_client` (`crates/millipede-core/src/crawler/builder.rs`); `CrawlPolicy::new().strategy(EnqueueStrategy::SameHostname).max_requests_per_crawl(n)` (`crates/millipede-core/src/link_extraction.rs`); `FinalStatistics { requests_finished, requests_failed, requests_retries, .. }`. Millipede uses **reqwest 0.12** (with `gzip`/`brotli`/`deflate` features) and **scraper 0.24**.
- Repo toolchain: `rust-toolchain.toml` pins **1.96**; workspace MSRV for published crates is 1.85 and is unaffected by this plan (see §12, B-1).

## 1. Scope and claims policy

Headline suite: **HTTP-only, offline, loopback**, synthetic axum site, identical page sets, fixed concurrency, zero client-side delays, no retries, robots disabled on both engines. Every accepted timing must pass exact-count/byte/digest validation (§8).

Published claims are scoped to what is measured: success-path HTTP/1.1 (plus one redirect and one compression scenario) crawl throughput and peak RSS on loopback. The report must state explicitly that it does **not** characterize TLS, DNS, error/retry paths, anti-bot behavior, JS rendering, or politeness compliance. Live-network examples are never benchmarked directly; their workload shapes are replicated locally. Browser/smart-crawl comparison is a separately-reported optional suite (§10), off by default.

Existing criterion micro-benches (`queue_ops.rs`, `engine_overhead.rs`, `selector_bench.rs`, `link_extraction.rs`) stay as-is for regression tracking; spider exposes no equivalent stable component APIs, so no head-to-head microbenchmarks.

## 2. Spider pin and feature policy

```toml
spider = { version = "=2.52.9", default-features = false, features = ["sync"] }
```

- **Headline config = `sync` only.** Rationale: spider's default `basic` bundle activates disk storage and native TLS; `io_uring`/`splice`/`tcp_fastopen`/`numa`/`zero_copy` are Linux-only. The minimal portable set makes the headline comparable to millipede's in-memory configuration on both macOS and Linux, and avoids charging spider for disk-cache writes.
- **Sensitivity run:** cargo feature `spider-upstream-defaults = ["spider/default"]` produces separately-labelled rows (Linux). Never merged into the headline table. This resolves both "don't silently trim spider's features" and "don't charge spider for disk writes" concerns: both configs are published, clearly labelled.
- Committed `benchmarks/spider-bench/Cargo.lock` pins the transitive graph. The bench package has its own `deny.toml` (root policy copied; bench-local additions documented inline) and a manual CI job runs `cargo deny check` against it (§11).
- Toolchain: whatever `rust-toolchain.toml` pins (currently 1.96). **No 1.85 gate for the bench package** (§12, B-1).

## 3. Example triage (merged)

| Example | Verdict | Mapping |
|---|---|---|
| `http_crawl.rs` | **Adopt shape, replace extractor** | Binary-tree fan-out + queue dedup → `tree`, `latency`, `payload`, `redirects`, `compressed` site shapes. Its `href="` string-splitter is a placeholder and is **not used** in any headline row (§12, B-2); all millipede rows use `HtmlKind`. |
| `basic.rs` | **Adopt (config pattern)** | `HtmlKind` + `EnqueueStrategy::SameHostname` + off-host-link leak check is the configuration template for every scenario. |
| `scrape_books.rs` | **Adopt shape, reject live form** | Catalog shape (listings → details, selector extraction) → `books` scenario, fully local, memory-only digest accumulation. |
| `hackernews.rs` | **Adopt shape, reject live form** | Link-dense large pages, duplicate anchors, pagination, selector-heavy extraction → `hn` scenario, local, no politeness delay. |
| `autoscale_demo.rs` | **Adopt workload, reject feature** | AIMD vs spider's scheduling is apples-to-oranges; its flat 5,000-URL workload → `wide` scenario at fixed concurrency both sides. |
| `rate_limit.rs` | **Adopt shape as server-side latency only** | Benchmarking configured sleeps measures `tokio::time::sleep`. Instead `latency` gives both engines identical 10 ms server-side latency — measures pipeline-keeping, which is a real differentiator. |
| `basic_engine.rs` | Reject | No HTTP (`BasicKind`); spider has no network-free mode. Covered by existing `engine_overhead.rs` bench. |
| `error_handling.rs` | Reject | Retry/anti-bot semantics differ by design; a comparison would encode policy, not performance. Revisit only with an exact attempt-schedule audit. |
| `proxy_switcher.rs` | Reject | Proxy hop dominates; round-robin semantics differ. |
| `fingerprint_crawl.rs` | Reject | Header generation is µs vs ms of I/O; spider's `ua_generator` differs by design. |
| `browser_crawl.rs`, `smart_crawl.rs` | Optional/secondary | Chromium dominates cost and variance. Separate suite, off by default (§10). |
| `phase0_hello.rs`, `phase1_queue_demo.rs` | Reject | Trivial demos; spider exposes no standalone queue API. Dedup shape folded into `mesh`. |

## 4. Benchmark matrix

Common controls: axum server on `127.0.0.1:0` (IP-literal URLs, no DNS), HTTP/1.1 keep-alive, `Content-Type: text/html; charset=utf-8`, `Content-Length`, `Cache-Control: no-store`, identity encoding except `compressed`, pre-rendered immutable `Bytes` bodies (O(1) clone), run-nonce path segment, `/robots.txt` served (`Allow: /`) but **must receive zero hits**. Every internal page also carries one duplicate root link (dedup check) and one off-host link (`http://localhost:<port>/…` while seeds use `127.0.0.1` — a filtering failure stays local and observable). Deterministic seeded generation; bodies padded with inert comments to exact sizes.

Concurrency **C = 32** primary; optional sweep `{8, 32, 128}` via `--concurrency`. Tokio runtime: `worker_threads = 4` on both engines (CLI-overridable, recorded in metadata).

| # | Scenario | Site shape | Pages | Page size | Notes |
|---|---|---|---|---|---|
| 1 | `tree` | Binary tree depth 13; `/p/{i}` links `2i+1`, `2i+2` | 8,191 | 4 KiB | Scheduler + dedup dominated; scale via `--depth` if median < 750 ms |
| 2 | `wide` | Root links to 5,000 leaves | 5,001 | root ~220 KiB, leaves 2 KiB | Frontier fan-out stress (autoscale_demo shape) |
| 3 | `mesh` | 8,192 pages; page *i* links to 8 seeded forward pages + 2 duplicates + off-host | 8,192 | 4 KiB | ~90k link candidates, dedup stress; every page fetched exactly once |
| 4 | `latency` | Tree depth 11; server sleeps 10 ms per response | 2,047 | 4 KiB | Pipeline-keeping under symmetric server latency |
| 5 | `payload` | Tree depth 10 | 1,023 | 256 KiB | ~256 MiB transferred; report MiB/s; parser cost on large bodies |
| 6 | `redirects` | Tree depth 11; every link targets `/r/{i}` → `301` → `/p/{i}` | 2,047 (+2,047 redirect hops) | 4 KiB | Redirect-path cost; both engines redirect limit 7 |
| 7 | `compressed` | Tree depth 12; compressible HTML, served gzip when `Accept-Encoding` permits | 4,095 | 32 KiB decoded | Real content-decoding path; server records per-request `Accept-Encoding`; report bytes-on-wire per engine |
| 8 | `books` | 100 listing pages (50 detail links + next-page) + 5,000 details | 5,100 | listings 16 KiB, details 4 KiB | Extraction: `h1` title + `p.price` per detail via `scraper`; digest-validated |
| 9 | `hn` | 40 front pages (25 story links + pagination, duplicate title/comment anchors) + 1,000 items (40 comments each) | 1,040 | fronts 48 KiB, items 32 KiB | Link-dense large pages; extraction: 1,000 stories + 40,000 comments; digest-validated |

Scenarios 1–7 are **raw rows**: per-page work is count + decoded-byte-sum + running checksum only, on both engines, with **no re-parse** (§7, parse parity). Scenarios 8–9 are **extraction rows**: identical `scraper` selector code runs in millipede's handler and in spider's subscriber.

## 5. Engine configurations (exact)

### Millipede (all scenarios)

```rust
let http = millipede::HttpKind::builder()
    .disable_sessions()                      // spider has no equivalent session-pool work in this baseline
    .coalesce_in_flight(false)
    .header_generator(false)
    .user_agents(["millipede-bench/1.0"])
    .retry_status_codes([])
    .retry_server_errors(false)
    .session_status_codes([])
    .request_timeout(Duration::from_secs(15))
    .max_redirects(7)
    .build()?;
let kind = millipede::HtmlKind::from_http(http);   // real DOM parse; parsed once, shared with handler

let crawler = millipede::Crawler::builder(kind)
    .min_concurrency(c).max_concurrency(c).desired_concurrency(c)   // fixed; no autoscale
    .max_request_retries(0)
    .max_session_rotations(0)
    .same_domain_delay(Duration::ZERO)
    .storage_client(Arc::new(millipede::MemoryStorageClient::new()))
    .crawl_policy(
        millipede::CrawlPolicy::new()
            .strategy(millipede::EnqueueStrategy::SameHostname)
            .max_requests_per_crawl(expected_pages + 16),           // slack; exact-count gate catches truncation
    )
    .request_handler(move |ctx: millipede::HtmlContext| async move {
        // raw rows: count + byte-sum + checksum from ctx.response (no extra parse; DOM already built by HtmlKind)
        // extraction rows: shared selector fns against ctx.html (reuses the one parse)
        let _ = ctx.enqueue.options()
            .strategy(millipede::EnqueueStrategy::SameHostname)
            .send().await?;
        Ok(())
    })
    .build().await?;

let stats = crawler.run(root_url).await?;   // gate: stats.requests_finished == expected && requests_failed == 0
```

### Spider (all scenarios)

```rust
let mut website = spider::website::Website::new(&root_url);
website
    .with_concurrency_limit(Some(c))
    .with_delay(0)
    .with_respect_robots_txt(false)
    .with_retry(0)
    .with_depth(0)                                   // unlimited; site is terminal
    .with_limit(Some(expected_pages + 16))           // re-verify integer type against pinned docs at impl time
    .with_user_agent(Some("millipede-bench/1.0"))
    .with_request_timeout(Some(Duration::from_secs(15)))
    .with_redirect_limit(7)
    .with_subdomains(false)
    .with_tld(false)
    .with_normalize(false)
    .with_return_page_links(false)
    .with_full_resources(false)
    .with_shared_queue(false)
    .with_modify_headers(false)
    .with_no_control_thread(true);

// Capacity >= total messages: the channel can NEVER drop, even if the consumer stalls entirely.
let mut rx = website.subscribe(expected_pages + 64).expect("sync feature enabled");
let drain = tokio::spawn(async move {
    let mut out = Accum::default();
    loop {
        match rx.recv().await {
            Ok(page) => out.record(&page),            // raw rows: count/bytes/checksum; extraction rows: shared scraper fns
            Err(broadcast::error::RecvError::Lagged(_)) => { out.lagged = true; }  // defense in depth => run invalid
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
    out
});
website.crawl().await;      // client construction happens inside — inside the timed region (§6)
website.unsubscribe();
let out = drain.await?;     // gate: out.count == expected && !out.lagged; server hit-set is the authoritative count
```

`scrape()` is never used in headline rows (retains the whole corpus; changes the memory workload). Do not rely on `get_links()` for validation — the server-side hit set is authoritative.

**Signatures re-verified against the compiled spider 2.52.9 (`sync`-only) at scaffold time:** `with_limit` takes a plain `u32` (not `Option<usize>`); `subscribe(capacity)` returns `tokio::sync::broadcast::Receiver<Page>` directly (not `Option`). All other §5 methods exist as listed under the `sync`-only feature set. Drain bodies come from `page.get_html_bytes_u8()`; extraction rows re-parse via `scraper::Html::parse_document(&page.get_html())`.

### Baseline ("speed of light" control)

Raw `reqwest 0.12` client + `FuturesUnordered` at concurrency C fetching the **exact known URL list** through the **same server process and the same instrumented handler path** as the crawlers (single code path; the server cannot distinguish engines). Establishes the server ceiling: if the fastest crawler reaches ≥ 70 % of baseline throughput, the row is flagged possibly server-bound and the scenario must be scaled up before publication.

## 6. Measurement protocol

Custom wall-clock harness, **not criterion**: crawls run 1–10 s, need subprocess isolation for RSS, ready/go handshakes, interleaving, and cross-engine structured output — outside criterion's model. Criterion benches remain for micro-regressions.

Per (scenario, C):

1. Orchestrator pre-renders the site, starts axum in-process, warms it with a small concurrent burst, and measures the baseline ceiling.
2. One unmeasured warm-up run per engine, then **N = 5 measured trials per engine** (CLI `--iters`; ≥ 5 valid samples required to publish a row).
3. Trials interleave engines (`M, S, B, M, S, B, …`) to spread thermal/cache drift.
4. Each trial is a **fresh child process**: `spider-bench run --scenario tree --engine spider --url http://127.0.0.1:PORT/<nonce>/ --concurrency 32 --json`.
5. **Ready/go handshake:** the child parses args and scenario spec, then prints `ready` — **before constructing any HTTP client, crawler, or Website**. The parent records baseline child RSS and replies `go`.
6. The child starts `Instant` on `go`, then **constructs its engine and runs the crawl**: millipede's `HttpKind::build()` + `Crawler::build().await` + `run()`, or spider's config + `subscribe` + `crawl()` + `unsubscribe` + drain-join, or baseline's `Client` build + fetch loop. The timer stops only after all handlers/subscribers have drained. Both engines are thus charged symmetrically for client/pool construction (spider builds its client inside `crawl()`; millipede builds it explicitly inside the same timed region).
7. The child then reads `getrusage(RUSAGE_SELF)` (see §7) and prints one JSON line:

```json
{"scenario":"tree","engine":"spider","pages":8191,"wall_ms":2417,"pages_per_sec":3389.0,
 "bytes_decoded":33546240,"bytes_on_wire":33546240,"max_rss_bytes":61200000,
 "cpu_user_ms":2900,"cpu_sys_ms":410,"valid":true,"validation_errors":[]}
```

8. The orchestrator cross-validates against server-side metrics (§8) and discards invalid trials; any invalid trial fails the suite loudly.

Runtime: both engines and baseline run under `tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all()` (identical; `--runtime-workers` recorded in metadata). The fixed worker count caps physical parallelism equally for both engines regardless of task structure (§12, A-3).

Statistics per row: median, IQR, min/max wall clock; median pages/s (= expected_pages / s); MiB/s for `payload`/`compressed`; millipede:spider throughput ratio; median peak RSS; median CPU (user+sys). Never publish a ratio without absolute numbers and raw samples.

## 7. Memory, CPU, and parse-parity accounting

**Peak RSS + CPU:** the **child crawler process itself** calls `getrusage(RUSAGE_SELF)` immediately before printing its result JSON — never the orchestrator, and never `RUSAGE_CHILDREN` (which aggregates all reaped children and cannot be attributed to one trial). `ru_maxrss` is normalized (macOS reports bytes, Linux KiB). `ru_utime`/`ru_stime` give CPU time from the same call. One 3-line `unsafe` libc call — legal here because the bench package is outside the workspace `unsafe_code = "deny"` lint wall. A tracking allocator is rejected (measures heap, not RSS; perturbs allocation behavior). Parent-side sysinfo sampling is not used in v1 (redundant with child self-report; may be added later as a diagnostic cross-check).

**RSS disclosure:** spider's subscription channel (capacity = expected_pages + 64) can retain undrained `Page`s if the consumer momentarily lags; that memory is part of spider's documented processing model and is included in its RSS, with a note in the report. Millipede's handler-in-slot model holds at most C pages. This is an architectural difference being measured, not an artifact to correct.

**Parse parity (per-stage accounting):**
- Raw rows (1–7): each engine parses each page **exactly once with its own native parser** as the cost of link discovery — millipede via `HtmlKind` (scraper), spider via its internal lol_html extractor. The accounting work (count/bytes/checksum) is identical and trivial on both sides; **no re-parse anywhere**. Parser choice is part of the framework comparison, deliberately.
- Extraction rows (8–9): identical `scraper` selector functions (shared module) run in millipede's handler (which **reuses** the already-parsed DOM) and in spider's subscriber (which must **re-parse** bytes with scraper, because spider does not expose its lol_html DOM). Spider therefore parses twice on these rows; the report states this explicitly as a consequence of the two architectures, and the published CPU-time column makes the extra work visible rather than hidden inside wall-clock.
- Concurrency accounting: C bounds fetch concurrency on both engines; millipede's parse+handler work runs inside its C permits, spider's subscriber work runs on one extra consumer task (its documented model). The single drain task (no per-page task spawning) plus the shared 4-worker runtime bound spider's effective extra parallelism; the report documents the residual asymmetry instead of pretending the same integer C means identical accounting.

An optional, clearly-labelled sensitivity row `M-http-raw` (millipede `HttpKind` + the example's string-scan extractor) may be produced with `--sensitivity` to show the cost of real parsing; it is never a headline row.

## 8. Fairness and validation gates

Enforced in code; a trial is valid only if **all** pass, and the suite fails if any engine cannot produce 5 valid trials:

1. Server-side unique-path hit set == expected page set (authoritative count, engine-independent).
2. Server-side duplicate-fetch count == 0 (each page fetched exactly once; `redirects` additionally requires exactly one `/r/{i}` hit per page).
3. `/robots.txt` hits == 0; off-host (`Host: localhost`) hits == 0.
4. Engine-side count == expected (millipede `stats.requests_finished`, `requests_failed == 0`; spider drain count, no `Lagged`).
5. Total decoded bytes == expected; running checksum == precomputed (bodies cannot be skipped or optimized away).
6. Extraction rows: record count + commutative digest (sum/XOR of per-record hashes — order-independent) == precomputed.
7. `compressed`: both engines' decoded checksums match; per-engine `Accept-Encoding` behavior and bytes-on-wire are recorded and reported (a negotiation difference is a reportable finding, not an error).

Controls: same fixed UA, same 15 s timeout, same redirect limit 7, retries/rotations zero, sessions/header-generation/coalescing/proxies/anti-bot off (millipede), delays zero both sides, robots off both sides plus allow-all robots.txt served, system allocator both sides (no jemalloc/mimalloc features), HTTP-only loopback, no logging in the measured path. TCP connection counts are recorded server-side and reported; pooling differences (reqwest 0.12 vs spider's reqwest 0.13) are a legitimate part of the comparison, not normalized away. Machine must be idle and on AC power; metadata records OS, kernel, arch, CPU model, RAM, rustc, git commit, spider version + features, runtime workers, C, and profile. Deltas < 5 % are treated as noise in the report narrative. Never compare RSS across OSes.

## 9. Harness layout

Standalone package, **not a workspace member** (own `[workspace]` table); root `Cargo.toml` additionally gains `exclude = ["benchmarks"]` for explicitness. Root CI (`--workspace` build/test/clippy/deny) never compiles spider.

```
benchmarks/spider-bench/
├── Cargo.toml            # pins spider =2.52.9; own [workspace]
├── Cargo.lock            # committed
├── deny.toml             # root policy copied; bench-local allowlist additions documented inline
├── README.md             # how to run; claims-scope statement
└── src/
    ├── main.rs           # clap CLI: orchestrate | run | report
    ├── scenario.rs       # ScenarioSpec: site map, latency, redirects, gzip, expected values, per-page work
    ├── server.rs         # axum server + instrumentation (single code path for all engines)
    ├── measure.rs        # ready/go protocol, Instant, getrusage(RUSAGE_SELF), JSON line
    ├── report.rs         # samples.jsonl -> summary.md + metadata.json
    ├── engines/
    │   ├── mod.rs
    │   ├── millipede.rs  # generic driver, config as §5
    │   ├── spider.rs     # generic driver, config as §5
    │   └── baseline.rs   # reqwest + FuturesUnordered ceiling
    └── scenarios/
        ├── mod.rs        # registry (fixed at scaffold time)
        ├── tree.rs  wide.rs  mesh.rs  latency.rs  payload.rs
        ├── redirects.rs  compressed.rs  books.rs  hn.rs
```

`benchmarks/spider-bench/Cargo.toml`:

```toml
[package]
name = "spider-bench"
version = "0.0.0"
edition = "2024"
publish = false
# no rust-version: built with the repo-pinned toolchain (rust-toolchain.toml, currently 1.96)

[workspace]          # standalone; keeps spider out of the root workspace graph

[dependencies]
millipede = { path = "../../millipede", default-features = false, features = ["http", "html", "storage-memory"] }
spider = { version = "=2.52.9", default-features = false, features = ["sync"] }
tokio = { version = "1", features = ["full"] }
axum = { version = "0.8", default-features = false, features = ["http1", "tokio"] }
bytes = "1"
clap = { version = "4", features = ["derive"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
scraper = { version = "0.24", features = ["atomic"] }
reqwest = { version = "0.12", default-features = false, features = ["gzip", "brotli", "deflate"] }
flate2 = "1"
anyhow = "1"
libc = "0.2"
url = "2"
seahash = "4"        # cheap deterministic checksums/digests

[features]
spider-upstream-defaults = ["spider/default"]

[profile.release]
lto = "thin"
codegen-units = 1
```

Output layout: `benchmarks/spider-bench/target/results/<timestamp>-{metadata.json,samples.jsonl,summary.md}`. Summary columns: scenario · C · engine · pages · median wall · IQR · pages/s · MiB/s · vs-spider ratio · peak RSS · CPU (u+s) · conns · validation.

## 10. Browser suite (optional, deferred)

`smart_crawl` shape (static + JS-shell pages) vs spider `crawl_smart()` (`smart` feature) is implemented only after the HTTP suite is stable, behind `--include-browser`, reported in a separate table. Requirements: pinned Chrome build/executable path, prelaunched comparable browser processes, launch time excluded, concurrency ≤ 4, ≥ 10 samples. Never mixed with HTTP rows.

## 11. Commands and CI

Root `.cargo/config.toml`:

```toml
[alias]
bench-spider-check = "check --manifest-path benchmarks/spider-bench/Cargo.toml"
bench-spider = "run --release --manifest-path benchmarks/spider-bench/Cargo.toml --"
```

```sh
cargo bench-spider-check
cargo bench-spider orchestrate --iters 5 --concurrency 32          # full suite
cargo bench-spider orchestrate --scenario tree --quick             # 1 trial, validation only, not publishable
cargo bench-spider report target/results/<ts>-samples.jsonl
```

- Never on push/PR CI. Hosted shared runners must never be the source of performance claims.
- **Bit-rot + compliance guard:** `.github/workflows/bench-compare.yml`, `workflow_dispatch` (optionally monthly `schedule:`): (a) `cargo check` on the bench manifest, (b) `cargo deny check licenses advisories bans sources --manifest-path benchmarks/spider-bench/Cargo.toml` using the bench-local `deny.toml`, (c) smoke run `--scenario tree --depth 9 --quick` (511 pages, 1 iter), artifact-uploads the report. This closes the "excluded workspace escapes cargo-deny" gap.
- Published numbers must name machine, OS, versions, feature sets, and the exact command used.

## 12. Review resolutions

Findings from the independent reviews of the two draft plans, and how this merged plan resolves each:

| ID | Finding | Resolution |
|---|---|---|
| A-CRIT-1 | Spider's bounded broadcast can silently drop pages; detection-after-the-fact isn't enough; big capacities distort RSS | **Prevention, not detection:** capacity = expected_pages + 64 ≥ total messages ever sent, so the channel mathematically cannot drop even with a fully stalled consumer. `Lagged` handling remains as defense in depth and invalidates the trial. Counts are validated against the server-side hit set (authoritative), not `get_links()`. The channel's worst-case retention is disclosed in the RSS notes as part of spider's subscription architecture (§5, §7). |
| A-2 | Client/transport construction not symmetrically timed (spider builds its client inside `crawl()`, millipede before) | Ready/go handshake: the child signals `ready` **before any HTTP client exists on either side**; the timer starts at `go` and covers engine construction + crawl + drain for both engines. Both are charged for client/pool construction inside the timed region (§6, step 5–6). |
| A-3 | Same integer C ≠ same unit of work (spider's subscriber runs outside its permit pool) | Cannot be perfectly equalized without distorting one engine's architecture. Mitigations: identical fixed 4-worker runtime caps physical parallelism equally; single drain task (parse parallelism 1, no per-page spawning); CPU-time (user+sys) published per row so total work is visible independently of where it runs; the asymmetry is documented in the report instead of hidden (§6, §7). |
| A-4 | Raw scenarios' parse-path asymmetry unspecified (re-parse or not on spider's side) | Nailed down: raw rows do **no re-parse** on either side — each engine pays exactly its own native link-discovery parse (scraper vs lol_html) plus identical trivial accounting. Extraction rows use identical scraper code; spider's necessary second parse is explicitly documented and visible in the CPU column (§7). |
| A-5 | Baseline ceiling may hit a leaner server path than the crawlers | Requirement stated and enforced: one server code path; the server cannot distinguish engines; baseline fetches the identical URL list through the identical instrumented handlers. Ceiling threshold 70 % (§5 Baseline, §8). |
| A-6 | getrusage ambiguity (which process calls it; RUSAGE_CHILDREN hazard) | Specified: the child calls `getrusage(RUSAGE_SELF)` itself immediately before emitting its JSON; the orchestrator never calls `RUSAGE_CHILDREN`; macOS-bytes/Linux-KiB normalized (§7). |
| A-7 | Matrix omits redirects, compression, error paths, TLS, DNS, cookies, large pages | Added `redirects` (301 hop per page) and `compressed` (real gzip negotiation + decode) as headline rows; `payload` covers very large pages. Error paths are deliberately excluded (retry semantics differ — policy, not performance; §3) and TLS/DNS/cookies are declared out of scope with an explicit claims-scope statement in the report (§1). Claims are narrowed to what is measured. |
| A-8 | cargo-deny/MSRV for spider's tree asserted, not verified; excluded workspace escapes root deny job | spider 2.52.9 license verified MIT via crates.io API (passes the allowlist); transitive compliance enforced by bench-local `deny.toml` + committed `Cargo.lock` + the manual workflow's `cargo deny check` step (§11). MSRV question dissolved by building with the repo-pinned 1.96 toolchain (see B-1). |
| B-1 | `cargo +1.85 check` gate fails (icu/idna transitive deps need 1.86) and "pin older spider" can't fix it | Gate **dropped**. The bench package is not a workspace member; the dependency direction is bench → millipede, so nothing here can move millipede's published 1.85 MSRV. The bench builds with the repo's pinned toolchain (1.96 per `rust-toolchain.toml`), which is what any `cargo bench-spider` invocation uses anyway. The finding's note is accepted: the icu/idna constraint comes via url→idna shared by all spider versions, so downgrading spider was never a remedy (§2, §9). |
| B-2 | Raw rows gave millipede a string-scan extractor while spider runs a real parser | All headline millipede rows use `HtmlKind` (real scraper parse, millipede's actual equivalent of spider's native link discovery). The string-scan variant survives only as the clearly-labelled, off-by-default `M-http-raw` sensitivity row (§7). |
| B-3 | Subscriber protocol (small capacity + bounded parse tasks + "lag invalidates") is self-contradictory under load | Resolved by the same decision as A-CRIT-1: capacity sized to the full message count (drop-free by construction), one drain task, lag still invalidates as defense in depth, channel retention disclosed in RSS notes, and the fetch/parse parallelism asymmetry documented in the report (§5, §7). |

## 13. Implementation order

1. **Scaffold** (single owner of all shared files): package + workspace exclude + CLI + `scenario.rs` + instrumented server + measurement/handshake + report + engine drivers + compiling scenario stubs + aliases + workflow. `cargo bench-spider-check` and `cargo deny check` must pass; spider API signatures re-verified against the pinned build here.
2. **Scenario tasks (parallel, disjoint files):** tree+wide · mesh+latency · payload+redirects+compressed · books · hn. Each implements only its `scenarios/*.rs` files against the scaffold's `ScenarioSpec` and must pass `--quick` validation for all three engines.
3. Report polish, full-suite run on an idle machine, README with claims-scope statement.
4. Optional: browser suite (§10), `spider-upstream-defaults` sensitivity runs on Linux.
