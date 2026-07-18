# Migrating from Crawlee

Millipede keeps Crawlee's crawler, router, queue, dataset, session, and proxy concepts, but
expresses them through Rust's type system. This guide assumes you know Crawlee's JavaScript or
TypeScript API and want the closest Millipede 0.1 equivalent.

## 1. Concept map

| Crawlee | Millipede |
|---|---|
| `CheerioCrawler` | `HtmlCrawler` |
| `PlaywrightCrawler` / `PuppeteerCrawler` | `BrowserCrawler` with the chromiumoxide provider; there is no Playwright provider yet ([tracked issue](../tracked-issues.md#tracked-9-add-webdriver-bidi-and-playwright-style-browser-providers)) |
| `BasicCrawler` | `BasicCrawler` for no-fetch work, or `HttpCrawler` for HTTP fetching |
| `router.addHandler(label, handler)` | `Router::<C>::new().route(label, handler)` |
| `enqueueLinks` | `ctx.enqueue` plus `EnqueueLinksOptions` |
| `Dataset.pushData` | `DatasetExt::push` for typed values, or `Dataset::push_json` for a JSON value |
| `KeyValueStore.getValue` / `setValue` | `KeyValueStore` plus typed `KeyValueStoreExt::get` / `KeyValueStoreExt::set` |
| `useState` | `AutoSaved<T>` |
| `ProxyConfiguration` | `ProxyConfiguration` with `round_robin`, `rotating`, or `tiered` |
| `SessionPool` | `SessionPool` plus `SessionPoolOptions` |
| `log` | `tracing` macros and subscribers |

For example, dataset writes remain one operation, but Rust makes the serializable item type
explicit.

```js
await Dataset.pushData({ url: request.url, title });
```

```rust
use millipede::DatasetExt;

ctx.storage
    .dataset()
    .push(&serde_json::json!({ "url": ctx.request.url, "title": title }))
    .await?;
```

## 2. Typed errors instead of string dispatch

A Crawlee failure handler commonly receives an `Error` and reconstructs intent from its message or
an attached status.

```js
failedRequestHandler: async ({ request }, error) => {
    if (error.message.includes('blocked') || error.statusCode === 403) {
        log.warning(`Blocked: ${request.url}`);
    } else {
        log.exception(error, `Failed: ${request.url}`);
    }
}
```

Millipede's `failed_request_handler` receives a `FailedRequestContext`. Its `error` is an
`Arc<CrawlError>`, so classification is an exhaustive-looking typed match (the enum itself is
non-exhaustive for future additions). The handler only receives terminal, non-critical errors:
ordinary retries arrive after their request budget is exhausted, session-rotating errors arrive
after their rotation budget is exhausted, and permanent errors arrive immediately. With the
default dispatcher shown here, `ForceRetry` is always reclaimed regardless of
`max_request_retries`, while `Critical` aborts the crawler before failure-handler dispatch. Their
arms below document those invariants and keep the complete current variant list visible.

```rust
use millipede::{AntiBotTech, CrawlError, FailedRequestContext};

.failed_request_handler(|ctx: FailedRequestContext| async move {
    match ctx.error.as_ref() {
        CrawlError::Retry(source) => tracing::warn!(%source, "retry budget exhausted"),
        CrawlError::Session(source) => tracing::warn!(%source, "session rotations exhausted"),
        CrawlError::ForceRetry(source) => {
            tracing::error!(%source, "unreachable: default dispatch always reclaims ForceRetry")
        }
        CrawlError::NonRetryable(source) => tracing::error!(%source, "permanent request error"),
        CrawlError::Critical(source) => {
            tracing::error!(%source, "unreachable: Critical aborts before failure dispatch")
        }
        CrawlError::MissingRoute { label, method } => {
            tracing::error!(?label, %method, "no matching route")
        }
        CrawlError::AntiBotDetected { tech: AntiBotTech::Cloudflare, .. } => {
            tracing::warn!("Cloudflare response detected")
        }
        CrawlError::AntiBotDetected { tech, .. } => tracing::warn!(?tech, "anti-bot response"),
        _ => tracing::error!(error = %ctx.error, "unrecognized crawl error"),
    }
    Ok(())
})
```

Handlers create the five intent-bearing wrapper variants through constructor helpers rather than
encoding intent in text:

```rust
let transient = CrawlError::retry(anyhow::anyhow!("upstream reset"));
let blocked_session = CrawlError::session(anyhow::anyhow!("rotate identity"));
let must_retry = CrawlError::force_retry(anyhow::anyhow!("retry regardless of budget"));
let bad_input = CrawlError::non_retryable(anyhow::anyhow!("invalid input"));
let abort = CrawlError::critical(anyhow::anyhow!("invariant violated"));
```

`max_request_retries` sets the crawler's ordinary request retry budget. HTTP, HTML, and browser
kind builders expose `retry_status_codes` to classify selected response codes as retryable.
`CrawlError::ignores_max_retries()` is true for `ForceRetry`, while `Session` and
`AntiBotDetected` rotate a session. Consequently, the default dispatcher can deliver `Retry` only
after its request retry budget, `Session` or `AntiBotDetected` only after the session-rotation
budget, and `NonRetryable` or `MissingRoute` immediately. It does not deliver `ForceRetry` or
`Critical` to `failed_request_handler`. Typed matching survives message rewrites and preserves
the difference between a block, a retryable transport failure, a permanent input error, and a
crawler abort; `err.message.includes('blocked')` does not.

## 3. Router labels and methods

Crawlee registers a label handler and a fallback on a mutable router.

```js
router.addHandler('detail', async ({ request }) => {
    // Handle detail request.
});
router.addDefaultHandler(async ({ request }) => {
    // Handle unlabeled or unmatched request.
});
```

Millipede builds a `Router<C>` by value. `route` matches a label with any HTTP method;
`route_method` and `route_methods` additionally constrain the method. Register a method-specific
route before a same-label catch-all because the first matching route wins. The retained fallback
API is `default(handler)`, and `middleware()` appends middleware that runs before the selected
handler.

```rust
use millipede::{HtmlContext, Method, Router};

let router = Router::<HtmlContext>::new()
    .route_method("detail", Method::GET, |ctx: HtmlContext| async move {
        tracing::info!(url = %ctx.request.url, "GET detail");
        Ok(())
    })
    .route_methods("detail", [Method::POST, Method::PUT], |ctx: HtmlContext| async move {
        tracing::info!(url = %ctx.request.url, "write detail");
        Ok(())
    })
    .route("detail", |ctx: HtmlContext| async move {
        tracing::info!(url = %ctx.request.url, "other detail method");
        Ok(())
    })
    .middleware(|ctx: HtmlContext| async move {
        tracing::debug!(url = %ctx.request.url, "routing request");
        Ok(ctx)
    })
    .default(|ctx: HtmlContext| async move {
        tracing::debug!(url = %ctx.request.url, "fallback route");
        Ok(())
    });
```

Without a matching route or fallback, routing returns the typed
`CrawlError::MissingRoute { label, method }`; it is not a JavaScript-style thrown exception.
There is no `this` binding: handlers are ordinary async closures over a typed context.

Set the child's label when enqueueing it. Labels are not inherited from the parent.

```js
await enqueueLinks({ selector: 'a.product', label: 'detail' });
```

```rust
ctx.enqueue
    .options()
    .selector("a.product")
    .label("detail")
    .send()
    .await?;
```

## 4. `enqueueLinks` becomes a typed pipeline

Crawlee puts extraction, matching, request transformation, and queue placement in one options
object.

```js
await enqueueLinks({
    selector: 'a.product',
    globs: ['**/products/**'],
    label: 'detail',
    strategy: EnqueueStrategy.SameDomain,
    transformRequestFunction: (request) => ({ ...request, userData: { source: 'listing' } }),
});
```

Millipede exposes the same stages as an `EnqueueLinksOptions` builder, terminated by the async
`send` call.

```rust
use millipede::{EnqueueStrategy, TransformResult, UserData};

let mut user_data = UserData::default();
user_data.set_typed("source", &"listing")?;

let result = ctx.enqueue
    .options()
    .selector("a.product")
    .globs(["**/products/**"])
    .label("detail")
    .user_data(user_data)
    .strategy(EnqueueStrategy::SameDomain)
    .transform(|request| Box::pin(async move {
        request.headers.insert("x-discovered-by", "listing".parse().unwrap());
        TransformResult::Enqueue
    }))
    .forefront(false)
    .limit(100)
    .send()
    .await?;
```

The complete per-call setter surface is `urls`, `raw_urls`, `base_url`, `label`, `user_data`,
`selector`, `strategy`, `globs`, `regex`, `exclude`, `transform`, `limit`, and `forefront`. Use
`raw_urls` plus `base_url` for relative strings; `urls` accepts already-parsed absolute `Url`
values. `EnqueueStrategy` has `All`, `SameHostname`, `SameDomain`, and `SameOrigin`. The strategy
filters extracted and raw links. Verified implementation detail: absolute values supplied through
`urls` bypass the relationship strategy filter, although the remaining filters and crawl limits
still apply.

Where Crawlee can silently omit filtered candidates, Millipede's `send` returns an `EnqueueResult`
with `added` requests and `skipped` `SkippedUrl` values. Each reported skip carries a `SkipReason`,
including depth or crawl-count limits, strategy/glob/regex exclusion, transform rejection,
duplicate `uniqueKey`, and invalid URLs. These reported filtering, admission, and queue-duplicate
decisions are observable per call, and a `CrawlPolicy::on_skipped` callback can observe them across
calls. Two operations are silent in the current implementation: candidate-level URL deduplication
and `limit` truncation omit candidates without adding a `SkippedUrl` or invoking the callback.

`CrawlPolicy` applies the configured strategy and enforces `max_crawl_depth` and
`max_requests_per_crawl` while candidates are admitted, before they enter the queue. Robots-file
enforcement is not implemented in Millipede as of Phase 8: the core source and post-audit baseline
explicitly defer robots policy. There is no robots check at enqueue time, so Millipede does not yet
satisfy Crawlee's `respectRobotsTxtFile` behavior and that setting has no direct mapping.

```rust
use millipede::{CrawlPolicy, Crawler, EnqueueStrategy};

let policy = CrawlPolicy::new()
    .strategy(EnqueueStrategy::SameHostname)
    .max_crawl_depth(4)
    .max_requests_per_crawl(10_000)
    .on_skipped(|url: &str, reason: &millipede::SkipReason| {
        tracing::debug!(%url, ?reason, "enqueue candidate skipped");
    });

let crawler = Crawler::builder(kind)
    .crawl_policy(policy)
    .request_handler(router)
    .build()
    .await?;
```

## 5. `useState` becomes `AutoSaved<T>`

Crawlee returns a mutable persisted object from `useState`.

```js
const state = await useState('STATE', { pages: 0 });
state.pages += 1;
```

Millipede uses a standalone typed wrapper. The default KVS `Arc` comes from
`ctx.storage.key_value_store().clone()` on HTTP, HTML, and browser contexts.

```rust
use millipede::AutoSaved;
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize)]
struct State {
    pages: u64,
}

let state = AutoSaved::<State>::open(
    ctx.storage.key_value_store().clone(),
    "STATE",
    State { pages: 0 },
).await?;

let before = state.get().await;
state.update(|value| value.pages += 1).await;
state.set(State { pages: before.pages + 2 }).await;
state.persist().await?;
```

A KVS handle has no method named `auto_saved`; the standalone `AutoSaved::open` constructor is the
shipped API. `open` reads JSON from the key or uses the supplied default. `get` clones the
in-memory value. `set` and `update` mutate only that in-memory value. Persistence happens only when
your code awaits `persist`, which serializes the current value as JSON and writes it to the store;
the current engine does not automatically register an `AutoSaved<T>` instance with periodic
persist-state events.

## 6. File-system storage compatibility

`FsStorageClient::new("./storage")` can open a Crawlee project's storage directory. The evidence is
deliberately narrower than “complete wire compatibility”:

- `crates/millipede-storage-fs/tests/client.rs::opens_crawlee_shaped_storage` creates a
  Crawlee-shaped dataset item and default KVS `INPUT`, then reads both through `FsStorageClient`.
- `crates/millipede-storage-fs/tests/queue.rs::authentic_crawlee_fixture_opens_resumes_reads_and_dedupes`
  writes authentic Crawlee request envelopes without Millipede's `json` extension field, counts
  the pending and handled records, verifies the reconstructed pending request fields, resumes and
  handles that pending request, and deduplicates both requests by `uniqueKey` after reopening.

These tests support compatible dataset and key-value-store layouts and opening, resuming, reading,
and deduplicating Crawlee queue envelopes. They do not establish byte-for-byte archival
compatibility. Millipede-written request files add a `json` field for its full serialized request,
and unmapped Crawlee-private metadata need not round-trip. Concurrent writes to the same storage
directory from Crawlee and Millipede are unsupported; stop all writers before migrating.

**Data-loss warning: `purge_on_start=true` is the default. Pointing a crawler at existing storage
without setting `purge_on_start(false)` can delete managed datasets, queues, and KVS records. Back
up the directory first.**

Follow [Crawlee storage migration](crawlee-storage-migration.md) for the backup, configuration,
layout, and queue-resume procedure.

## 7. Not yet in Millipede

- Playwright and Puppeteer providers are not available. Browser crawling currently uses
  `BrowserCrawler` with the chromiumoxide provider; follow [the provider issue](../tracked-issues.md#tracked-9-add-webdriver-bidi-and-playwright-style-browser-providers).
- `infinite_scroll` and `save_snapshot` helpers are planned for the community extras layer rather
  than core. See the [extras policy](extras.md).
- Apify platform storage is not implemented. Follow [the storage issue](../tracked-issues.md#tracked-6-add-a-millipede-storage-apify-platform-client).

Continue with [Getting started](getting-started.md), then browse the complete
[`millipede/examples/` directory](../../millipede/examples/). These runnable examples are useful
porting references:

- [`millipede/examples/scrape_books.rs`](../../millipede/examples/scrape_books.rs)
- [`millipede/examples/error_handling.rs`](../../millipede/examples/error_handling.rs)
