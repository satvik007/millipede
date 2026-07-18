# Getting started

Millipede requires Rust 1.85 or newer. Add the umbrella crate with Cargo:

```console
cargo add millipede
```

The actual default feature set is `http`, `html`, and `storage-memory`. This makes HTTP crawling,
parsed HTML crawling, and `MemoryStorageClient` available without extra flags. `storage-fs`,
`browser`, `browser-chromiumoxide`, and `fingerprint` remain opt-in.

## Crawl anatomy

`CrawlerKind` defines one crawler flavor's request lifecycle and typed context. The shared
`Crawler<K>` engine schedules requests for a concrete kind `K`. `CrawlerBuilder<K>` combines that
kind with engine-wide configuration, including storage, concurrency, retry limits, a
`request_handler`, and an optional `failed_request_handler`.

The normal construction path is:

```text
kind builder -> Crawler::builder(kind) -> request_handler -> build().await -> run().await
```

`run()` accepts start URLs or built `Request` values and resolves to `FinalStatistics`. Common
fields include `requests_finished`, `requests_failed`, and `requests_retries`.

## First HTTP crawler

This compact version follows `millipede/examples/http_crawl.rs`. The full example uses a local
mock site, extracts links from response text, and relies on queue deduplication as pages fan out.

```rust
use std::sync::Arc;

use millipede::{Crawler, HttpContext, HttpKind, MemoryStorageClient};

let kind = HttpKind::builder().build()?;
let crawler: millipede::HttpCrawler = Crawler::builder(kind)
    .max_concurrency(8)
    .storage_client(Arc::new(MemoryStorageClient::new()))
    .request_handler(|ctx: HttpContext| async move {
        println!("{}: {}", ctx.response.status, ctx.request.url);
        Ok(())
    })
    .build()
    .await?;

let stats = crawler.run("https://example.com/").await?;
println!("finished={}", stats.requests_finished);
```

`HttpContext` provides the current `request`, the `response`, optional `session` and `proxy_info`,
an `enqueue` linker, a `storage` handle, and the crawler handle.

## First HTML crawler

`HtmlKind` adds a synchronized parsed document to `HtmlContext`. The following is based on
`millipede/examples/scrape_books.rs`: it queries `ctx.html`, stores a value, and asks `ctx.enqueue`
to discover same-host pages under one long-lived `CrawlPolicy`.

```rust
use std::sync::Arc;

use millipede::{
    CrawlPolicy, Crawler, DatasetExt, EnqueueStrategy, HtmlContext, HtmlKind,
    MemoryStorageClient,
};

let crawler: millipede::HtmlCrawler = Crawler::builder(HtmlKind::new()?)
    .storage_client(Arc::new(MemoryStorageClient::new()))
    .crawl_policy(
        CrawlPolicy::new()
            .strategy(EnqueueStrategy::SameHostname)
            .max_requests_per_crawl(200),
    )
    .request_handler(|ctx: HtmlContext| async move {
        let selector = millipede::html::scraper::Selector::parse("title")
            .expect("static selector is valid");
        let title = ctx.html
            .select_first(&selector, |el| el.text().collect::<String>())
            .unwrap_or_default();
        ctx.storage.dataset().push(&serde_json::json!({ "title": title })).await?;
        let _ = ctx.enqueue.same_hostname().await?;
        Ok(())
    })
    .build()
    .await?;
```

For labeled page types, use `Router<HtmlContext>` as the request handler. The books example routes
detail pages separately and uses `ctx.enqueue.options().selector(...).label(...).send()`.

## Browser crawler

Enable the Chromiumoxide integration:

```console
cargo add millipede --features browser-chromiumoxide
```

`ChromiumoxideProvider` needs a local Chromium or Chrome installation. `find_browser()` searches
for one, and the `MILLIPEDE_CHROME` environment variable can point to a browser binary. A browser
kind is constructed with `BrowserKind::builder(ChromiumoxideProvider)` and can receive
`ChromiumLaunchOptions::default().with_executable(...)`.

The complete [browser crawler example](../../millipede/examples/browser_crawl.rs) launches a local
site, evaluates `document.title`, writes dataset rows, and calls `ctx.enqueue.same_hostname()`.
Run it with:

```console
cargo run -p millipede --features browser-chromiumoxide,storage-memory --example browser_crawl
```

## Smart crawler

`SmartKind<ChromiumoxideProvider>` follows an HTTP-first strategy and promotes pages selected by a
promotion detector to Chromium. `SmartContext` distinguishes the HTTP and browser paths. See the
[smart crawler example](../../millipede/examples/smart_crawl.rs), which keeps server-rendered pages
on HTTP and promotes a JavaScript shell.

## Where does data go?

The `storage-memory` feature is enabled by default, but `CrawlerBuilder` does not silently create a
storage client. Supply `MemoryStorageClient::new()` with `storage_client(...)`, or put a storage
client in `Configuration`. Building without either returns `CrawlerBuildError::MissingStorage`.

Once configured, `build()` purges storage when configured, uses a supplied request queue or opens
the default one, and opens the default key-value store. The HTTP kind opens the default dataset
later, when `run()` starts the kind, so a dataset-open failure surfaces from `run()` rather than
`build()`. The default `Configuration` has `purge_on_start()` enabled. This is harmless for a fresh
memory client but important for persistent storage; use
`Configuration::builder().purge_on_start(false).build()?` when resuming.

Memory storage is process-local and disappears when the process exits. Choose the `storage-fs`
feature and `FsStorageClient` for Crawlee-compatible disk persistence. See
[Requests and storage](./request-storage.md) before pointing a crawler at existing data.

## All umbrella-crate examples

These are the 14 source files currently under `millipede/examples/`:

| Example | Focus |
|---|---|
| [`phase0_hello.rs`](../../millipede/examples/phase0_hello.rs) | Minimal early smoke example. |
| [`phase1_queue_demo.rs`](../../millipede/examples/phase1_queue_demo.rs) | Memory request-queue operations. |
| [`basic_engine.rs`](../../millipede/examples/basic_engine.rs) | Generic engine, retries, results, and statistics. |
| [`http_crawl.rs`](../../millipede/examples/http_crawl.rs) | HTTP crawling, enqueueing, and deduplication. |
| [`autoscale_demo.rs`](../../millipede/examples/autoscale_demo.rs) | AIMD convergence and snapshots. |
| [`scrape_books.rs`](../../millipede/examples/scrape_books.rs) | Routed HTML scraping and file-system datasets. |
| [`browser_crawl.rs`](../../millipede/examples/browser_crawl.rs) | Chromium navigation and page evaluation. |
| [`smart_crawl.rs`](../../millipede/examples/smart_crawl.rs) | HTTP-first crawling with browser promotion. |
| [`fingerprint_crawl.rs`](../../millipede/examples/fingerprint_crawl.rs) | Headers, anti-bot detection, and error snapshots. |
| [`basic.rs`](../../millipede/examples/basic.rs) | Basic reference crawl. |
| [`error_handling.rs`](../../millipede/examples/error_handling.rs) | Typed crawler error handling. |
| [`rate_limit.rs`](../../millipede/examples/rate_limit.rs) | Rate limits and crawl pacing. |
| [`proxy_switcher.rs`](../../millipede/examples/proxy_switcher.rs) | Proxy rotation. |
| [`hackernews.rs`](../../millipede/examples/hackernews.rs) | Multi-route real-world HTML crawl. |

Use the `required-features` entries in `millipede/Cargo.toml` when an example needs more than the
default set. Continue with [Requests and storage](./request-storage.md) for the state model behind
every crawler.
