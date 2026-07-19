# millipede

An idiomatic Rust web-crawling library inspired by Crawlee.

[![crates.io](https://img.shields.io/crates/v/millipede.svg)](https://crates.io/crates/millipede)
[![docs.rs](https://docs.rs/millipede/badge.svg)](https://docs.rs/millipede)
[![CI](https://github.com/satvik007/millipede/actions/workflows/ci.yml/badge.svg)](https://github.com/satvik007/millipede/actions/workflows/ci.yml)
[![MSRV 1.85](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://www.rust-lang.org)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/satvik007/millipede#license)

Millipede is a 0.1.0 release candidate and is **not yet published to crates.io**. While the project follows SemVer, `0.x.0` releases may break the public API; `0.x.y` releases must remain compatible.

## Quick start

```rust
use std::sync::Arc;

use millipede::{CrawlPolicy, Crawler, DatasetExt, HtmlContext, HtmlCrawler, HtmlKind};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crawler: HtmlCrawler = Crawler::builder(HtmlKind::new()?)
        .storage_client(Arc::new(millipede::MemoryStorageClient::new()))
        .crawl_policy(CrawlPolicy::new().max_requests_per_crawl(100))
        .request_handler(|ctx: HtmlContext| async move {
            ctx.storage
                .dataset()
                .push(&serde_json::json!({
                    "url": ctx.request.url.as_str(),
                    "status": ctx.response.status.as_u16(),
                }))
                .await?;
            let _ = ctx.enqueue.same_hostname().await?;
            Ok(())
        })
        .build()
        .await?;

    let stats = crawler.run("https://example.com/").await?;
    println!("finished: {}", stats.requests_finished);
    Ok(())
}
```

This example is doc-tested from the `millipede` crate README.

## Workspace layout

| Crate | Description |
|---|---|
| `millipede` | An idiomatic Rust web-crawling library inspired by Crawlee. Umbrella crate re-exporting the Millipede workspace. |
| `millipede-core` | Core primitives for the Millipede web crawler: request model, storage traits, events, errors, configuration. |
| `millipede-storage-memory` | In-memory StorageClient implementation for the Millipede web crawler. |
| `millipede-storage-fs` | File-system StorageClient implementation for the Millipede web crawler. |
| `millipede-http` | HttpCrawler for the Millipede web crawler: reqwest-based HTTP fetching. |
| `millipede-html` | HtmlCrawler for the Millipede web crawler: HTML parsing via scraper. |
| `millipede-browser` | BrowserCrawler core, BrowserProvider trait, and BrowserPool for the Millipede web crawler. |
| `millipede-browser-chromiumoxide` | Chromium CDP browser provider for the Millipede web crawler, via chromiumoxide. |
| `millipede-fingerprint` | Browser-like header generation and fingerprint hooks for the Millipede web crawler. |

`millipede-cli` is deferred until post-1.0; the pre-0.2 scaffolding plan uses a `cargo-generate` template instead.

## Examples

| Example | Description | Run command |
|---|---|---|
| `phase0_hello` | Confirms the umbrella crate and its prelude resolve. | `cargo run -p millipede --features http,html,storage-memory --example phase0_hello` |
| `phase1_queue_demo` | Demonstrates request deduplication and lease-based queue workers. | `cargo run -p millipede --features storage-memory --example phase1_queue_demo` |
| `basic_engine` | Exercises fixed-concurrency crawling, retries, failures, and result snapshots over synthetic requests. | `cargo run -p millipede --features storage-memory --example basic_engine` |
| `http_crawl` | Crawls a 100-page local mock site with HTTP fetching and URL enqueueing. | `cargo run -p millipede --features http,storage-memory --example http_crawl` |
| `autoscale_demo` | Demonstrates AIMD concurrency convergence against transient failures on a local mock server. | `cargo run -p millipede --features http,storage-memory --example autoscale_demo` |
| `scrape_books` | Crawls category and detail pages from Books to Scrape into a file-system dataset. | `cargo run -p millipede --features http,html,storage-fs --example scrape_books` |
| `browser_crawl` | Crawls a local mock catalog with headless Chromium and stores page titles. | `cargo run -p millipede --features browser-chromiumoxide,storage-memory --example browser_crawl` |
| `smart_crawl` | Uses HTTP first and promotes a JavaScript shell to headless Chromium. | `cargo run -p millipede --features browser-chromiumoxide,html,storage-memory --example smart_crawl` |
| `fingerprint_crawl` | Demonstrates deterministic browser-like headers, anti-bot detection, and error snapshots offline. | `cargo run -p millipede --features http,fingerprint,storage-memory --example fingerprint_crawl` |
| `basic` | No-op stub reserved for a future basic link-scraping example. | `cargo run -p millipede --features http,html,storage-memory --example basic` |
| `error_handling` | No-op stub reserved for a future typed-error-handling example. | `cargo run -p millipede --features http,storage-memory --example error_handling` |
| `rate_limit` | No-op stub reserved for a future rate-limiting example. | `cargo run -p millipede --features http,storage-memory --example rate_limit` |
| `proxy_switcher` | No-op stub reserved for a future proxy-rotation example. | `cargo run -p millipede --features http,storage-memory --example proxy_switcher` |
| `hackernews` | No-op stub reserved for a future Hacker News scraper; it currently makes no requests. | `cargo run -p millipede --features http,html,storage-memory --example hackernews` |

The browser examples require Chrome or Chromium. Set `MILLIPEDE_CHROME` to override the discovered browser executable. `scrape_books` makes requests to a live external site.

## Documentation

- [Benchmark overview](https://github.com/satvik007/millipede/blob/main/docs/benchmarks.md)
- [Millipede vs Spider vs Colly vs Crawlee](https://github.com/satvik007/millipede/blob/main/docs/benchmarks-vs-crawlers.md)
- [Autoscaler](https://github.com/satvik007/millipede/blob/main/docs/guide/autoscaler.md)
- [Fingerprinting](https://github.com/satvik007/millipede/blob/main/docs/guide/fingerprinting.md)
- [Crawlee storage migration](https://github.com/satvik007/millipede/blob/main/docs/guide/crawlee-storage-migration.md)
- [Extras policy](https://github.com/satvik007/millipede/blob/main/docs/guide/extras.md)

The `migrating-from-crawlee` guide is not present in this checkout, so it is not linked here.

## Development

Every change must pass:

```console
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps
cargo +1.85 check --workspace --all-targets
```

## License

Licensed under either the [MIT License](LICENSE-MIT) or the [Apache License, Version 2.0](LICENSE-APACHE), at your option.
