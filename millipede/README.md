# millipede

An idiomatic Rust web-crawling library inspired by Crawlee.

[![crates.io](https://img.shields.io/crates/v/millipede.svg)](https://crates.io/crates/millipede)
[![docs.rs](https://docs.rs/millipede/badge.svg)](https://docs.rs/millipede)
[![CI](https://github.com/satvik007/millipede/actions/workflows/ci.yml/badge.svg)](https://github.com/satvik007/millipede/actions/workflows/ci.yml)
[![MSRV 1.85](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://www.rust-lang.org)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/satvik007/millipede#license)

## Quick start

```toml
[dependencies]
millipede = "0.1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde_json = "1"
```

```rust,no_run
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

## Feature flags

`http`, `html`, and `storage-memory` are enabled by default. The core API is always available.

| Feature | Enables | Default |
|---|---|:---:|
| `http` | Reqwest-based HTTP fetching and `HttpCrawler` | Yes |
| `html` | HTML parsing and `HtmlCrawler` | Yes |
| `storage-memory` | In-memory datasets, key-value stores, and request queues | Yes |
| `storage-fs` | File-system-backed storage | No |
| `browser` | Browser crawler abstractions and smart crawling | No |
| `browser-chromiumoxide` | The chromiumoxide CDP provider; also enables `browser` | No |
| `fingerprint` | Browser-like header generation and fingerprint hooks | No |

## Crawler kinds

`HttpCrawler` fetches URLs over HTTP and exposes response data, sessions, proxies, and URL enqueueing to handlers.

`HtmlCrawler` adds synchronized HTML parsing and DOM-based link extraction to HTTP crawling.

`BrowserCrawler` drives pages through a browser provider for JavaScript-rendered sites.

`SmartCrawler` starts on the faster HTTP path and promotes requests to a browser when its promotion detector identifies a JavaScript shell or another browser-only response.

## Links

- [Autoscaler guide](https://github.com/satvik007/millipede/blob/main/docs/guide/autoscaler.md)
- [Fingerprinting guide](https://github.com/satvik007/millipede/blob/main/docs/guide/fingerprinting.md)
- [Crawlee storage migration](https://github.com/satvik007/millipede/blob/main/docs/guide/crawlee-storage-migration.md)
- [Extras policy](https://github.com/satvik007/millipede/blob/main/docs/guide/extras.md)
- [Examples](https://github.com/satvik007/millipede/tree/main/millipede/examples)
- [Roadmap](https://github.com/satvik007/millipede/blob/main/docs/ROADMAP.md)
- [Interface design](https://github.com/satvik007/millipede/blob/main/docs/INTERFACE.md)

## License

Licensed under either the MIT License or the Apache License, Version 2.0, at your option. Contributions intentionally submitted for inclusion are licensed under the same terms.
