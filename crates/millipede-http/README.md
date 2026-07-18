# millipede-http

HttpCrawler for the Millipede web crawler: reqwest-based HTTP fetching.

[![crates.io](https://img.shields.io/crates/v/millipede-http.svg)](https://crates.io/crates/millipede-http) [![docs.rs](https://docs.rs/millipede-http/badge.svg)](https://docs.rs/millipede-http) [![license](https://img.shields.io/crates/l/millipede-http.svg)](https://github.com/satvik007/millipede#license)

This crate supplies [Millipede](https://github.com/satvik007/millipede) with its reqwest-backed HTTP crawler, including redirects, cookies, sessions, proxies, retry classification, response streaming, and request coalescing.

## Installation

```toml
[dependencies]
millipede-http = "0.1"
```

Most users should depend on the umbrella [`millipede`](https://crates.io/crates/millipede) crate instead.

## Example

```rust,no_run
use std::sync::Arc;

use millipede_core::prelude::Crawler;
use millipede_http::{HttpContext, HttpCrawler, HttpKindBuilder};
use millipede_storage_memory::MemoryStorageClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let kind = HttpKindBuilder::default().build()?;
    let crawler: HttpCrawler = Crawler::builder(kind)
        .max_request_retries(2)
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .request_handler(|ctx: HttpContext| async move {
            println!("{} -> {}", ctx.request.url, ctx.response.status);
            Ok(())
        })
        .build()
        .await?;

    crawler.run(["https://example.com/"]).await?;
    Ok(())
}
```

## Part of Millipede

See the [Millipede guide](https://github.com/satvik007/millipede/tree/main/docs/guide) for crawler concepts, storage, retries, sessions, and migration guidance.

## License

Licensed under either **MIT OR Apache-2.0** at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this crate is dual-licensed as above, without any additional terms or conditions.
