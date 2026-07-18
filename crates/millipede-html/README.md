# millipede-html

HtmlCrawler for the Millipede web crawler: HTML parsing via scraper.

[![crates.io](https://img.shields.io/crates/v/millipede-html.svg)](https://crates.io/crates/millipede-html) [![docs.rs](https://docs.rs/millipede-html/badge.svg)](https://docs.rs/millipede-html) [![license](https://img.shields.io/crates/l/millipede-html.svg)](https://github.com/satvik007/millipede#license)

This crate adds synchronized HTML parsing, owned CSS-selector helpers, and link extraction to [Millipede](https://github.com/satvik007/millipede)'s HTTP crawler. Its public `selectors!` macro caches validated selectors for reuse.

## Installation

```toml
[dependencies]
millipede-html = "0.1"
```

Most users should depend on the umbrella [`millipede`](https://crates.io/crates/millipede) crate instead.

## Example

```rust,no_run
use std::sync::Arc;

use millipede_core::prelude::Crawler;
use millipede_html::{HtmlContext, HtmlCrawler, HtmlKind};
use millipede_storage_memory::MemoryStorageClient;

millipede_html::selectors! {
    title_selector = "title";
}

# async fn crawl() -> Result<(), Box<dyn std::error::Error>> {
let crawler: HtmlCrawler = Crawler::builder(HtmlKind::new()?)
    .storage_client(Arc::new(MemoryStorageClient::new()))
    .request_handler(|ctx: HtmlContext| async move {
        if let Some(title) = ctx
            .html
            .select_first(title_selector(), |element| element.text().collect::<String>())
        {
            println!("{}: {title}", ctx.request.url);
        }
        let _ = ctx.enqueue.options().selector("a[href]").send().await?;
        Ok(())
    })
    .build()
    .await?;

crawler.run(["https://example.com/"]).await?;
# Ok(())
# }
```

## Part of Millipede

See the [Millipede guide](https://github.com/satvik007/millipede/tree/main/docs/guide) for crawler concepts, link discovery, routing, and scraping patterns.

## License

Licensed under either **MIT OR Apache-2.0** at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this crate is dual-licensed as above, without any additional terms or conditions.
