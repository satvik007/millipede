# millipede-browser-chromiumoxide

Chromium CDP browser provider for the Millipede web crawler, via chromiumoxide.

[![crates.io](https://img.shields.io/crates/v/millipede-browser-chromiumoxide.svg)](https://crates.io/crates/millipede-browser-chromiumoxide) [![docs.rs](https://docs.rs/millipede-browser-chromiumoxide/badge.svg)](https://docs.rs/millipede-browser-chromiumoxide) [![license](https://img.shields.io/crates/l/millipede-browser-chromiumoxide.svg)](https://github.com/satvik007/millipede#license)

This crate connects [Millipede](https://github.com/satvik007/millipede)'s provider-neutral browser crawler to Chromium or Google Chrome over the Chrome DevTools Protocol. Browser discovery checks `MILLIPEDE_CHROME`, then `CHROME`, then conventional platform installation paths.

## Installation

```toml
[dependencies]
millipede-browser-chromiumoxide = "0.1"
```

Most users should depend on the umbrella [`millipede`](https://crates.io/crates/millipede) crate with its browser Chromiumoxide feature instead.

## Example

This example needs a real Chrome or Chromium installation. Set `MILLIPEDE_CHROME` when automatic discovery cannot find the executable.

```rust,no_run
use std::sync::Arc;

use millipede_browser::{BrowserContext, BrowserCrawler, BrowserKind};
use millipede_browser_chromiumoxide::{
    ChromiumLaunchOptions, ChromiumoxideProvider, find_browser,
};
use millipede_core::prelude::Crawler;
use millipede_storage_memory::MemoryStorageClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let executable = find_browser().expect("Chrome or Chromium is required");
    let kind = BrowserKind::builder(ChromiumoxideProvider)
        .launch_options(ChromiumLaunchOptions::default().with_executable(executable))
        .build()?;
    let crawler: BrowserCrawler<ChromiumoxideProvider> = Crawler::builder(kind)
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .request_handler(|ctx: BrowserContext| async move {
            let title = ctx.page.evaluate_js("document.title").await.ok();
            println!("{}: {title:?}", ctx.request.url);
            Ok(())
        })
        .build()
        .await?;

    crawler.run(["https://example.com/"]).await?;
    Ok(())
}
```

## Part of Millipede

See the [Millipede guide](https://github.com/satvik007/millipede/tree/main/docs/guide) for browser pools, hooks, smart crawling, and fingerprinting guidance.

## License

Licensed under either **MIT OR Apache-2.0** at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this crate is dual-licensed as above, without any additional terms or conditions.
