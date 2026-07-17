# millipede-html

`millipede-html` adds parsed HTML documents to Millipede's HTTP crawler. Each handler receives an
[`HtmlContext`](https://docs.rs/millipede-html/latest/millipede_html/struct.HtmlContext.html) with
the final HTTP response and a shared
[`SynchronizedHtml`](https://docs.rs/millipede-html/latest/millipede_html/struct.SynchronizedHtml.html)
with guard-free CSS selector helpers.

> **Unratified API drift:** `INTERFACE.md` §4.2 specifies `Arc<scraper::Html>` for
> `HtmlContext::html`, assuming scraper's `atomic` feature makes `scraper::Html: Sync`. This does
> not hold for scraper 0.24.0 and tendril 0.4.3 as resolved in this workspace. Scraper's element
> caches use `std::cell::OnceCell`, while atomic tendrils implement `Send` but not `Sync`, so
> `Arc<scraper::Html>` is not `Send` and cannot satisfy the crawler context bound. `SynchronizedHtml`
> is the minimum safe synchronization boundary; this deviation requires an `INTERFACE.md`
> amendment or ADR before dependent Phase 5 work such as selector-based enqueue extraction and
> the `scrape_books` example proceeds.

```no_run
use std::sync::Arc;

use millipede_core::{crawler::Crawler, request::Request};
use millipede_html::{HtmlContext, HtmlKind};
use millipede_storage_memory::MemoryStorageClient;
use scraper::Selector;

# async fn crawl() -> Result<(), Box<dyn std::error::Error>> {
let crawler = Crawler::builder(HtmlKind::new()?)
    .request_handler(|ctx: HtmlContext| async move {
        let title = Selector::parse("title").expect("valid selector");
        let text = ctx
            .html
            .select_first(&title, |element| element.text().collect::<String>());
        if let Some(text) = text {
            println!("{}: {text}", ctx.request.url);
        }
        Ok(())
    })
    .storage_client(Arc::new(MemoryStorageClient::new()))
    .build()
    .await?;

let stats = crawler
    .run([Request::get("https://example.com").build()?])
    .await?;
assert_eq!(stats.requests_finished, 1);
# Ok(())
# }
```
