# millipede-html

`millipede-html` adds parsed HTML documents to Millipede's HTTP crawler. Each handler receives an
[`HtmlContext`](https://docs.rs/millipede-html/latest/millipede_html/struct.HtmlContext.html) with
the final HTTP response and a shared
[`SynchronizedHtml`](https://docs.rs/millipede-html/latest/millipede_html/struct.SynchronizedHtml.html)
with owned CSS selector helpers and a dereferencing lock guard for the complete `scraper::Html`
API.

Scraper 0.24's `atomic` feature makes `scraper::Html` `Send`, but not `Sync`: its element caches use
`std::cell::OnceCell`, and atomic tendrils are not `Sync`. Therefore `Arc<scraper::Html>` is not
`Send` and cannot satisfy Millipede's spawned-handler context contract. `SynchronizedHtml` is the
ADR-0005 synchronization boundary; its `lock()` guard dereferences to `scraper::Html` when callers
need APIs beyond the owned helpers and must be dropped before `.await`.

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
