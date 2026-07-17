//! Manual live-network coverage. Run with
//! `cargo test -p millipede --test scrape_books_live -- --ignored`.

#![cfg(all(feature = "http", feature = "html"))]

use std::sync::Arc;

use millipede::{
    CrawlPolicy, Crawler, DatasetExt, EnqueueStrategy, HtmlContext, HtmlKind, ListOptions,
    MemoryStorageClient, Router, StorageClient,
};
use serde_json::json;

millipede_html::selectors! {
    title_selector = "h1";
}

#[tokio::test]
#[ignore = "hits books.toscrape.com over the real network"]
async fn scrapes_books_over_the_live_network() -> anyhow::Result<()> {
    let storage = Arc::new(MemoryStorageClient::new());
    let router = Router::<HtmlContext>::new()
        .default(|ctx: HtmlContext| async move {
            ctx.enqueue.options().selector("ul.pager a").send().await?;
            ctx.enqueue
                .options()
                .selector("article.product_pod h3 a")
                .globs(["**/catalogue/**"])
                .label("detail")
                .send()
                .await?;
            Ok(())
        })
        .route("detail", |ctx: HtmlContext| async move {
            let title = ctx
                .html
                .select_first(title_selector(), |element| {
                    element.text().collect::<String>()
                })
                .unwrap_or_default();
            ctx.storage
                .dataset()
                .push(&json!({ "url": ctx.request.url, "title": title.trim() }))
                .await?;
            Ok(())
        });
    let crawler = Crawler::builder(HtmlKind::new()?)
        .storage_client(storage.clone())
        .crawl_policy(
            CrawlPolicy::new()
                .strategy(EnqueueStrategy::SameHostname)
                .max_requests_per_crawl(30),
        )
        .max_concurrency(5)
        .request_handler(router)
        .build()
        .await?;

    let stats = crawler.run("https://books.toscrape.com/").await?;
    let dataset = storage.open_dataset(Some("default")).await?;
    let books = dataset
        .list::<serde_json::Value>(ListOptions::default())
        .await?;
    assert!(stats.requests_finished > 0);
    assert!(!books.items.is_empty());
    Ok(())
}
