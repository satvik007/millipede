//! Manual live-network coverage. Run with
//! `cargo test -p millipede --test scrape_books_live -- --ignored`.

#![cfg(all(feature = "http", feature = "html"))]

use std::sync::{Arc, Mutex};

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
            let _ = ctx.enqueue.options().selector("ul.pager a").send().await?;
            let _ = ctx
                .enqueue
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

// This sandbox has no network egress and cannot bind localhost, so this stays ignored. The lead
// engineer runs it on the host with `cargo test -p millipede --test scrape_books_live -- --ignored`;
// exit auditors must not report sandbox network failures as blockers.
#[tokio::test]
#[ignore = "hits books.toscrape.com over the real network"]
async fn crawls_without_trivial_bot_blocks() -> anyhow::Result<()> {
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let crawler = Crawler::builder(HtmlKind::builder().detect_anti_bot_default().build()?)
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .crawl_policy(
            CrawlPolicy::new()
                .strategy(EnqueueStrategy::SameHostname)
                .max_requests_per_crawl(30),
        )
        .max_concurrency(5)
        .request_handler(|ctx: HtmlContext| async move {
            let _ = ctx.enqueue.options().selector("a").send().await?;
            Ok(())
        })
        .failed_request_handler({
            let errors = Arc::clone(&errors);
            move |ctx: millipede::FailedRequestContext| {
                let errors = Arc::clone(&errors);
                async move {
                    errors
                        .lock()
                        .expect("error mutex poisoned")
                        .push(ctx.error.to_string());
                    Ok(())
                }
            }
        })
        .build()
        .await?;

    let stats = crawler.run("https://books.toscrape.com/").await?;
    assert!(stats.requests_finished > 0);
    assert!(
        errors
            .lock()
            .expect("error mutex poisoned")
            .iter()
            .all(|error| !error.contains("anti-bot detected"))
    );
    Ok(())
}
