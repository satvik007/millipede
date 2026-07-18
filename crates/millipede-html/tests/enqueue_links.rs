//! Scraper-backed enqueue integration over a realistic miniature site.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use millipede_core::{
    crawler::Crawler,
    enqueue::SkipReason,
    link_extraction::{CrawlPolicy, EnqueueStrategy, GlobPattern, TransformResult, UrlMatch},
    router::Router,
    storage::{DatasetExt, ListOptions, StorageClient},
};
use millipede_html::{HtmlContext, HtmlKind};
use millipede_storage_memory::MemoryStorageClient;
use scraper::Selector;
use serde_json::json;
use url::Url;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::path};

fn url(server: &MockServer, path: &str) -> Url {
    Url::parse(&format!("{}{path}", server.uri())).expect("mock URL must parse")
}

async fn page(server: &MockServer, path_value: &str, body: &'static str) {
    Mock::given(path(path_value))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/html"))
        .mount(server)
        .await;
}

async fn mount_site(server: &MockServer) {
    page(
        server,
        "/",
        r##"<a href="/categories/1">one</a>
           <a href="/categories/2">two</a>
           <a href="https://elsewhere.example/x">elsewhere</a>
           <a href="mailto:test@example.com">mail</a>
           <a href="#frag">fragment</a>
           <a class="promo" href="/products/promo">promo</a>"##,
    )
    .await;
    page(
        server,
        "/categories/1",
        r#"<base href="/">
           <a class="product" href="products/1-a">1a</a>
           <a class="product" href="products/1-b">1b</a>
           <a class="product" href="/offers/product-class">glob must exclude this</a>
           <a href="/products/selector-hidden">selector must exclude this</a>"#,
    )
    .await;
    page(
        server,
        "/categories/2",
        r#"<a class="product" href="/products/2-a">2a</a>
           <a class="product" href="/products/2-b">2b</a>
           <a href="/categories/1">cycle</a>"#,
    )
    .await;
    for (path_value, title) in [
        ("/products/1-a", "One A"),
        ("/products/1-b", "One B"),
        ("/products/2-a", "Two A"),
        ("/products/2-b", "Two B"),
        ("/products/promo", "Promo"),
    ] {
        let body = format!(
            "<html><head><title>{title}</title></head><body><span class=\"price\">$1</span></body></html>"
        );
        Mock::given(path(path_value))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/html"))
            .mount(server)
            .await;
    }
}

#[tokio::test]
async fn same_hostname_filters_and_queue_deduplicates_fragment()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    mount_site(&server).await;
    let result = Arc::new(Mutex::new(None));
    let crawler = Crawler::builder(HtmlKind::new()?)
        .request_handler({
            let result = Arc::clone(&result);
            move |ctx: HtmlContext| {
                let result = Arc::clone(&result);
                async move {
                    if ctx.request.url.path() == "/" {
                        *result.lock().expect("result mutex poisoned") =
                            Some(ctx.enqueue.same_hostname().await?);
                    }
                    Ok(())
                }
            }
        })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;
    let _ = crawler.run([url(&server, "/")]).await?;

    let result = result.lock().expect("result mutex poisoned");
    let result = result.as_ref().expect("root result captured");
    assert_eq!(result.added_count(), 3);
    let mut added_unique_keys: Vec<_> = result
        .added
        .iter()
        .map(|request| request.unique_key.clone())
        .collect();
    added_unique_keys.sort();
    let mut expected_unique_keys = vec![
        url(&server, "/categories/1").to_string(),
        url(&server, "/categories/2").to_string(),
        url(&server, "/products/promo").to_string(),
    ];
    expected_unique_keys.sort();
    assert_eq!(added_unique_keys, expected_unique_keys);
    assert!(
        result
            .skipped
            .iter()
            .any(|skip| skip.url == "https://elsewhere.example/x"
                && skip.reason == SkipReason::StrategyExcluded)
    );
    assert!(
        result
            .skipped
            .iter()
            .any(|skip| skip.url.starts_with("mailto:")
                && skip.reason == SkipReason::StrategyExcluded)
    );
    let duplicate = result
        .skipped
        .iter()
        .find(|skip| skip.reason == SkipReason::DuplicateUniqueKey)
        .expect("fragment link must be reported as the queue duplicate");
    let mut duplicate_url = Url::parse(&duplicate.url).expect("duplicate URL must parse");
    assert_eq!(duplicate_url.fragment(), Some("frag"));
    duplicate_url.set_fragment(None);
    assert_eq!(duplicate_url, url(&server, "/"));
    Ok(())
}

#[tokio::test]
async fn selector_glob_and_base_href_find_exact_products() -> Result<(), Box<dyn std::error::Error>>
{
    let server = MockServer::start().await;
    mount_site(&server).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let crawler = Crawler::builder(HtmlKind::new()?)
        .request_handler({
            let seen = Arc::clone(&seen);
            move |ctx: HtmlContext| {
                let seen = Arc::clone(&seen);
                async move {
                    if ctx.request.url.path().starts_with("/categories/") {
                        let _ = ctx
                            .enqueue
                            .options()
                            .selector("a.product")
                            .globs(["**/products/*"])
                            .label("detail")
                            .send()
                            .await?;
                    } else if ctx.request.label.as_deref() == Some("detail") {
                        seen.lock()
                            .expect("seen mutex poisoned")
                            .push(ctx.request.url.path().to_owned());
                    }
                    Ok(())
                }
            }
        })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;
    let stats = crawler
        .run([url(&server, "/categories/1"), url(&server, "/categories/2")])
        .await?;
    // The classless /products/selector-hidden link matches the products glob but not the
    // `a.product` selector and has no mock route; a selector-ignoring regression would enqueue
    // it and surface here as a failed request rather than in `seen`.
    assert_eq!(stats.requests_failed, 0);

    let mut seen = seen.lock().expect("seen mutex poisoned").clone();
    seen.sort();
    assert_eq!(
        seen,
        [
            "/products/1-a",
            "/products/1-b",
            "/products/2-a",
            "/products/2-b"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn relative_links_resolve_against_final_response_url_after_redirect()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(path("/redirect/original"))
        .respond_with(ResponseTemplate::new(302).insert_header("location", "/redirect/final/page"))
        .mount(&server)
        .await;
    page(
        &server,
        "/redirect/final/page",
        r#"<a href="child">child</a>"#,
    )
    .await;
    page(&server, "/redirect/final/child", "<title>child</title>").await;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let crawler = Crawler::builder(HtmlKind::new()?)
        .request_handler({
            let seen = Arc::clone(&seen);
            move |ctx: HtmlContext| {
                let seen = Arc::clone(&seen);
                async move {
                    if ctx.request.crawl_depth == 0 {
                        let _ = ctx.enqueue.same_hostname().await?;
                    } else {
                        seen.lock()
                            .expect("seen mutex poisoned")
                            .push(ctx.request.url.path().to_owned());
                    }
                    Ok(())
                }
            }
        })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;
    let _ = crawler.run([url(&server, "/redirect/original")]).await?;

    assert_eq!(
        *seen.lock().expect("seen mutex poisoned"),
        ["/redirect/final/child"]
    );
    Ok(())
}

#[tokio::test]
async fn per_pattern_override_labels_products_only() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    mount_site(&server).await;
    let seen = Arc::new(Mutex::new(HashMap::new()));
    let crawler = Crawler::builder(HtmlKind::new()?)
        .request_handler({
            let seen = Arc::clone(&seen);
            move |ctx: HtmlContext| {
                let seen = Arc::clone(&seen);
                async move {
                    if ctx.request.url.path() == "/categories/2" && ctx.request.crawl_depth == 0 {
                        let _ = ctx
                            .enqueue
                            .options()
                            .globs([
                                GlobPattern::from(UrlMatch::new("**/products/*").label("detail")),
                                GlobPattern::from("**/categories/*"),
                            ])
                            .send()
                            .await?;
                    } else {
                        seen.lock()
                            .expect("seen mutex poisoned")
                            .insert(ctx.request.url.path().to_owned(), ctx.request.label.clone());
                    }
                    Ok(())
                }
            }
        })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;
    let _ = crawler.run([url(&server, "/categories/2")]).await?;

    let seen = seen.lock().expect("seen mutex poisoned");
    assert_eq!(seen.get("/products/2-a"), Some(&Some("detail".to_owned())));
    assert_eq!(seen.get("/products/2-b"), Some(&Some("detail".to_owned())));
    assert_eq!(seen.get("/categories/1"), Some(&None));
    Ok(())
}

#[tokio::test]
async fn transform_and_policy_report_rejections() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    mount_site(&server).await;
    let counts = Arc::new(Mutex::new((0_u32, 0_u32)));
    let crawled = Arc::new(Mutex::new(Vec::new()));
    let enqueue_result = Arc::new(Mutex::new(None));
    let policy = CrawlPolicy::new().on_skipped({
        let counts = Arc::clone(&counts);
        move |_url: &str, reason: &SkipReason| {
            let mut counts = counts.lock().expect("counts mutex poisoned");
            match reason {
                SkipReason::TransformRejected { .. } => counts.0 += 1,
                SkipReason::StrategyExcluded => counts.1 += 1,
                _ => {}
            }
        }
    });
    let crawler = Crawler::builder(HtmlKind::new()?)
        .request_handler({
            let crawled = Arc::clone(&crawled);
            let enqueue_result = Arc::clone(&enqueue_result);
            move |ctx: HtmlContext| {
                let crawled = Arc::clone(&crawled);
                let enqueue_result = Arc::clone(&enqueue_result);
                async move {
                    if ctx.request.url.path() == "/" {
                        let result = ctx
                            .enqueue
                            .options()
                            .strategy(EnqueueStrategy::SameHostname)
                            .transform(|request| {
                                Box::pin(async move {
                                    if request.url.path() == "/categories/1" {
                                        request.label = Some("mutated".to_owned());
                                    }
                                    if request.url.path() == "/categories/2" {
                                        return TransformResult::Skip {
                                            reason: "not this one".to_owned(),
                                        };
                                    }
                                    TransformResult::Enqueue
                                })
                            })
                            .send()
                            .await?;
                        *enqueue_result
                            .lock()
                            .expect("enqueue result mutex poisoned") = Some(result);
                    } else {
                        crawled
                            .lock()
                            .expect("crawled mutex poisoned")
                            .push((ctx.request.url.clone(), ctx.request.label.clone()));
                    }
                    Ok(())
                }
            }
        })
        .crawl_policy(policy)
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;
    let _ = crawler.run([url(&server, "/")]).await?;

    let counts = *counts.lock().expect("counts mutex poisoned");
    assert_eq!(counts.0, 1);
    assert_eq!(counts.1, 2);

    let rejected_url = url(&server, "/categories/2");
    let enqueue_result = enqueue_result
        .lock()
        .expect("enqueue result mutex poisoned");
    let enqueue_result = enqueue_result.as_ref().expect("enqueue result captured");
    assert_eq!(enqueue_result.added_count(), 2);
    assert!(
        enqueue_result
            .added
            .iter()
            .all(|request| request.unique_key != rejected_url.as_str())
    );

    let crawled = crawled.lock().expect("crawled mutex poisoned");
    assert!(
        crawled
            .iter()
            .all(|(request_url, _)| request_url != &rejected_url)
    );
    assert!(crawled.iter().any(|(request_url, label)| {
        request_url.path() == "/categories/1" && label.as_deref() == Some("mutated")
    }));
    Ok(())
}

#[tokio::test]
async fn router_crawls_four_products_once() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    mount_site(&server).await;
    let storage = Arc::new(MemoryStorageClient::new());
    let router = Router::<HtmlContext>::new()
        .route("category", |ctx: HtmlContext| async move {
            let _ = ctx
                .enqueue
                .options()
                .selector("a.product")
                .globs(["**/products/*"])
                .label("detail")
                .send()
                .await?;
            Ok(())
        })
        .route("detail", |ctx: HtmlContext| async move {
            let selector = Selector::parse("title").expect("title selector must parse");
            let title = ctx
                .html
                .select_first(&selector, |element| element.text().collect::<String>())
                .unwrap_or_default();
            ctx.storage
                .dataset()
                .push(&json!({ "url": ctx.request.url, "title": title }))
                .await?;
            Ok(())
        })
        .default(|ctx: HtmlContext| async move {
            let _ = ctx
                .enqueue
                .options()
                .globs(["**/categories/*"])
                .label("category")
                .send()
                .await?;
            Ok(())
        });
    let crawler = Crawler::builder(HtmlKind::new()?)
        .request_handler(router)
        .crawl_policy(
            CrawlPolicy::new()
                .strategy(EnqueueStrategy::SameHostname)
                .max_crawl_depth(3),
        )
        .storage_client(storage.clone())
        .build()
        .await?;
    let stats = crawler.run([url(&server, "/")]).await?;

    assert_eq!(stats.requests_finished, 7);
    let dataset = storage.open_dataset(Some("default")).await?;
    let items = dataset.list_raw(ListOptions::default()).await?.items;
    assert_eq!(items.len(), 4);
    let mut paths: Vec<_> = items
        .iter()
        .map(|item| {
            Url::parse(item["url"].as_str().expect("URL string"))
                .expect("valid URL")
                .path()
                .to_owned()
        })
        .collect();
    paths.sort();
    assert_eq!(
        paths,
        [
            "/products/1-a",
            "/products/1-b",
            "/products/2-a",
            "/products/2-b"
        ]
    );
    Ok(())
}
