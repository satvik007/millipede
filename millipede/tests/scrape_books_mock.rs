//! Local end-to-end coverage for the `scrape_books` example and sitemap ingestion.

#![cfg(all(feature = "http", feature = "html"))]

use std::{collections::BTreeSet, io::Write, path::Path, sync::Arc};

use flate2::{Compression, write::GzEncoder};
use millipede::{
    Configuration, CrawlPolicy, Crawler, DatasetExt, EnqueueStrategy, HtmlContext, HtmlKind,
    ListOptions, MemoryRequestQueue, MemoryStorageClient, RequestQueue, RequestQueueWithSitemap,
    ReqwestClient, Router, SitemapRequestListBuilder, StorageClient,
};
use millipede_storage_fs::FsStorageClient;
use serde_json::{Value, json};
use url::Url;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::path};

millipede_html::selectors! {
    title_selector = "h1";
    price_selector = "p.price_color";
    availability_selector = "p.instock.availability";
}

const BOOKS: [(&str, &str, &str, &str); 4] = [
    ("alpha", "Alpha", "£10.10", "In stock"),
    ("beta", "Beta", "£20.20", "In stock (3 available)"),
    ("gamma", "Gamma", "£30.30", "In stock"),
    ("delta", "Delta", "£40.40", "In stock (1 available)"),
];

fn listing(slugs: &[&str], pager: Option<&str>) -> String {
    let products = slugs
        .iter()
        .map(|slug| {
            format!(
                "<article class=\"product_pod\"><h3><a href=\"/catalogue/{slug}/index.html\">\
                 {slug}</a></h3></article>"
            )
        })
        .collect::<String>();
    let pager = pager.map_or_else(String::new, |href| {
        format!("<ul class=\"pager\"><li><a href=\"{href}\">next</a></li></ul>")
    });
    format!("<html><body>{products}{pager}</body></html>")
}

async fn mount_site(server: &MockServer) {
    Mock::given(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            listing(&["alpha", "beta"], Some("/catalogue/page-2.html")).into_bytes(),
            "text/html; charset=utf-8",
        ))
        .mount(server)
        .await;
    Mock::given(path("/catalogue/page-2.html"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            listing(&["gamma", "delta"], None).into_bytes(),
            "text/html; charset=utf-8",
        ))
        .mount(server)
        .await;

    for (slug, title, price, availability) in BOOKS {
        let body = format!(
            "<html><body><h1>{title}</h1><p class=\"price_color\">{price}</p>\
             <p class=\"instock availability\">\n  {availability}\n</p></body></html>"
        );
        Mock::given(path(format!("/catalogue/{slug}/index.html")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(body.into_bytes(), "text/html; charset=utf-8"),
            )
            .mount(server)
            .await;
    }

    let sitemap = sitemap_xml(server);
    Mock::given(path("/sitemap.xml"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(sitemap.clone().into_bytes(), "application/xml"),
        )
        .mount(server)
        .await;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(sitemap.as_bytes())
        .expect("gzip encoder accepts sitemap bytes");
    let compressed = encoder.finish().expect("gzip encoder finishes");
    Mock::given(path("/sitemap-gzip-magic"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/octet-stream")
                .set_body_bytes(compressed),
        )
        .mount(server)
        .await;
}

fn sitemap_xml(server: &MockServer) -> String {
    let urls = BOOKS
        .iter()
        .map(|(slug, _, _, _)| {
            format!(
                "<url><loc>{}/catalogue/{slug}/index.html</loc></url>",
                server.uri()
            )
        })
        .collect::<String>();
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">{urls}</urlset>"
    )
}

fn element_text(ctx: &HtmlContext, selector: &millipede_html::scraper::Selector) -> String {
    ctx.html
        .select_first(selector, |element| element.text().collect::<String>())
        .unwrap_or_default()
        .trim()
        .to_owned()
}

fn book_router() -> Router<HtmlContext> {
    Router::<HtmlContext>::new()
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
            ctx.storage
                .dataset()
                .push(&json!({
                    "url": ctx.request.url,
                    "title": element_text(&ctx, title_selector()),
                    "price": element_text(&ctx, price_selector()),
                    "availability": element_text(&ctx, availability_selector()),
                }))
                .await?;
            Ok(())
        })
}

fn expected_books(server: &MockServer) -> BTreeSet<(String, String, String, String)> {
    BOOKS
        .iter()
        .map(|(slug, title, price, availability)| {
            (
                format!("{}/catalogue/{slug}/index.html", server.uri()),
                (*title).to_owned(),
                (*price).to_owned(),
                (*availability).to_owned(),
            )
        })
        .collect()
}

fn parsed_books(items: Vec<Value>) -> BTreeSet<(String, String, String, String)> {
    items
        .into_iter()
        .map(|item| {
            (
                item["url"]
                    .as_str()
                    .expect("book URL is a string")
                    .to_owned(),
                item["title"]
                    .as_str()
                    .expect("book title is a string")
                    .to_owned(),
                item["price"]
                    .as_str()
                    .expect("book price is a string")
                    .to_owned(),
                item["availability"]
                    .as_str()
                    .expect("availability is a string")
                    .to_owned(),
            )
        })
        .collect()
}

async fn crawl_to_fs(server: &MockServer, storage: Arc<FsStorageClient>) -> anyhow::Result<()> {
    let crawler = Crawler::builder(HtmlKind::new()?)
        .configuration(Configuration::builder().purge_on_start(true).build()?)
        .storage_client(storage)
        .crawl_policy(
            CrawlPolicy::new()
                .strategy(EnqueueStrategy::SameHostname)
                .max_requests_per_crawl(200),
        )
        .max_concurrency(5)
        .request_handler(book_router())
        .build()
        .await?;
    let stats = crawler.run(format!("{}/", server.uri())).await?;
    assert_eq!(stats.requests_finished, 6);
    Ok(())
}

async fn assert_pretty_dataset_files(root: &Path) -> anyhow::Result<()> {
    let dataset_path = root.join("datasets/default");
    for sequence in 1..=4 {
        let path = dataset_path.join(format!("{sequence:09}.json"));
        let bytes = tokio::fs::read(&path).await?;
        let value: Value = serde_json::from_slice(&bytes)?;
        assert_eq!(bytes, serde_json::to_vec_pretty(&value)?);
    }
    assert!(!dataset_path.join("000000005.json").exists());
    Ok(())
}

#[tokio::test]
async fn router_crawl_persists_four_books_and_purges_before_restart() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    mount_site(&server).await;
    let temporary = tempfile::tempdir()?;
    let storage_root = temporary.path().join("storage");
    let storage = Arc::new(FsStorageClient::new(&storage_root));

    crawl_to_fs(&server, storage.clone()).await?;
    let dataset = storage.open_dataset(Some("default")).await?;
    let page = dataset.list::<Value>(ListOptions::default()).await?;
    assert_eq!(page.items.len(), 4);
    assert_eq!(parsed_books(page.items), expected_books(&server));
    assert_pretty_dataset_files(&storage_root).await?;

    crawl_to_fs(&server, storage.clone()).await?;
    let dataset = storage.open_dataset(Some("default")).await?;
    let page = dataset.list::<Value>(ListOptions::default()).await?;
    assert_eq!(page.items.len(), 4);
    assert_eq!(parsed_books(page.items), expected_books(&server));
    assert_pretty_dataset_files(&storage_root).await?;
    Ok(())
}

async fn crawl_sitemap(server: &MockServer, sitemap_path: &str) -> anyhow::Result<()> {
    let storage = Arc::new(MemoryStorageClient::new());
    let client = Arc::new(ReqwestClient::new()?);
    let list = SitemapRequestListBuilder::default()
        .sitemap_url(Url::parse(&format!("{}{sitemap_path}", server.uri()))?)
        .http_client(client)
        .label("detail")
        .build()?;
    let queue: Arc<dyn RequestQueue> = Arc::new(RequestQueueWithSitemap::new(
        Arc::new(MemoryRequestQueue::new("sitemap")),
        list,
    ));
    let crawler = Crawler::builder(HtmlKind::new()?)
        .storage_client(storage.clone())
        .request_queue(queue)
        .max_concurrency(5)
        .request_handler(book_router())
        .build()
        .await?;
    let stats = crawler.run(Vec::<String>::new()).await?;
    assert_eq!(stats.requests_finished, 4);

    let dataset = storage.open_dataset(Some("default")).await?;
    let page = dataset.list::<Value>(ListOptions::default()).await?;
    assert_eq!(page.items.len(), 4);
    assert_eq!(parsed_books(page.items), expected_books(server));
    Ok(())
}

#[tokio::test]
async fn sitemap_xml_drives_the_real_reqwest_transport() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    mount_site(&server).await;
    crawl_sitemap(&server, "/sitemap.xml").await
}

#[tokio::test]
async fn gzip_sitemap_magic_bytes_decode_without_dot_gz_suffix() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    mount_site(&server).await;
    crawl_sitemap(&server, "/sitemap-gzip-magic").await
}
