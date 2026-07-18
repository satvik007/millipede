//! Demonstrates gocolly-style link scraping and same-hostname filtering with `HtmlCrawler`.
//!
//! Run with: `cargo run -p millipede --example basic`

use std::{collections::BTreeSet, sync::Arc};

use millipede::{
    CrawlPolicy, Crawler, EnqueueStrategy, HtmlContext, HtmlKind, MemoryStorageClient,
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

millipede_html::selectors! {
    title_selector = "title";
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let server_uri = server.uri();

    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"<html><head><title>Home</title></head><body>
                <a href="/a">A</a>
                <a href="/b">B</a>
                <a href="http://external.invalid/x">External</a>
            </body></html>"#,
            "text/html",
        ))
        .mount(&server)
        .await;
    for (page, title) in [("/a", "Page A"), ("/b", "Page B")] {
        Mock::given(method("GET"))
            .and(path(page))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                format!("<html><head><title>{title}</title></head></html>"),
                "text/html",
            ))
            .mount(&server)
            .await;
    }

    let crawler = Crawler::builder(HtmlKind::new()?)
        .crawl_policy(CrawlPolicy::new().max_requests_per_crawl(10))
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .request_handler(|ctx: HtmlContext| async move {
            let title = ctx
                .html
                .select_first(title_selector(), |element| {
                    element.text().collect::<String>()
                })
                .unwrap_or_else(|| "<untitled>".to_owned());
            println!("{} -> {title}", ctx.request.url);
            let _ = ctx
                .enqueue
                .options()
                .strategy(EnqueueStrategy::SameHostname)
                .send()
                .await?;
            Ok(())
        })
        .build()
        .await?;

    let stats = crawler.run(format!("{server_uri}/")).await?;
    let requests = server
        .received_requests()
        .await
        .ok_or_else(|| anyhow::anyhow!("wiremock request recording is unavailable"))?;
    let paths = requests
        .iter()
        .map(|request| request.url.path().to_owned())
        .collect::<BTreeSet<_>>();

    anyhow::ensure!(
        stats.requests_finished == 3 && stats.requests_failed == 0,
        "domain filtering failed: expected 3 local successes and no failures, got {stats:#?}"
    );
    anyhow::ensure!(
        paths == BTreeSet::from(["/".to_owned(), "/a".to_owned(), "/b".to_owned()]),
        "expected only the three local paths, got {paths:?}"
    );
    anyhow::ensure!(
        requests.len() == 3,
        "external link may have escaped filtering: local server saw {} requests",
        requests.len()
    );
    println!(
        "summary: crawled {} local pages; external.invalid was filtered before fetching",
        stats.requests_finished
    );
    Ok(())
}
