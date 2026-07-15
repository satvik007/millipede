//! Crawls a 100-page mock site with `HttpCrawler`, sessions enabled by default, and
//! `EnqueueLinker` in URLs-only mode. Queue-level dedup lets the binary tree fan out while each
//! page is crawled exactly once.

use std::sync::Arc;

use wiremock::{Mock, MockServer, ResponseTemplate, matchers::path};

fn extract_links(body: &str) -> Vec<String> {
    // Phase 3 extracts URLs only; DOM parsing arrives with `HtmlCrawler` in Phase 5.
    body.split("href=\"")
        .skip(1)
        .filter_map(|fragment| fragment.split_once('"').map(|(url, _)| url.to_owned()))
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let server_uri = server.uri();

    for i in 0..100 {
        let mut body = String::new();
        for child in [2 * i + 1, 2 * i + 2] {
            if child < 100 {
                body.push_str(&format!("href=\"{server_uri}/page/{child}\"\n"));
            }
        }
        Mock::given(path(format!("/page/{i}")))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
    }

    let kind = millipede::HttpKind::builder().build()?;
    let crawler = millipede::Crawler::builder(kind)
        .max_concurrency(8)
        .storage_client(Arc::new(millipede::MemoryStorageClient::new()))
        .request_handler(|ctx: millipede::HttpContext| async move {
            let body = ctx.response.text().into_owned();
            let links = extract_links(&body);
            if !links.is_empty() {
                ctx.enqueue.options().raw_urls(links).send().await?;
            }
            Ok(())
        })
        .failed_request_handler(|ctx: millipede::FailedRequestContext| async move {
            eprintln!("failed to crawl {}: {}", ctx.request.url, ctx.error);
            Ok(())
        })
        .build()
        .await?;

    let stats = crawler.run(format!("{server_uri}/page/0")).await?;
    println!(
        "requests_finished={} requests_failed={} requests_retries={}",
        stats.requests_finished, stats.requests_failed, stats.requests_retries,
    );

    anyhow::ensure!(
        stats.requests_finished == 100,
        "expected 100 finished requests, got {}",
        stats.requests_finished
    );
    anyhow::ensure!(
        stats.requests_failed == 0,
        "expected no failed requests, got {}",
        stats.requests_failed
    );
    Ok(())
}
