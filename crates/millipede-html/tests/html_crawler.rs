//! End-to-end tests for the HTML crawler kind.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures_util::stream;
use http::{HeaderMap, HeaderValue, StatusCode, header::CONTENT_TYPE};
use millipede_core::{
    crawler::Crawler,
    errors::CrawlError,
    handler::FailedRequestContext,
    http_client::{HttpClient, HttpClientError, HttpRequest, HttpResponse, StreamingResponse},
    storage::{DatasetExt, ListOptions, StorageClient},
};
use millipede_html::{HtmlContext, HtmlError, HtmlKind};
use millipede_storage_memory::MemoryStorageClient;
use scraper::Selector;
use serde_json::json;
use url::Url;

fn url(path: &str) -> Url {
    Url::parse(&format!("https://example.test{path}")).expect("test URL must parse")
}

fn response(path: &str, body: &str, content_type: Option<&'static str>) -> HttpResponse {
    let mut headers = HeaderMap::new();
    if let Some(content_type) = content_type {
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    }
    HttpResponse::new(url(path), StatusCode::OK, headers, body.to_owned().into())
}

fn html(path: &str, body: &str) -> HttpResponse {
    response(path, body, Some("text/html; charset=utf-8"))
}

struct StaticClient {
    responses: HashMap<String, HttpResponse>,
    calls: Mutex<Vec<String>>,
}

impl StaticClient {
    fn new(responses: impl IntoIterator<Item = HttpResponse>) -> Arc<Self> {
        Arc::new(Self {
            responses: responses
                .into_iter()
                .map(|response| {
                    (
                        response
                            .redirect_chain
                            .first()
                            .unwrap_or(&response.url)
                            .path()
                            .to_owned(),
                        response,
                    )
                })
                .collect(),
            calls: Mutex::new(Vec::new()),
        })
    }

    fn call_count(&self, path: &str) -> usize {
        self.calls
            .lock()
            .expect("calls mutex poisoned")
            .iter()
            .filter(|called| called.as_str() == path)
            .count()
    }
}

#[async_trait::async_trait]
impl HttpClient for StaticClient {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpClientError> {
        let path = request.url.path().to_owned();
        self.calls
            .lock()
            .expect("calls mutex poisoned")
            .push(path.clone());
        Ok(self
            .responses
            .get(&path)
            .unwrap_or_else(|| panic!("no static response for {path}"))
            .clone())
    }

    async fn stream(&self, request: HttpRequest) -> Result<StreamingResponse, HttpClientError> {
        Ok(StreamingResponse::new(
            request.url,
            StatusCode::OK,
            HeaderMap::new(),
            Box::pin(stream::empty()),
        ))
    }
}

fn kind(client: Arc<StaticClient>) -> Result<HtmlKind, HttpClientError> {
    HtmlKind::builder().http_client(client).build()
}

#[tokio::test]
async fn extracts_title_and_pushes_it_to_dataset() -> Result<(), Box<dyn std::error::Error>> {
    let client = StaticClient::new([html(
        "/article",
        "<html><head><title>Millipede</title></head></html>",
    )]);
    let storage = Arc::new(MemoryStorageClient::new());
    let crawler = Crawler::builder(kind(client.clone())?)
        .request_handler(|ctx: HtmlContext| async move {
            let selector = Selector::parse("title").expect("title selector must parse");
            let title = ctx
                .html
                .select_first(&selector, |element| element.text().collect::<String>())
                .expect("page must contain a title");
            ctx.storage
                .dataset()
                .push(&json!({ "url": ctx.request.url, "title": title }))
                .await?;
            Ok(())
        })
        .storage_client(storage.clone())
        .build()
        .await?;

    let stats = crawler.run([url("/article")]).await?;

    assert_eq!(stats.requests_finished, 1);
    assert_eq!(client.call_count("/article"), 1);
    let dataset = storage.open_dataset(Some("default")).await?;
    let page = dataset.list_raw(ListOptions::default()).await?;
    assert_eq!(
        page.items,
        vec![json!({
            "url": url("/article"),
            "title": "Millipede"
        })]
    );
    Ok(())
}

#[tokio::test]
async fn rejects_non_html_content_type_permanently() -> Result<(), Box<dyn std::error::Error>> {
    let client = StaticClient::new([response("/document", "%PDF-1.7", Some("application/pdf"))]);
    let observed = Arc::new(Mutex::new(false));
    let crawler = Crawler::builder(kind(client.clone())?)
        .request_handler(|_: HtmlContext| async { Ok(()) })
        .failed_request_handler({
            let observed = Arc::clone(&observed);
            move |ctx: FailedRequestContext| {
                let observed = Arc::clone(&observed);
                async move {
                    let contains_html_error = match ctx.error.as_ref() {
                        CrawlError::NonRetryable(source) => source
                            .chain()
                            .any(|error| error.downcast_ref::<HtmlError>().is_some()),
                        _ => false,
                    };
                    *observed.lock().expect("observed mutex poisoned") = contains_html_error;
                    Ok(())
                }
            }
        })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;

    let stats = crawler.run([url("/document")]).await?;

    assert_eq!(stats.requests_failed, 1);
    assert_eq!(stats.requests_retries, 0);
    assert!(*observed.lock().expect("observed mutex poisoned"));
    assert_eq!(client.call_count("/document"), 1);
    Ok(())
}

#[tokio::test]
async fn parses_when_content_type_is_missing() -> Result<(), Box<dyn std::error::Error>> {
    let client = StaticClient::new([response(
        "/untyped",
        "<html><head><title>Untyped</title></head></html>",
        None,
    )]);
    let title = Arc::new(Mutex::new(None));
    let crawler = Crawler::builder(kind(client)?)
        .request_handler({
            let title = Arc::clone(&title);
            move |ctx: HtmlContext| {
                let title = Arc::clone(&title);
                async move {
                    let selector = Selector::parse("title").expect("title selector must parse");
                    let parsed = ctx
                        .html
                        .select_first(&selector, |element| element.inner_html())
                        .expect("page must contain a title");
                    *title.lock().expect("title mutex poisoned") = Some(parsed);
                    Ok(())
                }
            }
        })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;

    let stats = crawler.run([url("/untyped")]).await?;

    assert_eq!(stats.requests_finished, 1);
    assert_eq!(
        title.lock().expect("title mutex poisoned").as_deref(),
        Some("Untyped")
    );
    Ok(())
}

#[tokio::test]
async fn urls_only_enqueue_handles_the_second_page() -> Result<(), Box<dyn std::error::Error>> {
    let client = StaticClient::new([
        html("/a", "<html><body>A</body></html>"),
        html("/b", "<html><body>B</body></html>"),
    ]);
    let page_b = url("/b");
    let handled = Arc::new(Mutex::new(Vec::new()));
    let crawler = Crawler::builder(kind(client.clone())?)
        .request_handler({
            let handled = Arc::clone(&handled);
            move |ctx: HtmlContext| {
                let handled = Arc::clone(&handled);
                let page_b = page_b.clone();
                async move {
                    handled
                        .lock()
                        .expect("handled mutex poisoned")
                        .push(ctx.request.url.path().to_owned());
                    if ctx.request.url.path() == "/a" {
                        ctx.enqueue.urls([page_b]).await?;
                    }
                    Ok(())
                }
            }
        })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;

    let stats = crawler.run([url("/a")]).await?;

    assert_eq!(stats.requests_finished, 2);
    let mut handled = handled.lock().expect("handled mutex poisoned").clone();
    handled.sort();
    assert_eq!(handled, vec!["/a", "/b"]);
    assert_eq!(client.call_count("/a"), 1);
    assert_eq!(client.call_count("/b"), 1);
    Ok(())
}

#[tokio::test]
async fn redirect_parses_final_body_and_exposes_chain() -> Result<(), Box<dyn std::error::Error>> {
    let redirect = url("/redirect");
    let response = html("/final", "<html><head><title>Final</title></head></html>")
        .with_redirect_chain(vec![redirect]);
    let client = StaticClient::new([response]);
    let observed = Arc::new(Mutex::new(None));
    let crawler = Crawler::builder(kind(client.clone())?)
        .request_handler({
            let observed = Arc::clone(&observed);
            move |ctx: HtmlContext| {
                let observed = Arc::clone(&observed);
                async move {
                    let selector = Selector::parse("title").expect("title selector must parse");
                    let title = ctx
                        .html
                        .select_first(&selector, |element| element.inner_html())
                        .expect("final page must contain a title");
                    *observed.lock().expect("observed mutex poisoned") =
                        Some((title, ctx.response.redirect_chain.len()));
                    Ok(())
                }
            }
        })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;

    let stats = crawler.run([url("/redirect")]).await?;

    assert_eq!(stats.requests_finished, 1);
    assert_eq!(client.call_count("/redirect"), 1);
    assert_eq!(
        observed.lock().expect("observed mutex poisoned").as_ref(),
        Some(&("Final".to_owned(), 1))
    );
    Ok(())
}

#[tokio::test]
async fn cloning_context_shares_parsed_document() -> Result<(), Box<dyn std::error::Error>> {
    let client = StaticClient::new([html("/clone", "<html><body>shared</body></html>")]);
    let shared = Arc::new(Mutex::new(false));
    let crawler = Crawler::builder(kind(client)?)
        .request_handler({
            let shared = Arc::clone(&shared);
            move |ctx: HtmlContext| {
                let shared = Arc::clone(&shared);
                async move {
                    let cloned = ctx.clone();
                    *shared.lock().expect("shared mutex poisoned") =
                        Arc::ptr_eq(&ctx.html, &cloned.html);
                    Ok(())
                }
            }
        })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;

    let stats = crawler.run([url("/clone")]).await?;

    assert_eq!(stats.requests_finished, 1);
    assert!(*shared.lock().expect("shared mutex poisoned"));
    Ok(())
}
