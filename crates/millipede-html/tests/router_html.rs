//! End-to-end routing coverage for HTML request contexts.

use std::sync::{Arc, Mutex};

use millipede_core::{
    crawler::Crawler,
    errors::CrawlError,
    handler::FailedRequestContext,
    request::{Method, Request},
    router::Router,
};
use millipede_html::{HtmlContext, HtmlKind};
use millipede_storage_memory::MemoryStorageClient;
use url::Url;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::any};

fn url(server: &MockServer, path: &str) -> Url {
    Url::parse(&format!("{}{path}", server.uri())).expect("mock URL must parse")
}

async fn mount_html(server: &MockServer) {
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_raw("<title>route</title>", "text/html"))
        .mount(server)
        .await;
}

fn request(server: &MockServer, path: &str, label: Option<&str>, method: Method) -> Request {
    let mut builder = Request::builder().url(url(server, path)).method(method);
    if let Some(label) = label {
        builder = builder.label(label);
    }
    builder.build().expect("request must build")
}

#[tokio::test]
async fn html_router_dispatches_defaults_and_middleware_in_order()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    mount_html(&server).await;
    let log = Arc::new(Mutex::new(Vec::new()));
    let router = Router::new()
        .route("list", {
            let log = Arc::clone(&log);
            move |_ctx: HtmlContext| {
                let log = Arc::clone(&log);
                async move {
                    log.lock().expect("log mutex poisoned").push("list");
                    Ok(())
                }
            }
        })
        .route_method("detail", Method::GET, {
            let log = Arc::clone(&log);
            move |_ctx: HtmlContext| {
                let log = Arc::clone(&log);
                async move {
                    log.lock().expect("log mutex poisoned").push("detail");
                    Ok(())
                }
            }
        })
        .default({
            let log = Arc::clone(&log);
            move |_ctx: HtmlContext| {
                let log = Arc::clone(&log);
                async move {
                    log.lock().expect("log mutex poisoned").push("default");
                    Ok(())
                }
            }
        })
        .middleware({
            let log = Arc::clone(&log);
            move |ctx: HtmlContext| {
                let log = Arc::clone(&log);
                async move {
                    log.lock().expect("log mutex poisoned").push("middleware");
                    Ok(ctx)
                }
            }
        });
    let crawler = Crawler::builder(HtmlKind::builder().disable_sessions().build()?)
        .request_handler(router)
        .max_concurrency(1)
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;

    let stats = crawler
        .run([
            request(&server, "/list", Some("list"), Method::GET),
            request(&server, "/detail", Some("detail"), Method::GET),
            request(&server, "/default", None, Method::GET),
        ])
        .await?;
    assert_eq!(stats.requests_finished, 3);
    assert_eq!(
        *log.lock().expect("log mutex poisoned"),
        [
            "middleware",
            "list",
            "middleware",
            "detail",
            "middleware",
            "default",
        ]
    );
    Ok(())
}

#[tokio::test]
async fn html_missing_method_route_reaches_failure_handler()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    mount_html(&server).await;
    let failure = Arc::new(Mutex::new(None));
    let router =
        Router::new().route_method("detail", Method::GET, |_ctx: HtmlContext| async { Ok(()) });
    let crawler = Crawler::builder(HtmlKind::builder().disable_sessions().build()?)
        .request_handler(router)
        .failed_request_handler({
            let failure = Arc::clone(&failure);
            move |ctx: FailedRequestContext| {
                let failure = Arc::clone(&failure);
                async move {
                    let missing = matches!(
                        ctx.error.as_ref(),
                        CrawlError::MissingRoute {
                            label: Some(label),
                            method: Method::POST,
                        } if label == "detail"
                    );
                    *failure.lock().expect("failure mutex poisoned") = Some(missing);
                    Ok(())
                }
            }
        })
        .max_request_retries(0)
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;

    let stats = crawler
        .run([request(&server, "/detail", Some("detail"), Method::POST)])
        .await?;
    assert_eq!(stats.requests_failed, 1);
    assert_eq!(*failure.lock().expect("failure mutex poisoned"), Some(true));
    Ok(())
}
