//! End-to-end routing coverage for HTTP request contexts.

use std::sync::{Arc, Mutex};

use millipede_core::{
    crawler::Crawler,
    errors::CrawlError,
    handler::FailedRequestContext,
    request::{Method, Request},
    router::Router,
};
use millipede_http::{HttpContext, HttpKind};
use millipede_storage_memory::MemoryStorageClient;
use url::Url;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::any};

fn url(server: &MockServer, path: &str) -> Url {
    Url::parse(&format!("{}{path}", server.uri())).expect("mock URL must parse")
}

async fn mount_ok(server: &MockServer) {
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200))
        .mount(server)
        .await;
}

fn request(server: &MockServer, path: &str, label: &str, method: Method) -> Request {
    Request::builder()
        .url(url(server, path))
        .label(label)
        .method(method)
        .build()
        .expect("request must build")
}

#[tokio::test]
async fn http_router_matches_method_lists_and_first_registration_wins()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    mount_ok(&server).await;
    let log = Arc::new(Mutex::new(Vec::new()));
    let router = Router::new()
        .route_methods("page", [Method::GET, Method::HEAD], {
            let log = Arc::clone(&log);
            move |_ctx: HttpContext| {
                let log = Arc::clone(&log);
                async move {
                    log.lock().expect("log mutex poisoned").push("first");
                    Ok(())
                }
            }
        })
        .route_method("page", Method::GET, {
            let log = Arc::clone(&log);
            move |_ctx: HttpContext| {
                let log = Arc::clone(&log);
                async move {
                    log.lock().expect("log mutex poisoned").push("second");
                    Ok(())
                }
            }
        });
    let crawler = Crawler::builder(HttpKind::builder().disable_sessions().build()?)
        .request_handler(router)
        .max_concurrency(1)
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;

    let stats = crawler
        .run([
            request(&server, "/get", "page", Method::GET),
            request(&server, "/head", "page", Method::HEAD),
        ])
        .await?;
    assert_eq!(stats.requests_finished, 2);
    assert_eq!(*log.lock().expect("log mutex poisoned"), ["first", "first"]);
    Ok(())
}

#[tokio::test]
async fn http_missing_route_reaches_failed_request_handler()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    mount_ok(&server).await;
    let observed = Arc::new(Mutex::new(false));
    let router = Router::new().route_methods(
        "page",
        [Method::GET, Method::HEAD],
        |_ctx: HttpContext| async { Ok(()) },
    );
    let crawler = Crawler::builder(HttpKind::builder().disable_sessions().build()?)
        .request_handler(router)
        .failed_request_handler({
            let observed = Arc::clone(&observed);
            move |ctx: FailedRequestContext| {
                let observed = Arc::clone(&observed);
                async move {
                    *observed.lock().expect("observed mutex poisoned") = matches!(
                        ctx.error.as_ref(),
                        CrawlError::MissingRoute {
                            label: Some(label),
                            method: Method::POST,
                        } if label == "page"
                    );
                    Ok(())
                }
            }
        })
        .max_request_retries(0)
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;

    let stats = crawler
        .run([request(&server, "/post", "page", Method::POST)])
        .await?;
    assert_eq!(stats.requests_failed, 1);
    assert!(*observed.lock().expect("observed mutex poisoned"));
    Ok(())
}
