//! Integration tests for label- and method-based routing.

use std::sync::{Arc, Mutex};

use millipede_core::errors::CrawlError;
use millipede_core::handler::{FailedRequestContext, FailedRequestHandler, RequestHandler};
use millipede_core::request::{Method, Request};
use millipede_core::router::{HasRequest, Router};

struct TestCtx {
    request: Request,
}

impl HasRequest for TestCtx {
    fn request(&self) -> &Request {
        &self.request
    }
}

fn request(label: &str, method: Method) -> TestCtx {
    TestCtx {
        request: Request::get("https://example.com/x")
            .label(label)
            .method(method)
            .build()
            .expect("test request should build"),
    }
}

fn recording_handler(
    trace: Arc<Mutex<Vec<String>>>,
    entry: &'static str,
) -> impl RequestHandler<TestCtx> {
    move |_ctx: TestCtx| {
        let trace = Arc::clone(&trace);
        async move {
            trace.lock().expect("trace lock").push(entry.into());
            Ok(())
        }
    }
}

#[tokio::test]
async fn route_method_dispatches_by_label_and_method() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let router = Router::new()
        .route_method(
            "detail",
            Method::GET,
            recording_handler(Arc::clone(&trace), "get"),
        )
        .route_method(
            "detail",
            Method::POST,
            recording_handler(Arc::clone(&trace), "post"),
        );

    router
        .handle(request("detail", Method::POST))
        .await
        .expect("POST route should succeed");

    assert_eq!(*trace.lock().expect("trace lock"), ["post"]);
}

#[tokio::test]
async fn missing_route_returns_missing_route_error() {
    let router = Router::new().route("listing", |_ctx: TestCtx| async { Ok(()) });

    let error = router
        .handle(request("detail", Method::POST))
        .await
        .expect_err("unmatched request should fail");

    assert!(matches!(
        error,
        CrawlError::MissingRoute {
            label: Some(label),
            method: Method::POST,
        } if label == "detail"
    ));
}

#[tokio::test]
async fn default_handler_is_fallback() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let router = Router::new()
        .route("listing", |_ctx: TestCtx| async { Ok(()) })
        .default(recording_handler(Arc::clone(&trace), "default"));

    router
        .handle(request("detail", Method::GET))
        .await
        .expect("default handler should succeed");

    assert_eq!(*trace.lock().expect("trace lock"), ["default"]);
}

#[tokio::test]
async fn route_methods_accepts_listed_methods_only() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let router = Router::new().route_methods(
        "detail",
        [Method::GET, Method::HEAD],
        recording_handler(Arc::clone(&trace), "matched"),
    );

    router
        .handle(request("detail", Method::HEAD))
        .await
        .expect("HEAD should match");
    let post_error = router
        .handle(request("detail", Method::POST))
        .await
        .expect_err("POST should not match");

    assert!(matches!(post_error, CrawlError::MissingRoute { .. }));
    assert_eq!(*trace.lock().expect("trace lock"), ["matched"]);
}

#[tokio::test]
async fn registration_order_first_match_wins() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let router = Router::new()
        .route("detail", recording_handler(Arc::clone(&trace), "catch-all"))
        .route_method(
            "detail",
            Method::GET,
            recording_handler(Arc::clone(&trace), "get"),
        );

    router
        .handle(request("detail", Method::GET))
        .await
        .expect("route should succeed");

    assert_eq!(*trace.lock().expect("trace lock"), ["catch-all"]);
}

#[tokio::test]
async fn middleware_runs_in_order_and_short_circuits() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let first_trace = Arc::clone(&trace);
    let second_trace = Arc::clone(&trace);
    let router = Router::new()
        .route("detail", recording_handler(Arc::clone(&trace), "handler"))
        .middleware(move |ctx: TestCtx| {
            let trace = Arc::clone(&first_trace);
            async move {
                trace.lock().expect("trace lock").push("first".into());
                Ok(ctx)
            }
        })
        .middleware(move |ctx: TestCtx| {
            let trace = Arc::clone(&second_trace);
            async move {
                trace.lock().expect("trace lock").push("second".into());
                Ok(ctx)
            }
        });

    router
        .handle(request("detail", Method::GET))
        .await
        .expect("middleware chain should succeed");
    assert_eq!(
        *trace.lock().expect("trace lock"),
        ["first", "second", "handler"]
    );

    let short_trace = Arc::new(Mutex::new(Vec::new()));
    let middleware_trace = Arc::clone(&short_trace);
    let router = Router::new()
        .route(
            "detail",
            recording_handler(Arc::clone(&short_trace), "handler"),
        )
        .middleware(move |_ctx: TestCtx| {
            let trace = Arc::clone(&middleware_trace);
            async move {
                trace.lock().expect("trace lock").push("middleware".into());
                Err(CrawlError::non_retryable(anyhow::anyhow!("stop")))
            }
        });

    let error = router
        .handle(request("detail", Method::GET))
        .await
        .expect_err("middleware error should propagate");
    assert!(matches!(error, CrawlError::NonRetryable(_)));
    assert_eq!(*short_trace.lock().expect("trace lock"), ["middleware"]);
}

#[tokio::test]
async fn closures_are_handlers() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let handler_trace = Arc::clone(&trace);
    let router = Router::new().route("detail", move |_ctx: TestCtx| {
        let trace = Arc::clone(&handler_trace);
        async move {
            trace.lock().expect("trace lock").push("request");
            Ok(())
        }
    });
    router
        .handle(request("detail", Method::GET))
        .await
        .expect("closure route should run");

    let failed_trace = Arc::clone(&trace);
    let failed = move |_ctx: FailedRequestContext| {
        let trace = Arc::clone(&failed_trace);
        async move {
            trace.lock().expect("trace lock").push("failed request");
            Ok(())
        }
    };
    let failed_context = FailedRequestContext::new(
        Arc::new(request("detail", Method::GET).request),
        Arc::new(CrawlError::non_retryable(anyhow::anyhow!("failed"))),
        2,
    );
    FailedRequestHandler::handle(&failed, failed_context)
        .await
        .expect("closure failure handler should run");

    assert_eq!(
        *trace.lock().expect("trace lock"),
        ["request", "failed request"]
    );
}
