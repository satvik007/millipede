//! Progress test proving cookie locks are not held across network awaits.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use http::{HeaderMap, HeaderValue, header::SET_COOKIE};
use millipede_core::{
    cookies::CookieJar,
    http_client::{HttpClient, HttpRequest},
};
use millipede_http::ReqwestClient;
use url::Url;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cookie_lock_not_held_across_await() {
    // Run with RUSTFLAGS="--cfg tokio_unstable" to attach tokio-console instrumentation.
    #[cfg(tokio_unstable)]
    {
        console_subscriber::ConsoleLayer::builder().init();
    }

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/slow"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(500))
                .insert_header("Set-Cookie", "delayed=1; Path=/"),
        )
        .mount(&server)
        .await;

    let url = Url::parse(&format!("{}/slow", server.uri())).unwrap();
    let other_url = Url::parse(&format!("{}/other", server.uri())).unwrap();
    let jar = Arc::new(CookieJar::new());
    let task_jar = Arc::clone(&jar);
    let task_url = url.clone();
    let fetch = tokio::spawn(async move {
        ReqwestClient::new()
            .unwrap()
            .send(HttpRequest::new(task_url).cookie_jar(task_jar))
            .await
    });

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let received = server.received_requests().await.unwrap();
            if received.iter().any(|request| request.url.path() == "/slow") {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the delayed request should reach the mock server");
    let mut headers = HeaderMap::new();
    headers.insert(SET_COOKIE, HeaderValue::from_static("local=1; Path=/"));
    let started = Instant::now();
    for _ in 0..50 {
        let _ = jar.cookie_header_for(&url);
        jar.store_response_cookies(&other_url, &headers);
    }
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "cookie operations were blocked for {:?}",
        started.elapsed()
    );

    assert!(fetch.await.unwrap().is_ok());
    let cookies = jar.cookie_header_for(&url).unwrap();
    assert!(cookies.to_str().unwrap().contains("delayed=1"));
}
