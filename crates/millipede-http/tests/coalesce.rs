//! Integration tests for in-flight request coalescing.

use std::{sync::Arc, time::Duration};

use http::Method;
use millipede_core::{
    cookies::CookieJar,
    http_client::{HttpClient, HttpRequest},
};
use millipede_http::{CoalescingClient, ReqwestClient};
use url::Url;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

fn url(server: &MockServer) -> Url {
    Url::parse(&format!("{}/slow", server.uri())).unwrap()
}

#[tokio::test]
async fn identical_gets_share_one_upstream_request() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/slow"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(100))
                .set_body_string("shared"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let client = CoalescingClient::new(Arc::new(ReqwestClient::new().unwrap()));

    let (left, right) = tokio::join!(
        client.send(HttpRequest::new(url(&server))),
        client.send(HttpRequest::new(url(&server)))
    );
    assert_eq!(left.unwrap().body, "shared");
    assert_eq!(right.unwrap().body, "shared");
    server.verify().await;
}

#[tokio::test]
async fn different_cookie_jars_do_not_share_requests() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/slow"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(100)))
        .expect(2)
        .mount(&server)
        .await;
    let client = CoalescingClient::new(Arc::new(ReqwestClient::new().unwrap()));

    let first = HttpRequest::new(url(&server)).cookie_jar(Arc::new(CookieJar::new()));
    let second = HttpRequest::new(url(&server)).cookie_jar(Arc::new(CookieJar::new()));
    let (left, right) = tokio::join!(client.send(first), client.send(second));
    assert!(left.is_ok());
    assert!(right.is_ok());
    server.verify().await;
}

#[tokio::test]
async fn posts_are_never_coalesced() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/slow"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(100)))
        .expect(2)
        .mount(&server)
        .await;
    let client = CoalescingClient::new(Arc::new(ReqwestClient::new().unwrap()));

    let first = HttpRequest::new(url(&server)).method(Method::POST);
    let second = HttpRequest::new(url(&server)).method(Method::POST);
    let (left, right) = tokio::join!(client.send(first), client.send(second));
    assert!(left.is_ok());
    assert!(right.is_ok());
    server.verify().await;
}

#[tokio::test]
async fn solo_timeout_preserves_typed_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/slow"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(500)))
        .mount(&server)
        .await;
    let client = CoalescingClient::new(Arc::new(ReqwestClient::new().unwrap()));

    let error = client
        .send(HttpRequest::new(url(&server)).timeout(Duration::from_millis(50)))
        .await
        .unwrap_err();

    assert!(error.is_timeout());
}

#[tokio::test]
async fn cancelled_leader_promotes_follower_and_refetches() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/slow"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(500)))
        .expect(2)
        .mount(&server)
        .await;
    let client = Arc::new(CoalescingClient::new(Arc::new(
        ReqwestClient::new().unwrap(),
    )));

    let leader_client = Arc::clone(&client);
    let leader_url = url(&server);
    let leader =
        tokio::spawn(async move { leader_client.send(HttpRequest::new(leader_url)).await });
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
    .expect("the leader request should reach the mock server");

    let follower_client = Arc::clone(&client);
    let follower_url = url(&server);
    let follower =
        tokio::spawn(async move { follower_client.send(HttpRequest::new(follower_url)).await });
    // Give the follower a turn to attach to the leader's OnceCell before the
    // initializer is cancelled. `OnceCell::get_or_init` cancellation is safe,
    // but it does not preserve the cancelled initializer: the promoted follower
    // runs its own per-caller init future and therefore makes a second upstream
    // request. This re-fetch is the intentional cancellation-safety trade-off.
    tokio::task::yield_now().await;

    leader.abort();
    assert!(leader.await.unwrap_err().is_cancelled());

    let newcomer = tokio::time::timeout(
        Duration::from_secs(2),
        client.send(HttpRequest::new(url(&server))),
    )
    .await
    .expect("a new caller should join the promoted follower")
    .unwrap();
    let follower = follower.await.unwrap().unwrap();
    assert_eq!(follower.status, http::StatusCode::OK);
    assert_eq!(newcomer.status, http::StatusCode::OK);
    server.verify().await;
}
