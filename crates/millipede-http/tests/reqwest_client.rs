//! Integration tests for the concrete reqwest HTTP backend.

use std::{net::TcpListener, sync::Arc, time::Duration};

use futures_util::TryStreamExt;
use http::{HeaderMap, HeaderValue, Method, StatusCode, header::USER_AGENT};
use millipede_core::{
    cookies::CookieJar,
    http_client::{HttpClient, HttpClientError, HttpRequest},
    session::SessionToken,
};
use millipede_http::{ReqwestClient, ReqwestClientOptions};
use url::Url;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{any, header, method, path},
};

fn url(server: &MockServer, path: &str) -> Url {
    Url::parse(&format!("{}{path}", server.uri())).unwrap()
}

#[tokio::test]
async fn follows_redirects_and_records_intermediate_urls() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/a"))
        .respond_with(ResponseTemplate::new(302).insert_header("Location", "/b"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/b"))
        .respond_with(ResponseTemplate::new(303).insert_header("Location", "/c"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/c"))
        .respond_with(ResponseTemplate::new(200).set_body_string("done"))
        .mount(&server)
        .await;

    let response = ReqwestClient::new()
        .unwrap()
        .send(HttpRequest::new(url(&server, "/a")))
        .await
        .unwrap();

    assert_eq!(response.url, url(&server, "/c"));
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body, "done");
    assert_eq!(
        response.redirect_chain,
        vec![url(&server, "/a"), url(&server, "/b")]
    );
}

#[tokio::test]
async fn captures_and_forwards_cookies_from_every_redirect_hop() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/login"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", "/home")
                .insert_header("Set-Cookie", "sid=1; Path=/"),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/home"))
        .and(header("cookie", "sid=1"))
        .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/home"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/profile"))
        .and(header("cookie", "sid=1"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let client = ReqwestClient::new().unwrap();
    let jar = Arc::new(CookieJar::new());
    let login = client
        .send(HttpRequest::new(url(&server, "/login")).cookie_jar(Arc::clone(&jar)))
        .await
        .unwrap();
    assert_eq!(login.status, StatusCode::OK);

    let profile = client
        .send(HttpRequest::new(url(&server, "/profile")).cookie_jar(Arc::clone(&jar)))
        .await
        .unwrap();
    assert_eq!(profile.status, StatusCode::OK);
    assert!(jar.cookie_count() >= 1);
}

#[tokio::test]
async fn see_other_after_post_becomes_get() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/submit"))
        .respond_with(ResponseTemplate::new(303).insert_header("Location", "/result"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/result"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let response = ReqwestClient::new()
        .unwrap()
        .send(HttpRequest::new(url(&server, "/submit")).method(Method::POST))
        .await
        .unwrap();
    assert_eq!(response.status, StatusCode::OK);
}

#[tokio::test]
async fn reports_per_request_timeout() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(500)))
        .mount(&server)
        .await;

    let error = ReqwestClient::new()
        .unwrap()
        .send(HttpRequest::new(url(&server, "/slow")).timeout(Duration::from_millis(100)))
        .await
        .unwrap_err();
    assert!(error.is_timeout());
}

#[tokio::test]
async fn reports_connect_errors() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);

    let error = ReqwestClient::new()
        .unwrap()
        .send(HttpRequest::new(
            Url::parse(&format!("http://{address}/")).unwrap(),
        ))
        .await
        .unwrap_err();
    assert!(error.is_connect());
}

#[tokio::test]
async fn rejects_redirects_beyond_limit() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/loop"))
        .respond_with(ResponseTemplate::new(302).insert_header("Location", "/loop"))
        .mount(&server)
        .await;

    let error = ReqwestClient::new()
        .unwrap()
        .send(HttpRequest::new(url(&server, "/loop")).max_redirects(3))
        .await
        .unwrap_err();
    assert!(matches!(error, HttpClientError::Redirect(_)));
}

#[tokio::test]
async fn routes_each_request_through_its_proxy() {
    let proxy = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_string("proxied"))
        .mount(&proxy)
        .await;

    let response = ReqwestClient::new()
        .unwrap()
        .send(
            HttpRequest::new(Url::parse("http://millipede-proxy-probe.invalid/x").unwrap())
                .proxy(Url::parse(&proxy.uri()).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body, "proxied");
}

#[tokio::test]
async fn preserves_explicit_user_agent_and_applies_default_when_absent() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let client = ReqwestClient::with_options(
        ReqwestClientOptions::default().with_default_user_agent(Some("millipede-test".into())),
    )
    .unwrap();

    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("explicit-agent"));
    client
        .send(HttpRequest::new(url(&server, "/explicit")).headers(headers))
        .await
        .unwrap();
    client
        .send(HttpRequest::new(url(&server, "/default")))
        .await
        .unwrap();

    let requests = server.received_requests().await.unwrap();
    let explicit = requests
        .iter()
        .find(|request| request.url.path() == "/explicit")
        .unwrap();
    let defaulted = requests
        .iter()
        .find(|request| request.url.path() == "/default")
        .unwrap();
    assert_eq!(explicit.headers.get(USER_AGENT).unwrap(), "explicit-agent");
    assert_eq!(defaulted.headers.get(USER_AGENT).unwrap(), "millipede-test");
}

#[tokio::test]
async fn header_generator_applies_deterministic_profile() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let client = ReqwestClient::new().unwrap();

    for path in ["/generated-a", "/generated-b"] {
        client
            .send(
                HttpRequest::new(url(&server, path))
                    .use_header_generator(true)
                    .session_token(SessionToken::new("seed-a")),
            )
            .await
            .unwrap();
    }

    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("explicit-agent"));
    client
        .send(
            HttpRequest::new(url(&server, "/explicit"))
                .headers(headers)
                .use_header_generator(true)
                .session_token(SessionToken::new("seed-a")),
        )
        .await
        .unwrap();

    let requests = server.received_requests().await.unwrap();
    let generated_a = requests
        .iter()
        .find(|request| request.url.path() == "/generated-a")
        .unwrap();
    let generated_b = requests
        .iter()
        .find(|request| request.url.path() == "/generated-b")
        .unwrap();
    let explicit = requests
        .iter()
        .find(|request| request.url.path() == "/explicit")
        .unwrap();

    assert!(
        generated_a
            .headers
            .get("accept-language")
            .is_some_and(|value| !value.as_bytes().is_empty())
    );
    assert_eq!(
        generated_a.headers.get(USER_AGENT),
        generated_b.headers.get(USER_AGENT)
    );
    assert_eq!(explicit.headers.get(USER_AGENT).unwrap(), "explicit-agent");
}

#[tokio::test]
async fn streaming_and_buffered_responses_have_identical_bytes() {
    let server = MockServer::start().await;
    let body = vec![b'x'; 64 * 1024];
    Mock::given(method("GET"))
        .and(path("/big"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body.clone()))
        .mount(&server)
        .await;
    let client = ReqwestClient::new().unwrap();

    let streamed = client
        .stream(HttpRequest::new(url(&server, "/big")))
        .await
        .unwrap()
        .body
        .try_fold(Vec::new(), |mut bytes, chunk| async move {
            bytes.extend_from_slice(&chunk);
            Ok(bytes)
        })
        .await
        .unwrap();
    let buffered = client
        .send(HttpRequest::new(url(&server, "/big")))
        .await
        .unwrap();
    assert_eq!(streamed.as_slice(), buffered.body.as_ref());
    assert_eq!(streamed, body);
}
