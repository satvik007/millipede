//! Integration tests for backend-independent HTTP client abstractions.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use futures_util::{StreamExt, stream};
use http::{HeaderMap, StatusCode};
use millipede_core::{
    http_client::{
        HttpClient, HttpClientError, HttpRequest, HttpResponse, HttpStatusError, StreamingResponse,
    },
    request::{Method, Request, RequestBody},
};
use url::Url;

struct MockClient;

#[async_trait::async_trait]
impl HttpClient for MockClient {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpClientError> {
        Ok(HttpResponse::new(
            request.url,
            StatusCode::OK,
            HeaderMap::new(),
            Bytes::from_static(b"canned"),
        ))
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

#[tokio::test]
async fn http_client_is_object_safe() {
    let client: Arc<dyn HttpClient> = Arc::new(MockClient);
    let url = Url::parse("https://example.com/").unwrap();
    let response = client.send(HttpRequest::new(url.clone())).await.unwrap();
    assert_eq!(response.url, url);
    assert_eq!(response.body, Bytes::from_static(b"canned"));
}

#[test]
fn http_request_copies_request_fields() {
    let request = Request::post("https://example.com/submit")
        .header("x-test", "yes")
        .body(RequestBody::Bytes(vec![1, 2, 3]))
        .build()
        .unwrap();

    let http_request = HttpRequest::from_request(&request);
    assert_eq!(http_request.url, request.url);
    assert_eq!(http_request.method, Method::POST);
    assert_eq!(http_request.headers, request.headers);
    assert_eq!(http_request.body, request.body);
}

#[test]
fn buffered_response_decodes_text_and_json() {
    let value = serde_json::json!({"answer": 42});
    let body = serde_json::to_vec(&value).unwrap();
    let response = HttpResponse::new(
        Url::parse("https://example.com/data").unwrap(),
        StatusCode::OK,
        HeaderMap::new(),
        Bytes::from(body.clone()),
    );

    assert_eq!(response.text(), String::from_utf8(body).unwrap());
    assert_eq!(response.json::<serde_json::Value>().unwrap(), value);
}

#[tokio::test]
async fn streaming_response_collects_chunks() {
    let chunks = stream::iter([
        Ok(Bytes::from_static(b"hello ")),
        Ok(Bytes::from_static(b"world")),
    ]);
    let response = StreamingResponse::new(
        Url::parse("https://example.com/stream").unwrap(),
        StatusCode::OK,
        HeaderMap::new(),
        Box::pin(chunks),
    );

    let chunks: Vec<Bytes> = response.body.map(Result::unwrap).collect().await;
    assert_eq!(chunks.concat(), b"hello world");
}

#[test]
fn http_status_error_carries_retry_after() {
    let retry_after = Duration::from_secs(2);
    let error = HttpStatusError::new(StatusCode::TOO_MANY_REQUESTS).with_retry_after(retry_after);
    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(error.retry_after, Some(retry_after));
}
