//! Integration tests for synchronous cookie storage and persistence.

use http::{HeaderMap, HeaderValue, header::SET_COOKIE};
use millipede_core::cookies::{CookieJar, CookieJarError};
use url::Url;

#[test]
fn stores_cookie_for_matching_origin() {
    let jar = CookieJar::new();
    let url = Url::parse("https://example.com/").unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(SET_COOKIE, HeaderValue::from_static("k=v; Path=/"));

    jar.store_response_cookies(&url, &headers);

    assert_eq!(
        jar.cookie_header_for(&Url::parse("https://example.com/page").unwrap()),
        Some(HeaderValue::from_static("k=v"))
    );
    assert_eq!(
        jar.cookie_header_for(&Url::parse("https://other.example.org/").unwrap()),
        None
    );
}

#[test]
fn stores_multiple_set_cookie_headers() {
    let jar = CookieJar::new();
    let url = Url::parse("https://example.com/").unwrap();
    let mut headers = HeaderMap::new();
    headers.append(SET_COOKIE, HeaderValue::from_static("first=one; Path=/"));
    headers.append(SET_COOKIE, HeaderValue::from_static("second=two; Path=/"));

    jar.store_response_cookies(&url, &headers);

    let value = jar.cookie_header_for(&url).unwrap();
    let value = value.to_str().unwrap();
    assert!(value.contains("first=one"));
    assert!(value.contains("second=two"));
    assert_eq!(jar.cookie_count(), 2);
}

#[test]
fn json_round_trip_and_clear() {
    let jar = CookieJar::new();
    let url = Url::parse("https://example.com/").unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        SET_COOKIE,
        HeaderValue::from_static("session=alive; Path=/"),
    );
    jar.store_response_cookies(&url, &headers);
    let expected = jar.cookie_header_for(&url);

    let restored = CookieJar::from_json(&jar.to_json().unwrap()).unwrap();
    assert_eq!(restored.cookie_header_for(&url), expected);

    restored.clear();
    assert_eq!(restored.cookie_count(), 0);
    assert_eq!(restored.cookie_header_for(&url), None);
}

#[test]
fn session_cookie_without_expiry_survives_json_round_trip() {
    let jar = CookieJar::new();
    let url = Url::parse("https://example.com/").unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(SET_COOKIE, HeaderValue::from_static("token=abc123; Path=/"));
    jar.store_response_cookies(&url, &headers);

    let restored = CookieJar::from_json(&jar.to_json().unwrap()).unwrap();

    assert_eq!(restored.cookie_count(), 1);
    assert_eq!(
        restored.cookie_header_for(&url),
        Some(HeaderValue::from_static("token=abc123"))
    );
}

#[test]
fn deserialization_error_preserves_concrete_source() {
    let error = CookieJar::from_json("not json").unwrap_err();

    let CookieJarError::Deserialize(source) = error else {
        panic!("expected a deserialization error");
    };
    assert!(source.downcast_ref::<serde_json::Error>().is_some());
}
