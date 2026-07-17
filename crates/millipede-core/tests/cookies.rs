//! Integration tests for synchronous cookie storage and persistence.

use http::{HeaderMap, HeaderValue, header::SET_COOKIE};
use millipede_core::cookies::{Cookie, CookieJar, CookieJarError, SameSite};
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

#[test]
fn structured_cookie_round_trip_preserves_fields() {
    let jar = CookieJar::new();
    let mut host_only = Cookie::new("session", "host-value", "example.com");
    host_only.host_only = true;
    host_only.path = "/account".to_owned();

    let expires = time::macros::datetime!(2035-06-01 12:30:45 UTC);
    let mut domained = Cookie::new("persistent", "domain-value", "example.com");
    domained.path = "/catalog".to_owned();
    domained.expires = Some(expires);
    domained.secure = true;
    domained.http_only = true;
    domained.same_site = Some(SameSite::Lax);

    assert_eq!(
        jar.import_cookies(&[host_only.clone(), domained.clone()]),
        2
    );
    let exported = jar.export_cookies();
    let exported_host = exported
        .iter()
        .find(|cookie| cookie.name == host_only.name)
        .expect("host-only cookie should be exported");
    assert_eq!(exported_host, &host_only);

    let exported_domain = exported
        .iter()
        .find(|cookie| cookie.name == domained.name)
        .expect("domain cookie should be exported");
    assert_eq!(exported_domain.name, domained.name);
    assert_eq!(exported_domain.value, domained.value);
    assert_eq!(exported_domain.domain, domained.domain);
    assert_eq!(exported_domain.host_only, domained.host_only);
    assert_eq!(exported_domain.path, domained.path);
    assert_eq!(exported_domain.secure, domained.secure);
    assert_eq!(exported_domain.http_only, domained.http_only);
    assert_eq!(exported_domain.same_site, domained.same_site);
    let exported_expiry = exported_domain
        .expires
        .expect("persistent cookie should retain its expiry");
    assert!((exported_expiry - expires).abs() <= time::Duration::SECOND);
}

#[test]
fn import_counts_stored_cookies_and_skips_empty_names() {
    let jar = CookieJar::new();
    let valid = Cookie::new("valid", "value", "example.com");
    let invalid = Cookie::new("", "value", "example.com");

    assert_eq!(jar.import_cookies(&[valid, invalid]), 1);
    assert_eq!(jar.cookie_count(), 1);
}

#[test]
fn export_omits_already_expired_imported_cookie() {
    let jar = CookieJar::new();
    let mut expired = Cookie::new("expired", "value", "example.com");
    expired.expires = Some(time::OffsetDateTime::now_utc() - time::Duration::DAY);

    let _ = jar.import_cookies(&[expired]);

    assert!(jar.export_cookies().is_empty());
}

#[test]
fn imported_cookie_is_visible_to_matching_https_request() {
    let jar = CookieJar::new();
    let mut cookie = Cookie::new("browser", "shared", "example.com");
    cookie.secure = true;
    cookie.path = "/account".to_owned();

    assert_eq!(jar.import_cookies(&[cookie]), 1);
    assert_eq!(
        jar.cookie_header_for(&Url::parse("https://example.com/account/profile").unwrap()),
        Some(HeaderValue::from_static("browser=shared"))
    );
}
