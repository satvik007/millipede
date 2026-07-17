//! Integration tests for link-extraction types and pure URL policy logic.

use millipede_core::{
    enqueue::SkipReason,
    link_extraction::{
        CrawlPolicy, EnqueueStrategy, GlobPattern, SkippedHandler, UrlMatch, strategy_allows,
    },
    request::{HeaderMap, Method, UserData},
};
use regex::Regex;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use url::Url;

fn url(value: &str) -> Url {
    Url::parse(value).expect("test URL should parse")
}

#[test]
fn strategy_matrix_covers_origin_hostname_and_non_http_schemes() {
    let http = url("http://example.com/path");
    let https = url("https://example.com/path");
    let alternate_port = url("http://example.com:8080/path");

    assert!(strategy_allows(
        EnqueueStrategy::SameHostname,
        &http,
        &https
    ));
    assert!(!strategy_allows(EnqueueStrategy::SameOrigin, &http, &https));
    assert!(!strategy_allows(
        EnqueueStrategy::SameOrigin,
        &http,
        &alternate_port
    ));
    for strategy in [
        EnqueueStrategy::All,
        EnqueueStrategy::SameHostname,
        EnqueueStrategy::SameDomain,
        EnqueueStrategy::SameOrigin,
    ] {
        assert!(!strategy_allows(
            strategy,
            &http,
            &url("mailto:user@example.com")
        ));
        assert!(!strategy_allows(
            strategy,
            &http,
            &url("javascript:void(0)")
        ));
    }
}

#[test]
fn same_domain_uses_the_public_suffix_list() {
    let category = url("https://sub.example.co.uk/category");

    assert!(strategy_allows(
        EnqueueStrategy::SameDomain,
        &category,
        &url("https://shop.example.co.uk/product")
    ));
    assert!(!strategy_allows(
        EnqueueStrategy::SameDomain,
        &category,
        &url("https://other.co.uk/product")
    ));
}

#[test]
fn same_domain_falls_back_to_exact_host_for_non_registrable_hosts() {
    assert!(strategy_allows(
        EnqueueStrategy::SameDomain,
        &url("http://127.0.0.1/start"),
        &url("https://127.0.0.1/end")
    ));
    assert!(!strategy_allows(
        EnqueueStrategy::SameDomain,
        &url("http://127.0.0.1/start"),
        &url("http://127.0.0.2/end")
    ));
    assert!(strategy_allows(
        EnqueueStrategy::SameDomain,
        &url("http://localhost/start"),
        &url("https://localhost/end")
    ));
    assert!(!strategy_allows(
        EnqueueStrategy::SameDomain,
        &url("http://localhost/start"),
        &url("https://otherhost/end")
    ));
}

#[test]
fn same_domain_rejects_unrelated_ipv4_hosts_with_matching_psl_domains() {
    assert!(!strategy_allows(
        EnqueueStrategy::SameDomain,
        &url("http://10.1.0.1/start"),
        &url("http://192.168.0.1/end")
    ));
}

#[test]
fn url_match_fluent_builder_carries_all_overrides() {
    let mut user_data = UserData::default();
    user_data
        .set_typed("source", &"listing")
        .expect("serializable test data");
    let mut headers = HeaderMap::new();
    headers.insert("x-source", "listing".parse().expect("valid header value"));

    let matched = UrlMatch::new("**/products/*")
        .label("product")
        .user_data(user_data)
        .method(Method::POST)
        .headers(headers);

    assert_eq!(matched.label.as_deref(), Some("product"));
    assert_eq!(
        matched
            .user_data
            .as_ref()
            .and_then(|data| data.get("source")),
        Some(&serde_json::json!("listing"))
    );
    assert_eq!(matched.method, Some(Method::POST));
    assert_eq!(
        matched.headers.as_ref().and_then(|map| map.get("x-source")),
        Some(&"listing".parse().expect("valid header value"))
    );
}

#[test]
fn regex_converts_directly_into_glob_pattern() {
    let _: GlobPattern = Regex::new(r"/products/\\d+$")
        .expect("valid regular expression")
        .into();
}

#[test]
fn crawl_policy_defaults_and_closure_handler_work() {
    let default = CrawlPolicy::default();
    assert_eq!(default.strategy, EnqueueStrategy::SameHostname);
    assert_eq!(default.max_crawl_depth, None);
    assert_eq!(default.max_requests_per_crawl, None);
    assert!(default.on_skipped.is_none());

    let called = Arc::new(AtomicBool::new(false));
    let called_by_handler = Arc::clone(&called);
    let policy = CrawlPolicy::new().on_skipped(move |url: &str, reason: &SkipReason| {
        assert_eq!(url, "bad:url");
        assert_eq!(reason, &SkipReason::InvalidUrl);
        called_by_handler.store(true, Ordering::SeqCst);
    });

    policy
        .on_skipped
        .as_ref()
        .expect("handler should be configured")
        .on_skip("bad:url", &SkipReason::InvalidUrl);
    assert!(called.load(Ordering::SeqCst));

    fn assert_handler<T: SkippedHandler>(_handler: &T) {}
    assert_handler(&|_: &str, _: &SkipReason| {});
}
