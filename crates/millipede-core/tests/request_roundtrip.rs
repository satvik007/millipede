//! Integration coverage for complete request JSON round-tripping.

use millipede_core::request::{HeaderMap, Request, RequestState, UserData};
use serde_json::json;
use time::macros::datetime;

#[test]
fn maximal_request_roundtrips_through_json() {
    let mut user_data = UserData::default();
    user_data.set_typed("page", &3_u32).unwrap();

    let mut request = Request::post("https://example.com/items#source")
        .json(&json!({"kind": "maximal", "active": true}))
        .header("set-cookie", "a=1")
        .header("set-cookie", "b=2")
        .user_data(user_data)
        .label("items")
        .max_retries(7)
        .no_retry(true)
        .skip_navigation(true)
        .crawl_depth(4)
        .build()
        .unwrap();
    request.loaded_url = Some("https://example.com/items/final".parse().unwrap());
    request.retry_count = 2;
    request.session_rotation_count = 1;
    request.error_messages = vec!["transient".to_owned()];
    request.handled_at = Some(datetime!(2026-07-12 12:34:56 UTC));
    request.state = RequestState::Done;

    let encoded = serde_json::to_string(&request).unwrap();
    let decoded: Request = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded, request);
    assert_eq!(decoded.headers.get_all("set-cookie").iter().count(), 2);
    let _: &HeaderMap = &decoded.headers;
}
