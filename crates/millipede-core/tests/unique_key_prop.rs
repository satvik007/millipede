//! Property tests for deterministic request unique keys.

use millipede_core::request::{Method, Request, RequestBody};
use proptest::prelude::*;
use url::Url;

fn input_strategy() -> impl Strategy<Value = (Url, Method, Option<RequestBody>)> {
    (
        "[a-z]{1,10}",
        prop::collection::vec("[a-z]{1,8}", 0..4),
        prop::option::of(prop::collection::vec(("[a-z]{1,6}", "[a-z0-9]{0,6}"), 0..4)),
        0_u8..3,
        prop::option::of(prop::collection::vec(any::<u8>(), 0..32)),
    )
        .prop_map(|(host, segments, query, method_index, body)| {
            let mut url = Url::parse(&format!("https://{host}.com")).unwrap();
            {
                let mut path = url.path_segments_mut().unwrap();
                for segment in segments {
                    path.push(&segment);
                }
            }
            if let Some(query) = query {
                let mut pairs = url.query_pairs_mut();
                for (key, value) in query {
                    pairs.append_pair(&key, &value);
                }
            }
            let method = match method_index {
                0 => Method::GET,
                1 => Method::POST,
                _ => Method::PUT,
            };
            (url, method, body.map(RequestBody::Bytes))
        })
}

proptest! {
    #[test]
    fn unique_key_is_deterministic((url, method, body) in input_strategy()) {
        let first = Request::compute_unique_key(&url, &method, body.as_ref());
        let second = Request::compute_unique_key(&url, &method, body.as_ref());
        prop_assert_eq!(first, second);
    }

    #[test]
    fn fragment_does_not_change_unique_key((url, method, body) in input_strategy()) {
        let mut with_fragment = url.clone();
        with_fragment.set_fragment(Some("ignored"));
        prop_assert_eq!(
            Request::compute_unique_key(&url, &method, body.as_ref()),
            Request::compute_unique_key(&with_fragment, &method, body.as_ref())
        );
    }

    #[test]
    fn non_get_body_change_changes_unique_key(
        (url, method_index, bytes) in (
            input_strategy().prop_map(|(url, _, _)| url),
            0_u8..2,
            prop::collection::vec(any::<u8>(), 0..32),
        )
    ) {
        let method = if method_index == 0 { Method::POST } else { Method::PUT };
        let original = RequestBody::Bytes(bytes.clone());
        let mut changed = bytes;
        changed.push(0xff);
        let changed = RequestBody::Bytes(changed);
        prop_assert_ne!(
            Request::compute_unique_key(&url, &method, Some(&original)),
            Request::compute_unique_key(&url, &method, Some(&changed))
        );
    }
}
