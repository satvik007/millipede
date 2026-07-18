//! Session and session-pool integration tests.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, StatusCode, header::SET_COOKIE};
use millipede_core::{
    http_client::HttpResponse,
    session::{Session, SessionConfig, SessionPool, SessionPoolOptions},
    storage::StorageClient,
};
use millipede_storage_memory::MemoryStorageClient;
use url::Url;

#[tokio::test]
async fn scaled_error_scoring_blocks_and_floors() {
    let session = Session::new(SessionConfig::default());
    session.mark_bad().await;
    session.mark_bad().await;
    session.mark_bad().await;
    assert_eq!(session.error_score().await, 3.0);
    assert!(session.is_blocked().await);
    assert!(!session.is_usable().await);
    session.mark_good().await;
    assert_eq!(session.error_score().await, 2.5);
    for _ in 0..6 {
        session.mark_good().await;
    }
    assert_eq!(session.error_score().await, 0.0);
}

#[tokio::test]
async fn usage_limit_makes_session_unusable() {
    let session = Session::new(SessionConfig::default().with_max_usage_count(2));
    session.record_usage().await;
    session.record_usage().await;
    assert_eq!(session.usage_count().await, 2);
    assert!(!session.is_usable().await);
}

#[tokio::test]
async fn zero_age_expires_immediately() {
    let session = Session::new(SessionConfig::default().with_max_age(Duration::ZERO));
    assert!(session.is_expired());
    assert!(!session.is_usable().await);
}

#[tokio::test]
async fn retirement_and_pool_pruning_work() {
    let pool = SessionPool::new(SessionPoolOptions::default().with_max_pool_size(2));
    let retired = pool.session(None).await;
    let other = pool.session(None).await;
    assert_eq!(pool.session_count().await, 2);
    retired.retire().await;
    assert!(!retired.is_usable().await);
    let fresh = pool.session(None).await;
    assert!(!Arc::ptr_eq(&retired, &fresh));
    assert!(!Arc::ptr_eq(&retired, &other));
    assert!(!Arc::ptr_eq(&fresh, &other));
    assert!(Arc::ptr_eq(&other, &pool.session(Some(other.id())).await));
    assert!(fresh.is_usable().await);
    assert_eq!(pool.session_count().await, 2);
}

#[tokio::test]
async fn pool_grows_to_capacity_then_reuses() {
    let pool = SessionPool::new(SessionPoolOptions::default().with_max_pool_size(2));
    let first = pool.session(None).await;
    let second = pool.session(None).await;
    assert_eq!(pool.session_count().await, 2);
    for _ in 0..8 {
        let reused = pool.session(None).await;
        assert!(Arc::ptr_eq(&reused, &first) || Arc::ptr_eq(&reused, &second));
    }
    assert_eq!(pool.session_count().await, 2);
}

#[tokio::test]
async fn zero_sized_pool_never_retains_sessions() {
    let pool = SessionPool::new(SessionPoolOptions::default().with_max_pool_size(0));
    let first = pool.session(None).await;
    assert!(first.is_usable().await);
    assert_eq!(pool.session_count().await, 0);
    let second = pool.session(None).await;
    assert!(!Arc::ptr_eq(&first, &second));
    assert_eq!(pool.session_count().await, 0);
}

#[tokio::test]
async fn sticky_checkout_reuses_only_usable_session() {
    let pool = SessionPool::new(SessionPoolOptions::default());
    let first = pool.session(None).await;
    let sticky = pool.session(Some(first.id())).await;
    assert!(Arc::ptr_eq(&first, &sticky));
    first.retire().await;
    let replacement = pool.session(Some(first.id())).await;
    assert!(!Arc::ptr_eq(&first, &replacement));
}

#[tokio::test]
async fn persistence_round_trips_score_cookies_and_original_expiry() {
    let client = MemoryStorageClient::new();
    let kvs = client.open_key_value_store(None).await.unwrap();
    let expiring_options = SessionPoolOptions::default()
        .with_session_config(SessionConfig::default().with_max_age(Duration::ZERO));
    let pool = SessionPool::new(expiring_options);
    pool.attach_persistence(Arc::clone(&kvs));
    let session = pool.session(None).await;
    let id = session.id().clone();
    let url = Url::parse("https://example.com/path").unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(SET_COOKIE, HeaderValue::from_static("token=abc; Path=/"));
    session.set_cookies_from_response(&HttpResponse::new(
        url.clone(),
        StatusCode::OK,
        headers,
        Bytes::new(),
    ));
    session.mark_bad().await;
    pool.persist().await.unwrap();

    let restored = SessionPool::new(SessionPoolOptions::default());
    restored.attach_persistence(kvs);
    restored.restore().await.unwrap();
    assert_eq!(restored.session_count().await, 1);
    let replacement = restored.session(Some(&id)).await;
    assert_ne!(
        replacement.id(),
        &id,
        "expired persisted session must not receive a fresh TTL"
    );
    assert!(replacement.is_usable().await);

    let durable = SessionPool::new(SessionPoolOptions::default().with_persist_state_key("durable"));
    let kvs2 = client.open_key_value_store(None).await.unwrap();
    durable.attach_persistence(Arc::clone(&kvs2));
    let original = durable.session(None).await;
    original.set_cookies_from_response(&HttpResponse::new(
        url.clone(),
        StatusCode::OK,
        {
            let mut headers = HeaderMap::new();
            headers.insert(SET_COOKIE, HeaderValue::from_static("token=abc; Path=/"));
            headers
        },
        Bytes::new(),
    ));
    original.mark_bad().await;
    let original_id = original.id().clone();
    durable.persist().await.unwrap();
    let round_trip =
        SessionPool::new(SessionPoolOptions::default().with_persist_state_key("durable"));
    round_trip.attach_persistence(kvs2);
    round_trip.restore().await.unwrap();
    let loaded = round_trip.session(Some(&original_id)).await;
    assert_eq!(loaded.error_score().await, 1.0);
    assert!(
        loaded
            .cookie_jar()
            .cookie_header_for(&url)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("token=abc")
    );
}

#[tokio::test]
async fn user_data_closures_round_trip() {
    let session = Session::new(SessionConfig::default());
    session
        .update_user_data(|data| data.set_typed("answer", &42_u32).unwrap())
        .await;
    let value = session
        .with_user_data(|data| data.get_typed::<u32>("answer").unwrap().unwrap())
        .await;
    assert_eq!(value, 42);
}
