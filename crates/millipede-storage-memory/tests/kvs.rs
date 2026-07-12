//! Integration tests for the in-memory key-value store.

use bytes::Bytes;
use millipede_core::storage::{KeyValueStore, KeyValueStoreExt, ListKeysOptions};
use millipede_storage_memory::MemoryKeyValueStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[tokio::test]
async fn bytes_and_typed_values_round_trip() {
    let store: Arc<dyn KeyValueStore> = Arc::new(MemoryKeyValueStore::new("test"));
    store
        .set_bytes("raw", Bytes::from_static(b"hello"), "text/plain")
        .await
        .unwrap();
    let entry = store.get_bytes("raw").await.unwrap().unwrap();
    assert_eq!(entry.value, Bytes::from_static(b"hello"));
    assert_eq!(entry.content_type, "text/plain");

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Value {
        count: u32,
    }
    store.set("typed", &Value { count: 4 }).await.unwrap();
    assert_eq!(
        store.get::<Value>("typed").await.unwrap(),
        Some(Value { count: 4 })
    );
}

#[tokio::test]
async fn delete_is_idempotent() {
    let store = MemoryKeyValueStore::new("test");
    store
        .set_bytes("key", Bytes::from_static(b"value"), "text/plain")
        .await
        .unwrap();
    store.delete("key").await.unwrap();
    store.delete("key").await.unwrap();
    assert!(store.get_bytes("key").await.unwrap().is_none());
}

#[tokio::test]
async fn pagination_walks_sorted_keys_once() {
    let store = MemoryKeyValueStore::new("test");
    for key in ["e", "b", "d", "a", "c"] {
        store
            .set_bytes(key, Bytes::from_static(b"x"), "text/plain")
            .await
            .unwrap();
    }
    let mut start = None;
    let mut all = Vec::new();
    loop {
        let mut options = ListKeysOptions::default();
        options.limit = Some(2);
        options.exclusive_start_key = start;
        let page = store.list_keys(options).await.unwrap();
        all.extend(page.keys.iter().map(|key| key.key.clone()));
        assert!(page.keys.iter().all(|key| key.size == 1));
        if !page.is_truncated {
            assert!(page.next_exclusive_start_key.is_none());
            break;
        }
        assert_eq!(page.keys.len(), 2);
        start = page.next_exclusive_start_key;
    }
    assert_eq!(all, ["a", "b", "c", "d", "e"]);
}

#[tokio::test]
async fn zero_limit_does_not_report_an_unusable_truncated_page() {
    let store = MemoryKeyValueStore::new("test");
    store
        .set_bytes("key", Bytes::from_static(b"value"), "text/plain")
        .await
        .unwrap();
    let mut options = ListKeysOptions::default();
    options.limit = Some(0);

    let page = store.list_keys(options).await.unwrap();

    assert!(page.keys.is_empty());
    assert!(!page.is_truncated);
    assert!(page.next_exclusive_start_key.is_none());
}
