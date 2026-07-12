//! Integration tests for the in-memory storage client.

use bytes::Bytes;
use millipede_core::storage::{Dataset, ListOptions, StorageClient, StorageError};
use millipede_storage_memory::MemoryStorageClient;
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn named_handles_are_reused_and_isolated() {
    let client = MemoryStorageClient::new();
    let first = client.open_dataset(None).await.unwrap();
    let second = client.open_dataset(None).await.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    first.push_json(json!({"visible": true})).await.unwrap();
    assert_eq!(
        second.list_raw(ListOptions::default()).await.unwrap().total,
        1
    );

    let a = client.open_dataset(Some("a")).await.unwrap();
    let b = client.open_dataset(Some("b")).await.unwrap();
    assert!(!Arc::ptr_eq(&a, &b));
}

#[tokio::test]
async fn request_queue_is_not_supported_yet() {
    let error = match MemoryStorageClient::new().open_request_queue(None).await {
        Ok(_) => panic!("request queue unexpectedly supported"),
        Err(error) => error,
    };
    assert!(matches!(error, StorageError::Unsupported(_)));
}

#[tokio::test]
async fn purge_empties_existing_dataset_and_store_handles() {
    let client = MemoryStorageClient::new();
    let dataset = client.open_dataset(None).await.unwrap();
    let store = client.open_key_value_store(None).await.unwrap();
    dataset.push_json(json!(1)).await.unwrap();
    store
        .set_bytes("key", Bytes::from_static(b"value"), "text/plain")
        .await
        .unwrap();
    client.purge().await.unwrap();
    assert_eq!(dataset.info().await.unwrap().item_count, 0);
    assert!(store.get_bytes("key").await.unwrap().is_none());
}

#[allow(dead_code)]
fn _assert_dataset_object_safe(_: Arc<dyn Dataset>) {}
