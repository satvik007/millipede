//! Integration tests for the file-system storage client.

use bytes::Bytes;
use millipede_core::storage::{ListOptions, StorageClient};
use millipede_storage_fs::FsStorageClient;
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn opens_crawlee_shaped_storage() {
    let root = tempfile::tempdir().unwrap();
    let dataset_path = root.path().join("datasets/default");
    let kvs_path = root.path().join("key_value_stores/default");
    std::fs::create_dir_all(&dataset_path).unwrap();
    std::fs::create_dir_all(&kvs_path).unwrap();
    std::fs::write(
        dataset_path.join("000000001.json"),
        serde_json::to_vec_pretty(&json!({"from": "crawlee"})).unwrap(),
    )
    .unwrap();
    std::fs::write(kvs_path.join("INPUT.json"), br#"{"start":true}"#).unwrap();

    let client = FsStorageClient::new(root.path());
    let dataset = client.open_dataset(None).await.unwrap();
    assert_eq!(
        dataset
            .list_raw(ListOptions::default())
            .await
            .unwrap()
            .items,
        vec![json!({"from": "crawlee"})]
    );
    let input = client
        .open_key_value_store(None)
        .await
        .unwrap()
        .get_bytes("INPUT")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(input.value, Bytes::from_static(br#"{"start":true}"#));
    assert_eq!(input.content_type, "application/json");
}

#[tokio::test]
async fn purge_preserves_only_default_input() {
    let root = tempfile::tempdir().unwrap();
    let client = FsStorageClient::new(root.path());
    let dataset = client.open_dataset(None).await.unwrap();
    dataset.push_json(json!({"discard": true})).await.unwrap();
    let kvs = client.open_key_value_store(None).await.unwrap();
    kvs.set_bytes("INPUT", Bytes::from_static(b"{}"), "application/json")
        .await
        .unwrap();
    kvs.set_bytes("OUTPUT", Bytes::from_static(b"gone"), "text/plain")
        .await
        .unwrap();
    let named = client.open_key_value_store(Some("named")).await.unwrap();
    named
        .set_bytes("INPUT", Bytes::from_static(b"gone"), "text/plain")
        .await
        .unwrap();

    client.purge().await.unwrap();

    assert!(
        client
            .open_dataset(None)
            .await
            .unwrap()
            .list_raw(ListOptions::default())
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(
        root.path()
            .join("key_value_stores/default/INPUT.json")
            .is_file()
    );
    assert!(
        !root
            .path()
            .join("key_value_stores/default/OUTPUT.txt")
            .exists()
    );
    assert!(!root.path().join("key_value_stores/named").exists());
}

#[tokio::test]
async fn held_handles_remain_writable_and_reopens_are_fresh_after_purge() {
    let root = tempfile::tempdir().unwrap();
    let client = FsStorageClient::new(root.path());
    let dataset = client.open_dataset(Some("held")).await.unwrap();
    let kvs = client.open_key_value_store(Some("held")).await.unwrap();
    dataset.push_json(json!({"before": true})).await.unwrap();
    kvs.set_bytes("before", Bytes::from_static(b"before"), "text/plain")
        .await
        .unwrap();

    client.purge().await.unwrap();

    assert!(!root.path().join("datasets/held").exists());
    assert!(!root.path().join("key_value_stores/held").exists());

    dataset.push_json(json!({"after": true})).await.unwrap();
    kvs.set_bytes("after", Bytes::from_static(b"after"), "text/plain")
        .await
        .unwrap();
    assert_eq!(
        dataset
            .list_raw(ListOptions::default())
            .await
            .unwrap()
            .items,
        vec![json!({"after": true})]
    );
    assert_eq!(
        kvs.get_bytes("after").await.unwrap().unwrap().value,
        Bytes::from_static(b"after")
    );
    assert_eq!(
        client
            .open_dataset(Some("held"))
            .await
            .unwrap()
            .list_raw(ListOptions::default())
            .await
            .unwrap()
            .items,
        vec![json!({"after": true})]
    );
    let reopened = client.open_dataset(Some("held")).await.unwrap();
    let reopened_kvs = client.open_key_value_store(Some("held")).await.unwrap();
    assert!(!Arc::ptr_eq(&dataset, &reopened));
    assert!(!Arc::ptr_eq(&kvs, &reopened_kvs));
    assert_eq!(
        reopened_kvs
            .get_bytes("after")
            .await
            .unwrap()
            .unwrap()
            .value,
        Bytes::from_static(b"after")
    );
    reopened
        .push_json(json!({"reopened_handle": true}))
        .await
        .unwrap();
    assert_eq!(
        reopened
            .list_raw(ListOptions::default())
            .await
            .unwrap()
            .items,
        vec![json!({"after": true}), json!({"reopened_handle": true})]
    );
    assert!(root.path().join("datasets/held/000000001.json").is_file());
    assert!(root.path().join("datasets/held/000000002.json").is_file());
}

#[tokio::test]
async fn purge_waits_for_in_flight_dataset_batch() {
    let root = tempfile::tempdir().unwrap();
    let client = Arc::new(FsStorageClient::new(root.path()));
    let dataset = client.open_dataset(Some("racing")).await.unwrap();
    let items = (0..2_000).map(|value| json!({"value": value})).collect();

    let writer = tokio::spawn(async move { dataset.push_json_batch(items).await.unwrap() });
    let first_item = root.path().join("datasets/racing/000000001.json");
    while !first_item.exists() {
        tokio::task::yield_now().await;
    }
    let purge_client = Arc::clone(&client);
    let purger = tokio::spawn(async move { purge_client.purge().await.unwrap() });

    writer.await.unwrap();
    purger.await.unwrap();
    assert!(!root.path().join("datasets/racing").exists());
}

#[tokio::test]
async fn rejects_unsafe_storage_names() {
    let root = tempfile::tempdir().unwrap();
    let client = FsStorageClient::new(root.path());
    assert!(client.open_dataset(Some("../evil")).await.is_err());
    assert!(client.open_key_value_store(Some("../evil")).await.is_err());
    assert!(client.open_request_queue(Some("../evil")).await.is_err());
    assert!(!root.path().join("evil").exists());
}

#[tokio::test]
async fn request_queue_reports_phase_placeholder() {
    let root = tempfile::tempdir().unwrap();
    let error = FsStorageClient::new(root.path())
        .open_request_queue(None)
        .await
        .err()
        .expect("request queue is not implemented yet");
    assert!(error.to_string().contains("lands later in Phase 5"));
}
