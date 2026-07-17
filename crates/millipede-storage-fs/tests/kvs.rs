//! Integration tests for the file-system key-value store.

use bytes::Bytes;
use millipede_core::storage::{ListKeysOptions, StorageClient};
use millipede_storage_fs::FsStorageClient;

#[tokio::test]
async fn extensions_and_content_types_round_trip() {
    let root = tempfile::tempdir().unwrap();
    let client = FsStorageClient::new(root.path());
    let store = client.open_key_value_store(None).await.unwrap();
    let cases = [
        ("json", "application/json; charset=utf-8", "json"),
        ("text", "text/plain", "txt"),
        ("html", "text/html", "html"),
        ("binary", "unknown/type", "bin"),
    ];
    for (key, content_type, extension) in cases {
        store
            .set_bytes(key, Bytes::from_static(b"value"), content_type)
            .await
            .unwrap();
        assert!(
            root.path()
                .join(format!("key_value_stores/default/{key}.{extension}"))
                .is_file()
        );
        let entry = store.get_bytes(key).await.unwrap().unwrap();
        let expected = match extension {
            "json" => "application/json",
            "txt" => "text/plain",
            "html" => "text/html",
            _ => "application/octet-stream",
        };
        assert_eq!(entry.content_type, expected);
        assert_eq!(entry.value, Bytes::from_static(b"value"));
    }
}

#[tokio::test]
async fn crawlee_unknown_extension_is_discoverable_and_deletable() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("key_value_stores/default");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("INPUT.tmp-custom"), b"interop").unwrap();
    let client = FsStorageClient::new(root.path());
    let store = client.open_key_value_store(None).await.unwrap();

    let entry = store.get_bytes("INPUT").await.unwrap().unwrap();
    assert_eq!(entry.value, Bytes::from_static(b"interop"));
    assert_eq!(entry.content_type, "application/octet-stream");
    let listed = store.list_keys(ListKeysOptions::default()).await.unwrap();
    assert_eq!(listed.keys.len(), 1);
    assert_eq!(listed.keys[0].key, "INPUT");

    store.delete("INPUT").await.unwrap();
    assert!(!directory.join("INPUT.tmp-custom").exists());
}

#[tokio::test]
async fn in_flight_atomic_temp_file_is_ignored() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("key_value_stores/default");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("record.json"), b"committed").unwrap();
    std::fs::write(
        directory.join("record.json.tmp-0123456789abcdef"),
        b"partial",
    )
    .unwrap();
    let store = FsStorageClient::new(root.path())
        .open_key_value_store(None)
        .await
        .unwrap();

    assert_eq!(
        store.get_bytes("record").await.unwrap().unwrap().value,
        Bytes::from_static(b"committed")
    );
    let listed = store.list_keys(ListKeysOptions::default()).await.unwrap();
    assert_eq!(listed.keys.len(), 1);
    assert_eq!(listed.keys[0].key, "record");
}

#[tokio::test]
async fn overwrite_with_new_content_type_leaves_one_file() {
    let root = tempfile::tempdir().unwrap();
    let client = FsStorageClient::new(root.path());
    let store = client.open_key_value_store(None).await.unwrap();
    store
        .set_bytes("record", Bytes::from_static(b"{}"), "application/json")
        .await
        .unwrap();
    store
        .set_bytes("record", Bytes::from_static(b"new"), "text/plain")
        .await
        .unwrap();

    let files: Vec<_> = std::fs::read_dir(root.path().join("key_value_stores/default"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(files, ["record.txt"]);
    assert_eq!(
        store.get_bytes("record").await.unwrap().unwrap().value,
        Bytes::from_static(b"new")
    );
}

#[tokio::test]
async fn list_delete_and_sanitization_match_memory_semantics() {
    let root = tempfile::tempdir().unwrap();
    let client = FsStorageClient::new(root.path());
    let store = client.open_key_value_store(None).await.unwrap();
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
        start = page.next_exclusive_start_key;
    }
    assert_eq!(all, ["a", "b", "c", "d", "e"]);

    store.delete("c").await.unwrap();
    store.delete("c").await.unwrap();
    assert!(store.get_bytes("c").await.unwrap().is_none());
    assert!(store.get_bytes("../evil").await.is_err());

    let mut zero = ListKeysOptions::default();
    zero.limit = Some(0);
    let page = store.list_keys(zero).await.unwrap();
    assert!(page.keys.is_empty());
    assert!(!page.is_truncated);
    assert!(page.next_exclusive_start_key.is_none());
}
