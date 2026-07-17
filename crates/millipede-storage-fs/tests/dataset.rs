//! Integration tests for the file-system dataset.

use futures_util::StreamExt;
use millipede_core::storage::{Dataset, DatasetExt, ListOptions, StorageClient};
use millipede_storage_fs::FsStorageClient;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn uses_exact_layout_and_pretty_printed_items() {
    let root = tempfile::tempdir().unwrap();
    let client = FsStorageClient::new(root.path());
    let dataset = client.open_dataset(None).await.unwrap();
    let first = json!({"id": 1, "label": "one"});
    let second = json!({"id": 2});
    dataset.push_json(first.clone()).await.unwrap();
    dataset.push_json(second.clone()).await.unwrap();

    let directory = root.path().join("datasets/default");
    assert_eq!(
        std::fs::read(directory.join("000000001.json")).unwrap(),
        serde_json::to_vec_pretty(&first).unwrap()
    );
    assert_eq!(
        std::fs::read(directory.join("000000002.json")).unwrap(),
        serde_json::to_vec_pretty(&second).unwrap()
    );
}

#[tokio::test]
async fn reopening_continues_after_existing_sequence() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("datasets/default");
    std::fs::create_dir_all(&directory).unwrap();
    for sequence in 1..=3 {
        std::fs::write(
            directory.join(format!("{sequence:09}.json")),
            serde_json::to_vec_pretty(&json!({"id": sequence})).unwrap(),
        )
        .unwrap();
    }

    let client = FsStorageClient::new(root.path());
    client
        .open_dataset(None)
        .await
        .unwrap()
        .push_json(json!({"id": 4}))
        .await
        .unwrap();
    assert!(directory.join("000000004.json").is_file());
}

#[tokio::test]
async fn crash_leftover_atomic_temp_item_is_ignored() {
    let root = tempfile::tempdir().unwrap();
    let client = FsStorageClient::new(root.path());
    let dataset = client.open_dataset(None).await.unwrap();
    dataset.push_json(json!({"id": 1})).await.unwrap();
    let directory = root.path().join("datasets/default");
    std::fs::write(
        directory.join("000000002.json.tmp-0123456789abcdef"),
        br#"{"truncated":"#,
    )
    .unwrap();

    assert_eq!(
        dataset
            .list_raw(ListOptions::default())
            .await
            .unwrap()
            .items,
        vec![json!({"id": 1})]
    );
    drop(dataset);
    drop(client);

    let reopened = FsStorageClient::new(root.path())
        .open_dataset(None)
        .await
        .unwrap();
    reopened.push_json(json!({"id": 2})).await.unwrap();
    assert!(directory.join("000000002.json").is_file());
}

#[tokio::test]
async fn list_stream_and_exports_match_memory_semantics() {
    let root = tempfile::tempdir().unwrap();
    let client = FsStorageClient::new(root.path());
    let dataset = client.open_dataset(Some("exports")).await.unwrap();
    for value in 0..5 {
        dataset.push_json(json!(value)).await.unwrap();
    }

    let mut descending_page = ListOptions::default();
    descending_page.offset = 1;
    descending_page.limit = Some(2);
    descending_page.desc = true;
    let mut offset_only = ListOptions::default();
    offset_only.offset = 2;
    let mut oversized_limit = ListOptions::default();
    oversized_limit.offset = 2;
    oversized_limit.limit = Some(10);
    let mut descending = ListOptions::default();
    descending.desc = true;

    let cases = [
        (
            ListOptions::default(),
            vec![json!(0), json!(1), json!(2), json!(3), json!(4)],
        ),
        (descending_page, vec![json!(3), json!(2)]),
        (offset_only, vec![json!(2), json!(3), json!(4)]),
        (oversized_limit, vec![json!(2), json!(3), json!(4)]),
        (
            descending,
            vec![json!(4), json!(3), json!(2), json!(1), json!(0)],
        ),
    ];
    for (options, expected) in cases {
        let page = dataset.list_raw(options.clone()).await.unwrap();
        assert_eq!(page.items, expected);
        assert_eq!(page.total, 5);
        assert_eq!(page.offset, options.offset);
        assert_eq!(page.limit, options.limit);
        let streamed = dataset
            .stream_raw(options)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(streamed, page.items);
    }

    let object_dataset = client.open_dataset(Some("objects")).await.unwrap();
    object_dataset
        .push_json(json!({"a": 1, "b": "plain"}))
        .await
        .unwrap();
    object_dataset
        .push_json(json!({"b": "comma, and \"quote\"", "c": true}))
        .await
        .unwrap();
    let json_path = root.path().join("export.json");
    object_dataset.export_json(&json_path).await.unwrap();
    let expected = vec![
        json!({"a": 1, "b": "plain"}),
        json!({"b": "comma, and \"quote\"", "c": true}),
    ];
    assert_eq!(
        std::fs::read(json_path).unwrap(),
        serde_json::to_vec_pretty(&expected).unwrap()
    );
    let csv_path = root.path().join("export.csv");
    object_dataset.export_csv(&csv_path).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(csv_path).unwrap(),
        "a,b,c\r\n1,plain,\r\n,\"comma, and \"\"quote\"\"\",true"
    );
    let info = object_dataset.info().await.unwrap();
    assert_eq!(info.name, "objects");
    assert_eq!(info.item_count, 2);
    assert!(info.modified_at >= info.created_at);
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct MyStruct {
    id: u32,
    label: String,
}

#[tokio::test]
async fn typed_list_and_stream_round_trip_through_dyn_dataset() {
    let root = tempfile::tempdir().unwrap();
    let client = FsStorageClient::new(root.path());
    let dataset: Arc<dyn Dataset> = client.open_dataset(Some("typed")).await.unwrap();
    let item = MyStruct {
        id: 7,
        label: "seven".to_owned(),
    };
    dataset.push(&item).await.unwrap();
    let page = dataset
        .list::<MyStruct>(ListOptions::default())
        .await
        .unwrap();
    let streamed = dataset
        .stream::<MyStruct>(ListOptions::default())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(streamed, page.items);
    assert_eq!(
        page.items,
        vec![MyStruct {
            id: 7,
            label: "seven".to_owned(),
        }]
    );
}

#[tokio::test]
async fn corrupt_item_does_not_break_reads_or_exports() {
    let root = tempfile::tempdir().unwrap();
    let client = FsStorageClient::new(root.path());
    let dataset = client.open_dataset(Some("corrupt")).await.unwrap();
    dataset.push_json(json!({"id": 1})).await.unwrap();
    std::fs::write(
        root.path().join("datasets/corrupt/000000009.json"),
        br#"{"truncated":"#,
    )
    .unwrap();
    dataset.push_json(json!({"id": 2})).await.unwrap();

    let expected = vec![json!({"id": 1}), json!({"id": 2})];
    assert_eq!(
        dataset
            .list_raw(ListOptions::default())
            .await
            .unwrap()
            .items,
        expected
    );
    let streamed = dataset
        .stream_raw(ListOptions::default())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(streamed, expected);

    let json_path = root.path().join("corrupt-export.json");
    dataset.export_json(&json_path).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&std::fs::read(json_path).unwrap()).unwrap(),
        json!([{"id": 1}, {"id": 2}])
    );
    let csv_path = root.path().join("corrupt-export.csv");
    dataset.export_csv(&csv_path).await.unwrap();
    assert_eq!(std::fs::read_to_string(csv_path).unwrap(), "id\r\n1\r\n2");
    assert_eq!(dataset.info().await.unwrap().item_count, 3);
}

#[tokio::test]
async fn csv_rejects_non_object_items() {
    let root = tempfile::tempdir().unwrap();
    let client = FsStorageClient::new(root.path());
    let dataset = client.open_dataset(None).await.unwrap();
    dataset.push_json(json!(1)).await.unwrap();
    assert!(
        dataset
            .export_csv(&root.path().join("items.csv"))
            .await
            .is_err()
    );
}
