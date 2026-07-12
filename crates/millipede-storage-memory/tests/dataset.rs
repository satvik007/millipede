//! Integration tests for the in-memory dataset.

use futures_util::StreamExt;
use millipede_core::storage::{Dataset, DatasetExt, ListOptions};
use millipede_storage_memory::MemoryDataset;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn lists_items_with_pagination_and_ordering() {
    let ds = MemoryDataset::new("items");
    for value in 0..5 {
        ds.push_json(json!(value)).await.unwrap();
    }
    let page = ds.list_raw(ListOptions::default()).await.unwrap();
    assert_eq!(
        page.items,
        vec![json!(0), json!(1), json!(2), json!(3), json!(4)]
    );
    assert_eq!(page.total, 5);

    let mut options = ListOptions::default();
    options.offset = 1;
    options.limit = Some(2);
    options.desc = true;
    let page = ds.list_raw(options).await.unwrap();
    assert_eq!(page.items, vec![json!(3), json!(2)]);
    assert_eq!(page.total, 5);
    assert_eq!(page.offset, 1);
    assert_eq!(page.limit, Some(2));

    let mut ascending = ListOptions::default();
    ascending.offset = 2;
    ascending.limit = Some(10);
    assert_eq!(
        ds.list_raw(ascending).await.unwrap().items,
        vec![json!(2), json!(3), json!(4)]
    );

    let mut descending = ListOptions::default();
    descending.desc = true;
    assert_eq!(
        ds.list_raw(descending).await.unwrap().items,
        vec![json!(4), json!(3), json!(2), json!(1), json!(0)]
    );
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct MyStruct {
    id: u32,
    label: String,
}

#[tokio::test]
async fn typed_list_and_stream_round_trip_through_dyn_dataset() {
    let ds: Arc<dyn Dataset> = Arc::new(MemoryDataset::new("t"));
    let item = MyStruct {
        id: 7,
        label: "seven".to_owned(),
    };
    ds.push(&item).await.unwrap();
    let page = ds.list::<MyStruct>(ListOptions::default()).await.unwrap();
    assert_eq!(
        page.items,
        vec![MyStruct {
            id: 7,
            label: "seven".to_owned()
        }]
    );
    let streamed: Vec<_> = ds
        .stream::<MyStruct>(ListOptions::default())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(streamed, page.items);
}

#[tokio::test]
async fn exports_json_csv_and_reports_info() {
    let ds = MemoryDataset::new("exports");
    ds.push_json(json!({"a": 1, "b": "plain"})).await.unwrap();
    ds.push_json(json!({"b": "comma, and \"quote\"", "c": true}))
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let json_path = dir.path().join("items.json");
    ds.export_json(&json_path).await.unwrap();
    let exported: serde_json::Value =
        serde_json::from_slice(&std::fs::read(json_path).unwrap()).unwrap();
    assert_eq!(exported.as_array().unwrap().len(), 2);

    let csv_path = dir.path().join("items.csv");
    ds.export_csv(&csv_path).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(csv_path).unwrap(),
        "a,b,c\r\n1,plain,\r\n,\"comma, and \"\"quote\"\"\",true"
    );
    let info = ds.info().await.unwrap();
    assert_eq!(info.name, "exports");
    assert_eq!(info.item_count, 2);
}

#[tokio::test]
async fn csv_rejects_non_object_items() {
    let ds = MemoryDataset::new("invalid");
    ds.push_json(json!(1)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    assert!(ds.export_csv(&dir.path().join("items.csv")).await.is_err());
}
