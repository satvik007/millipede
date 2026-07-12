//! Integration tests for persisted typed state.

use millipede_core::storage::{AutoSaved, KeyValueStore};
use millipede_storage_memory::MemoryKeyValueStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct State {
    count: u32,
}

#[tokio::test]
async fn persisted_value_is_observed_by_a_second_wrapper() {
    let store: Arc<dyn KeyValueStore> = Arc::new(MemoryKeyValueStore::new("state"));
    let saved = AutoSaved::open(store.clone(), "counter", State::default())
        .await
        .unwrap();
    saved.update(|value| value.count += 3).await;
    saved.persist().await.unwrap();

    let reopened = AutoSaved::open(store.clone(), "counter", State::default())
        .await
        .unwrap();
    assert_eq!(reopened.get().await, State { count: 3 });
    assert_eq!(
        store
            .get_bytes("counter")
            .await
            .unwrap()
            .unwrap()
            .content_type,
        "application/json"
    );
}
