//! Auto-saved typed state integration tests.

use millipede_core::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct MapKvs(Mutex<HashMap<String, KvEntry>>);

#[async_trait::async_trait]
impl KeyValueStore for MapKvs {
    async fn get_bytes(&self, key: &str) -> StorageResult<Option<KvEntry>> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }

    async fn set_bytes(
        &self,
        key: &str,
        bytes: bytes::Bytes,
        content_type: &str,
    ) -> StorageResult<()> {
        self.0.lock().unwrap().insert(
            key.into(),
            KvEntry {
                key: key.into(),
                value: bytes,
                content_type: content_type.into(),
            },
        );
        Ok(())
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }

    async fn list_keys(&self, _: ListKeysOptions) -> StorageResult<KeyList> {
        Ok(KeyList {
            keys: Vec::new(),
            is_truncated: false,
            next_exclusive_start_key: None,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct State {
    count: u32,
}

#[tokio::test]
async fn auto_saved_round_trips_updates_and_replacements() {
    let store: Arc<dyn KeyValueStore> = Arc::new(MapKvs::default());
    let saved = AutoSaved::open(store.clone(), "state", State { count: 1 })
        .await
        .unwrap();
    assert_eq!(saved.get().await, State { count: 1 });

    saved.update(|state| state.count += 1).await;
    saved.persist().await.unwrap();
    let reopened = AutoSaved::open(store.clone(), "state", State { count: 0 })
        .await
        .unwrap();
    assert_eq!(reopened.get().await, State { count: 2 });

    reopened.set(State { count: 9 }).await;
    reopened.persist().await.unwrap();
    let overwritten = AutoSaved::open(store, "state", State { count: 0 })
        .await
        .unwrap();
    assert_eq!(overwritten.get().await, State { count: 9 });
}
