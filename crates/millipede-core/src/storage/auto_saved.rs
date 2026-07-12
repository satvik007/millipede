//! Typed persisted state wrapper.

use super::{KeyValueStore, KeyValueStoreExt, StorageResult};
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt, sync::Arc};

/// A typed value that can be explicitly persisted to a key-value store.
///
/// The engine calls [`Self::persist`] on every `PersistState` event when Phase 2 wires events to
/// storage.
pub struct AutoSaved<T> {
    store: Arc<dyn KeyValueStore>,
    key: String,
    value: tokio::sync::RwLock<T>,
}

impl<T> fmt::Debug for AutoSaved<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AutoSaved")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl<T: Serialize + DeserializeOwned + Send + Sync + 'static> AutoSaved<T> {
    /// Opens a persisted value, falling back to `default` when the key is absent.
    pub async fn open(
        store: Arc<dyn KeyValueStore>,
        key: impl Into<String>,
        default: T,
    ) -> StorageResult<Self> {
        let key = key.into();
        let value = store.get(&key).await?.unwrap_or(default);
        Ok(Self {
            store,
            key,
            value: tokio::sync::RwLock::new(value),
        })
    }

    /// Clones and returns the current value.
    pub async fn get(&self) -> T
    where
        T: Clone,
    {
        self.value.read().await.clone()
    }

    /// Replaces the current in-memory value without persisting it.
    pub async fn set(&self, value: T) {
        *self.value.write().await = value;
    }

    /// Mutates the current in-memory value without persisting it.
    pub async fn update<F: FnOnce(&mut T) + Send>(&self, f: F) {
        f(&mut *self.value.write().await);
    }

    /// Serializes and persists the current value as JSON.
    pub async fn persist(&self) -> StorageResult<()> {
        let bytes = serde_json::to_vec(&*self.value.read().await)?;
        self.store
            .set_bytes(&self.key, bytes.into(), "application/json")
            .await
    }
}
