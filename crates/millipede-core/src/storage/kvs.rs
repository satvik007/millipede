//! Key-value storage contracts.

use super::StorageResult;
use serde::{Serialize, de::DeserializeOwned};

/// A stored byte value and its metadata.
#[derive(Debug, Clone)]
pub struct KvEntry {
    /// Storage key.
    pub key: String,
    /// Stored bytes.
    pub value: bytes::Bytes,
    /// MIME content type.
    pub content_type: String,
}

/// Options controlling key pagination.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
#[must_use = "list options do nothing unless passed to KeyValueStore::list_keys"]
pub struct ListKeysOptions {
    /// Maximum number of keys to return.
    pub limit: Option<usize>,
    /// Key after which listing begins.
    pub exclusive_start_key: Option<String>,
}

/// Metadata for one stored key.
#[derive(Debug, Clone)]
pub struct KeyInfo {
    /// Storage key.
    pub key: String,
    /// Value size in bytes.
    pub size: u64,
}

/// A page of keys and continuation metadata.
#[derive(Debug, Clone)]
pub struct KeyList {
    /// Keys in this page.
    pub keys: Vec<KeyInfo>,
    /// Whether more keys remain.
    pub is_truncated: bool,
    /// Continuation key for the next page.
    pub next_exclusive_start_key: Option<String>,
}

/// Object-safe byte-oriented key-value storage.
#[async_trait::async_trait]
pub trait KeyValueStore: Send + Sync {
    /// Gets a stored byte value.
    async fn get_bytes(&self, key: &str) -> StorageResult<Option<KvEntry>>;
    /// Sets a stored byte value and content type.
    async fn set_bytes(
        &self,
        key: &str,
        bytes: bytes::Bytes,
        content_type: &str,
    ) -> StorageResult<()>;
    /// Deletes a key, doing nothing when it is absent.
    async fn delete(&self, key: &str) -> StorageResult<()>;
    /// Lists stored keys.
    async fn list_keys(&self, opts: ListKeysOptions) -> StorageResult<KeyList>;
}

/// Typed JSON convenience operations available on every [`KeyValueStore`].
#[async_trait::async_trait]
pub trait KeyValueStoreExt: KeyValueStore {
    /// Gets and deserializes a JSON value.
    async fn get<T: DeserializeOwned + 'static>(&self, key: &str) -> StorageResult<Option<T>> {
        match self.get_bytes(key).await? {
            Some(entry) => Ok(Some(serde_json::from_slice(&entry.value)?)),
            None => Ok(None),
        }
    }

    /// Serializes and stores a JSON value.
    async fn set<T: Serialize + Send + Sync>(&self, key: &str, value: &T) -> StorageResult<()> {
        self.set_bytes(key, serde_json::to_vec(value)?.into(), "application/json")
            .await
    }
}

impl<K: KeyValueStore + ?Sized> KeyValueStoreExt for K {}
