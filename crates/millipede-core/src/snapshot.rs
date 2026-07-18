//! Failure-artifact capture and reload support.
//!
//! Artifacts are captured by crawler kinds on the `HandlerFailed` cleanup path.
//! Execute-time failures that occur before a handler context exists produce no snapshot.

use crate::{
    request::Request,
    storage::{KeyValueStore, StorageResult},
};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
};

/// A failure-time artifact reloaded from storage.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ErrorSnapshot {
    /// MIME content type of the captured artifact.
    pub content_type: String,
    /// Captured artifact bytes.
    pub bytes: bytes::Bytes,
}

/// Captures and reloads failure-time artifacts in a crawler key-value store.
pub struct ErrorSnapshotter {
    kvs: Arc<dyn KeyValueStore>,
}

impl ErrorSnapshotter {
    /// Creates a snapshotter backed by `kvs`.
    pub fn new(kvs: Arc<dyn KeyValueStore>) -> Self {
        Self { kvs }
    }

    /// Returns the deterministic base storage key for `request`.
    pub fn base_key(request: &Request) -> String {
        let mut h = DefaultHasher::new();
        request.unique_key.hash(&mut h);
        format!("ERROR_SNAPSHOT_{:016x}", h.finish())
    }

    /// Captures an artifact under the request base key and `suffix`.
    pub async fn capture(
        &self,
        request: &Request,
        suffix: &str,
        bytes: bytes::Bytes,
        content_type: &str,
    ) -> StorageResult<String> {
        let key = format!("{}.{suffix}", Self::base_key(request));
        self.kvs.set_bytes(&key, bytes, content_type).await?;
        Ok(key)
    }

    /// Reloads a captured artifact by its storage key.
    pub async fn load(&self, key: &str) -> StorageResult<Option<ErrorSnapshot>> {
        Ok(self.kvs.get_bytes(key).await?.map(|entry| ErrorSnapshot {
            content_type: entry.content_type,
            bytes: entry.value,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{ErrorSnapshot, ErrorSnapshotter};
    use crate::storage::{KeyList, KeyValueStore, KvEntry, ListKeysOptions, StorageResult};
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

    fn request(url: &str) -> crate::request::Request {
        crate::request::Request::get(url).build().unwrap()
    }

    #[test]
    fn base_key_is_deterministic_and_request_specific() {
        let first = request("https://example.com/a");
        let same = request("https://example.com/a");
        let different = request("https://example.com/b");

        assert_eq!(
            ErrorSnapshotter::base_key(&first),
            ErrorSnapshotter::base_key(&same)
        );
        assert_ne!(
            ErrorSnapshotter::base_key(&first),
            ErrorSnapshotter::base_key(&different)
        );
    }

    #[tokio::test]
    async fn capture_then_load_round_trips() {
        let snapshotter = ErrorSnapshotter::new(Arc::new(MapKvs::default()));
        let req = request("https://example.com/a");
        let key = snapshotter
            .capture(
                &req,
                "html",
                bytes::Bytes::from_static(b"<html></html>"),
                "text/html",
            )
            .await
            .unwrap();

        assert_eq!(
            snapshotter.load(&key).await.unwrap(),
            Some(ErrorSnapshot {
                content_type: "text/html".into(),
                bytes: bytes::Bytes::from_static(b"<html></html>"),
            })
        );
    }

    #[tokio::test]
    async fn repeated_capture_same_suffix_overwrites() {
        let snapshotter = ErrorSnapshotter::new(Arc::new(MapKvs::default()));
        let req = request("https://example.com/a");
        let first_key = snapshotter
            .capture(
                &req,
                "body",
                bytes::Bytes::from_static(b"first"),
                "text/plain",
            )
            .await
            .unwrap();
        let second_key = snapshotter
            .capture(
                &req,
                "body",
                bytes::Bytes::from_static(b"second"),
                "application/octet-stream",
            )
            .await
            .unwrap();

        assert_eq!(first_key, second_key);
        assert_eq!(
            snapshotter.load(&first_key).await.unwrap(),
            Some(ErrorSnapshot {
                content_type: "application/octet-stream".into(),
                bytes: bytes::Bytes::from_static(b"second"),
            })
        );
    }

    #[tokio::test]
    async fn load_missing_key_returns_none() {
        let snapshotter = ErrorSnapshotter::new(Arc::new(MapKvs::default()));

        assert_eq!(snapshotter.load("missing").await.unwrap(), None);
    }
}
