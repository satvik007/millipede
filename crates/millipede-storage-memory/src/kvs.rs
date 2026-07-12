use bytes::Bytes;
use millipede_core::storage::{
    KeyInfo, KeyList, KeyValueStore, KvEntry, ListKeysOptions, StorageResult,
};
use std::{collections::HashMap, sync::Mutex};

/// An in-process byte-oriented key-value store.
pub struct MemoryKeyValueStore {
    name: String,
    inner: Mutex<HashMap<String, KvEntry>>,
}

impl MemoryKeyValueStore {
    /// Creates an empty key-value store with the supplied name.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            inner: Mutex::new(HashMap::new()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, KvEntry>> {
        let _store_name = &self.name;
        // A panic while holding this lock is a programming bug, so poisoning is unrecoverable.
        self.inner
            .lock()
            .expect("MemoryKeyValueStore mutex poisoned")
    }

    pub(crate) fn clear(&self) {
        self.lock().clear();
    }
}

#[async_trait::async_trait]
impl KeyValueStore for MemoryKeyValueStore {
    async fn get_bytes(&self, key: &str) -> StorageResult<Option<KvEntry>> {
        Ok(self.lock().get(key).cloned())
    }

    async fn set_bytes(&self, key: &str, value: Bytes, content_type: &str) -> StorageResult<()> {
        self.lock().insert(
            key.to_owned(),
            KvEntry {
                key: key.to_owned(),
                value,
                content_type: content_type.to_owned(),
            },
        );
        Ok(())
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        self.lock().remove(key);
        Ok(())
    }

    /// Lists keys in lexical order after the exclusive cursor.
    ///
    /// A zero limit returns an empty, non-truncated page. This avoids claiming
    /// that a caller can continue when the page contains no key to use as its
    /// next exclusive cursor.
    async fn list_keys(&self, opts: ListKeysOptions) -> StorageResult<KeyList> {
        if opts.limit == Some(0) {
            return Ok(KeyList {
                keys: Vec::new(),
                is_truncated: false,
                next_exclusive_start_key: None,
            });
        }

        let entries = self.lock();
        let mut keys: Vec<_> = entries.keys().cloned().collect();
        keys.sort_unstable();
        let start = opts.exclusive_start_key.as_deref();
        let mut filtered = keys
            .into_iter()
            .filter(|key| start.is_none_or(|start| key.as_str() > start));
        let limit = opts.limit.unwrap_or(usize::MAX);
        let selected: Vec<_> = filtered.by_ref().take(limit).collect();
        let is_truncated = filtered.next().is_some();
        let next_exclusive_start_key = is_truncated.then(|| selected.last().cloned()).flatten();
        let keys = selected
            .into_iter()
            .map(|key| KeyInfo {
                size: entries.get(&key).expect("selected key exists").value.len() as u64,
                key,
            })
            .collect();
        Ok(KeyList {
            keys,
            is_truncated,
            next_exclusive_start_key,
        })
    }
}
