use crate::layout::{is_temporary_file, temporary_suffix, validate_key};
use bytes::Bytes;
use millipede_core::storage::{
    KeyInfo, KeyList, KeyValueStore, KvEntry, ListKeysOptions, StorageResult,
};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::sync::{Mutex, RwLock};

/// A file-system-backed byte-oriented key-value store.
pub struct FsKeyValueStore {
    name: String,
    path: PathBuf,
    operations: Arc<RwLock<()>>,
    writes: Mutex<()>,
}

impl FsKeyValueStore {
    pub(crate) fn open(name: String, path: PathBuf, operations: Arc<RwLock<()>>) -> Self {
        Self {
            name,
            path,
            operations,
            writes: Mutex::new(()),
        }
    }

    async fn matching_files(&self, key: &str) -> StorageResult<Vec<(String, PathBuf)>> {
        tokio::fs::create_dir_all(&self.path).await?;
        let mut entries = tokio::fs::read_dir(&self.path).await?;
        let mut matches = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if is_temporary_file(name) {
                continue;
            }
            let Some((stored_key, extension)) = name.rsplit_once('.') else {
                continue;
            };
            if stored_key != key || extension.is_empty() {
                continue;
            }
            matches.push((extension.to_owned(), entry.path()));
        }
        matches.sort_unstable_by(|left, right| left.1.cmp(&right.1));
        Ok(matches)
    }
}

#[async_trait::async_trait]
impl KeyValueStore for FsKeyValueStore {
    async fn get_bytes(&self, key: &str) -> StorageResult<Option<KvEntry>> {
        validate_key(key)?;
        let _operation = self.operations.read().await;
        let _guard = self.writes.lock().await;
        let Some((extension, path)) = self.matching_files(key).await?.into_iter().next() else {
            return Ok(None);
        };
        Ok(Some(KvEntry {
            key: key.to_owned(),
            value: Bytes::from(tokio::fs::read(path).await?),
            content_type: content_type_for_extension(&extension).to_owned(),
        }))
    }

    async fn set_bytes(&self, key: &str, bytes: Bytes, content_type: &str) -> StorageResult<()> {
        validate_key(key)?;
        let _operation = self.operations.read().await;
        let _guard = self.writes.lock().await;
        tokio::fs::create_dir_all(&self.path).await?;
        let extension = extension_for_content_type(content_type);
        let destination = self.path.join(format!("{key}.{extension}"));
        let temporary = self
            .path
            .join(format!("{key}.{extension}.{}", temporary_suffix()));
        tokio::fs::write(&temporary, &bytes).await?;
        if let Err(error) = tokio::fs::rename(&temporary, &destination).await {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(error.into());
        }
        for (_, old_path) in self.matching_files(key).await? {
            if old_path != destination {
                tokio::fs::remove_file(old_path).await?;
            }
        }
        tracing::trace!(store = %self.name, key, content_type, "stored key-value entry");
        Ok(())
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        validate_key(key)?;
        let _operation = self.operations.read().await;
        let _guard = self.writes.lock().await;
        for (_, path) in self.matching_files(key).await? {
            tokio::fs::remove_file(path).await?;
        }
        Ok(())
    }

    /// Lists keys in lexical order after the exclusive cursor.
    ///
    /// A zero limit returns an empty, non-truncated page, matching the memory
    /// backend's pagination semantics.
    async fn list_keys(&self, opts: ListKeysOptions) -> StorageResult<KeyList> {
        let _operation = self.operations.read().await;
        if opts.limit == Some(0) {
            return Ok(KeyList {
                keys: Vec::new(),
                is_truncated: false,
                next_exclusive_start_key: None,
            });
        }

        let _guard = self.writes.lock().await;
        tokio::fs::create_dir_all(&self.path).await?;
        let mut entries = tokio::fs::read_dir(&self.path).await?;
        let mut keys = BTreeMap::new();
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if is_temporary_file(name) {
                continue;
            }
            let Some((key, extension)) = name.rsplit_once('.') else {
                continue;
            };
            if key.is_empty() || extension.is_empty() {
                continue;
            }
            let size = entry.metadata().await?.len();
            keys.entry(key.to_owned()).or_insert(size);
        }

        let start = opts.exclusive_start_key.as_deref();
        let mut filtered = keys
            .into_iter()
            .filter(|(key, _)| start.is_none_or(|start| key.as_str() > start));
        let limit = opts.limit.unwrap_or(usize::MAX);
        let selected: Vec<_> = filtered.by_ref().take(limit).collect();
        let is_truncated = filtered.next().is_some();
        let next_exclusive_start_key = is_truncated
            .then(|| selected.last().map(|(key, _)| key.clone()))
            .flatten();
        Ok(KeyList {
            keys: selected
                .into_iter()
                .map(|(key, size)| KeyInfo { key, size })
                .collect(),
            is_truncated,
            next_exclusive_start_key,
        })
    }
}

fn extension_for_content_type(content_type: &str) -> &'static str {
    match content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "application/json" => "json",
        "text/plain" => "txt",
        "text/html" => "html",
        "application/xml" | "text/xml" => "xml",
        "image/png" => "png",
        "image/jpeg" => "jpeg",
        "application/octet-stream" => "bin",
        _ => "bin",
    }
}

fn content_type_for_extension(extension: &str) -> &'static str {
    match extension.to_ascii_lowercase().as_str() {
        "json" => "application/json",
        "txt" => "text/plain",
        "html" => "text/html",
        "xml" => "application/xml",
        "png" => "image/png",
        "jpeg" => "image/jpeg",
        "bin" => "application/octet-stream",
        _ => "application/octet-stream",
    }
}
