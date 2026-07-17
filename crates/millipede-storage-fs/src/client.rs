use crate::{
    FsDataset, FsKeyValueStore,
    layout::{DATASETS, KEY_VALUE_STORES, REQUEST_QUEUES, store_name, store_path},
};
use millipede_core::storage::{
    Dataset, KeyValueStore, RequestQueue, StorageClient, StorageError, StorageResult,
};
use std::{collections::HashMap, path::Path, path::PathBuf, sync::Arc};
use tokio::sync::{Mutex, RwLock};

/// A file-system storage client using Crawlee-compatible directory layouts.
pub struct FsStorageClient {
    root: PathBuf,
    operations: Arc<RwLock<()>>,
    datasets: Mutex<HashMap<String, Arc<FsDataset>>>,
    kv_stores: Mutex<HashMap<String, Arc<FsKeyValueStore>>>,
}

impl FsStorageClient {
    /// Creates a client rooted at the supplied storage directory.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            operations: Arc::new(RwLock::new(())),
            datasets: Mutex::new(HashMap::new()),
            kv_stores: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the storage root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[async_trait::async_trait]
impl StorageClient for FsStorageClient {
    async fn open_dataset(&self, name: Option<&str>) -> StorageResult<Arc<dyn Dataset>> {
        let name = store_name(name)?;
        let _operation = self.operations.read().await;
        let mut datasets = self.datasets.lock().await;
        if let Some(dataset) = datasets.get(&name) {
            tokio::fs::create_dir_all(store_path(&self.root, DATASETS, &name)).await?;
            return Ok(dataset.clone());
        }

        let path = store_path(&self.root, DATASETS, &name);
        tokio::fs::create_dir_all(&path).await?;
        let dataset =
            Arc::new(FsDataset::open(name.clone(), path, Arc::clone(&self.operations)).await?);
        datasets.insert(name, dataset.clone());
        Ok(dataset)
    }

    async fn open_key_value_store(
        &self,
        name: Option<&str>,
    ) -> StorageResult<Arc<dyn KeyValueStore>> {
        let name = store_name(name)?;
        let _operation = self.operations.read().await;
        let mut stores = self.kv_stores.lock().await;
        if let Some(store) = stores.get(&name) {
            tokio::fs::create_dir_all(store_path(&self.root, KEY_VALUE_STORES, &name)).await?;
            return Ok(store.clone());
        }

        let path = store_path(&self.root, KEY_VALUE_STORES, &name);
        tokio::fs::create_dir_all(&path).await?;
        let store = Arc::new(FsKeyValueStore::open(
            name.clone(),
            path,
            Arc::clone(&self.operations),
        ));
        stores.insert(name, store.clone());
        Ok(store)
    }

    async fn open_request_queue(&self, name: Option<&str>) -> StorageResult<Arc<dyn RequestQueue>> {
        let _ = store_name(name)?;
        let _operation = self.operations.read().await;
        Err(StorageError::Backend(anyhow::anyhow!(
            "FsStorageClient request queue lands later in Phase 5"
        )))
    }

    /// Deletes stored contents and clears the opened dataset and store caches.
    ///
    /// The default key-value store's `INPUT.<ext>` file is preserved for
    /// Crawlee migration parity. This matters when `purge_on_start` uses its
    /// default value of `true`. Previously opened handles remain usable, but
    /// subsequent opens return fresh instances backed by the purged layout.
    async fn purge(&self) -> StorageResult<()> {
        let _operation = self.operations.write().await;
        let mut datasets = self.datasets.lock().await;
        let mut kv_stores = self.kv_stores.lock().await;
        purge_category(&self.root.join(DATASETS), false).await?;
        for dataset in datasets.values() {
            dataset.reset_sequence().await;
        }
        purge_category(&self.root.join(KEY_VALUE_STORES), true).await?;
        purge_category(&self.root.join(REQUEST_QUEUES), false).await?;
        datasets.clear();
        kv_stores.clear();
        Ok(())
    }
}

async fn purge_category(path: &Path, preserve_input: bool) -> StorageResult<()> {
    tokio::fs::create_dir_all(path).await?;
    let mut entries = tokio::fs::read_dir(path).await?;
    while let Some(entry) = entries.next_entry().await? {
        let entry_path = entry.path();
        let file_type = entry.file_type().await?;
        if preserve_input && file_type.is_dir() && entry.file_name() == "default" {
            purge_default_kvs(&entry_path).await?;
        } else if file_type.is_dir() {
            tokio::fs::remove_dir_all(entry_path).await?;
        } else {
            tokio::fs::remove_file(entry_path).await?;
        }
    }
    Ok(())
}

async fn purge_default_kvs(path: &Path) -> StorageResult<()> {
    let mut entries = tokio::fs::read_dir(path).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        let name = entry.file_name();
        let preserve = file_type.is_file()
            && name
                .to_str()
                .and_then(|name| name.rsplit_once('.'))
                .is_some_and(|(key, extension)| key == "INPUT" && !extension.is_empty());
        if preserve {
            continue;
        }
        if file_type.is_dir() {
            tokio::fs::remove_dir_all(entry.path()).await?;
        } else {
            tokio::fs::remove_file(entry.path()).await?;
        }
    }
    Ok(())
}
