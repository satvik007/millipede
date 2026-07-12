use crate::{MemoryDataset, MemoryKeyValueStore, MemoryRequestQueue};
use millipede_core::storage::{Dataset, KeyValueStore, RequestQueue, StorageClient, StorageResult};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// An in-process storage client that shares named stores across open calls.
pub struct MemoryStorageClient {
    datasets: Mutex<HashMap<String, Arc<MemoryDataset>>>,
    kv_stores: Mutex<HashMap<String, Arc<MemoryKeyValueStore>>>,
    queues: Mutex<HashMap<String, Arc<dyn RequestQueue>>>,
}

impl MemoryStorageClient {
    /// Creates an empty storage client.
    #[must_use]
    pub fn new() -> Self {
        Self {
            datasets: Mutex::new(HashMap::new()),
            kv_stores: Mutex::new(HashMap::new()),
            queues: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for MemoryStorageClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl StorageClient for MemoryStorageClient {
    async fn open_dataset(&self, name: Option<&str>) -> StorageResult<Arc<dyn Dataset>> {
        let name = name.unwrap_or("default");
        let mut datasets = self.datasets.lock().expect("datasets mutex poisoned");
        Ok(datasets
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(MemoryDataset::new(name)))
            .clone())
    }

    async fn open_key_value_store(
        &self,
        name: Option<&str>,
    ) -> StorageResult<Arc<dyn KeyValueStore>> {
        let name = name.unwrap_or("default");
        let mut stores = self.kv_stores.lock().expect("kv stores mutex poisoned");
        Ok(stores
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(MemoryKeyValueStore::new(name)))
            .clone())
    }

    async fn open_request_queue(&self, name: Option<&str>) -> StorageResult<Arc<dyn RequestQueue>> {
        let name = name.unwrap_or("default");
        let mut queues = self.queues.lock().expect("queues mutex poisoned");
        Ok(queues
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(MemoryRequestQueue::new(name)))
            .clone())
    }

    /// Empties existing handles, then detaches all datasets, stores, and queues.
    async fn purge(&self) -> StorageResult<()> {
        let mut datasets = self.datasets.lock().expect("datasets mutex poisoned");
        for dataset in datasets.values() {
            dataset.clear();
        }
        datasets.clear();

        let mut stores = self.kv_stores.lock().expect("kv stores mutex poisoned");
        for store in stores.values() {
            store.clear();
        }
        stores.clear();
        self.queues.lock().expect("queues mutex poisoned").clear();
        Ok(())
    }
}
