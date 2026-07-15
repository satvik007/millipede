use std::{fmt, sync::Arc};

use super::{Dataset, KeyValueStore, RequestQueue, StorageClient, StorageResult};

/// Open storage resources shared by crawler contexts.
///
/// ```
/// # use std::sync::Arc;
/// # use millipede_core::storage::{Dataset, KeyValueStore, RequestQueue, StorageClient, StorageHandle};
/// # fn build(c: Arc<dyn StorageClient>, d: Arc<dyn Dataset>, k: Arc<dyn KeyValueStore>, q: Arc<dyn RequestQueue>) {
/// let storage = StorageHandle::new(c, d, k, q);
/// let _default_dataset = storage.dataset();
/// # }
/// ```
#[derive(Clone)]
pub struct StorageHandle {
    client: Arc<dyn StorageClient>,
    dataset: Arc<dyn Dataset>,
    key_value_store: Arc<dyn KeyValueStore>,
    request_queue: Arc<dyn RequestQueue>,
}

impl StorageHandle {
    /// Wraps a client and its already-opened default resources.
    pub fn new(
        client: Arc<dyn StorageClient>,
        dataset: Arc<dyn Dataset>,
        key_value_store: Arc<dyn KeyValueStore>,
        request_queue: Arc<dyn RequestQueue>,
    ) -> Self {
        Self {
            client,
            dataset,
            key_value_store,
            request_queue,
        }
    }

    /// Returns the default dataset.
    pub fn dataset(&self) -> &Arc<dyn Dataset> {
        &self.dataset
    }

    /// Returns the default key-value store.
    pub fn key_value_store(&self) -> &Arc<dyn KeyValueStore> {
        &self.key_value_store
    }

    /// Returns the default request queue.
    pub fn request_queue(&self) -> &Arc<dyn RequestQueue> {
        &self.request_queue
    }

    /// Opens a named dataset through the underlying client.
    pub async fn dataset_named(&self, name: &str) -> StorageResult<Arc<dyn Dataset>> {
        self.client.open_dataset(Some(name)).await
    }

    /// Opens a named key-value store through the underlying client.
    pub async fn kvs_named(&self, name: &str) -> StorageResult<Arc<dyn KeyValueStore>> {
        self.client.open_key_value_store(Some(name)).await
    }

    /// Returns the storage backend client.
    pub fn client(&self) -> &Arc<dyn StorageClient> {
        &self.client
    }
}

impl fmt::Debug for StorageHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageHandle")
            .field("client", &"<dyn StorageClient>")
            .field("dataset", &"<dyn Dataset>")
            .field("key_value_store", &"<dyn KeyValueStore>")
            .field("request_queue", &"<dyn RequestQueue>")
            .finish()
    }
}
