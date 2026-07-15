//! Object-safe storage backend contracts.

mod auto_saved;
mod dataset;
mod handle;
mod kvs;
mod queue;

pub use auto_saved::AutoSaved;
pub use dataset::{Dataset, DatasetExt, DatasetInfo, ListOptions, Page};
pub use handle::StorageHandle;
pub use kvs::{KeyInfo, KeyList, KeyValueStore, KeyValueStoreExt, KvEntry, ListKeysOptions};
pub use queue::{
    AddOptions, AddRequestsBatchedResult, BatchAddHandle, Lease, LeaseId, ProcessedRequest,
    QueueOpInfo, ReclaimOptions, RequestQueue, RequestSource,
};

use std::sync::Arc;

/// An error produced by a storage backend.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageError {
    /// JSON serialization or deserialization failed.
    #[error("serialization: {0}")]
    Serialization(#[from] serde_json::Error),
    /// An input/output operation failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// The referenced lease is no longer active.
    #[error("lease {lease_id} not found (already completed, abandoned, or expired)")]
    LeaseNotFound {
        /// Identifier of the missing lease.
        lease_id: LeaseId,
    },
    /// A backend-specific operation failed.
    #[error("storage backend: {0}")]
    Backend(#[source] anyhow::Error),
    /// The backend does not implement an optional operation.
    #[error("operation not supported by this backend: {0}")]
    Unsupported(&'static str),
}

/// Result type returned by every storage operation.
pub type StorageResult<T> = Result<T, StorageError>;

impl From<StorageError> for crate::errors::CrawlError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Serialization(error) => Self::NonRetryable(anyhow::Error::new(error)),
            StorageError::Io(error) => Self::Retry(anyhow::Error::new(error)),
            error @ StorageError::LeaseNotFound { .. } => {
                Self::NonRetryable(anyhow::Error::new(error))
            }
            StorageError::Backend(error) => Self::Retry(error),
            error @ StorageError::Unsupported(_) => Self::NonRetryable(anyhow::Error::new(error)),
        }
    }
}

/// Opens named or default storage objects supplied by a backend.
#[async_trait::async_trait]
pub trait StorageClient: Send + Sync + 'static {
    /// Opens a dataset, using the default dataset when `name` is `None`.
    async fn open_dataset(&self, name: Option<&str>) -> StorageResult<Arc<dyn Dataset>>;
    /// Opens a key-value store, using the default store when `name` is `None`.
    async fn open_key_value_store(
        &self,
        name: Option<&str>,
    ) -> StorageResult<Arc<dyn KeyValueStore>>;
    /// Opens a request queue, using the default queue when `name` is `None`.
    async fn open_request_queue(&self, name: Option<&str>) -> StorageResult<Arc<dyn RequestQueue>>;
    /// Removes all storage data managed by this client.
    async fn purge(&self) -> StorageResult<()>;
}
