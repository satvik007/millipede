//! Dataset storage contracts.

use super::{StorageError, StorageResult};
use futures_util::{StreamExt, stream::BoxStream};
use serde::{Serialize, de::DeserializeOwned};

/// Options controlling dataset listing order and pagination.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
#[must_use = "list options do nothing unless passed to Dataset::list"]
pub struct ListOptions {
    /// Number of items to skip.
    pub offset: u64,
    /// Maximum number of items to return.
    pub limit: Option<u64>,
    /// Whether to list newest items first.
    pub desc: bool,
}

/// A page of dataset items and pagination metadata.
#[derive(Debug, Clone)]
pub struct Page<T> {
    /// Items in this page.
    pub items: Vec<T>,
    /// Total number of items in the dataset.
    pub total: u64,
    /// Offset used for this page.
    pub offset: u64,
    /// Limit used for this page.
    pub limit: Option<u64>,
}

/// Dataset identity and timestamps.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DatasetInfo {
    /// Dataset name.
    pub name: String,
    /// Number of stored items.
    pub item_count: u64,
    /// Creation timestamp.
    pub created_at: time::OffsetDateTime,
    /// Most recent modification timestamp.
    pub modified_at: time::OffsetDateTime,
}

impl DatasetInfo {
    /// Creates dataset metadata for a storage backend.
    pub fn new(
        name: String,
        item_count: u64,
        created_at: time::OffsetDateTime,
        modified_at: time::OffsetDateTime,
    ) -> Self {
        Self {
            name,
            item_count,
            created_at,
            modified_at,
        }
    }
}

/// Object-safe storage for append-only JSON records.
#[async_trait::async_trait]
pub trait Dataset: Send + Sync {
    /// Appends one raw JSON value.
    async fn push_json(&self, item: serde_json::Value) -> StorageResult<()>;
    /// Appends a batch of raw JSON values.
    async fn push_json_batch(&self, items: Vec<serde_json::Value>) -> StorageResult<()>;
    /// Lists raw JSON values according to the supplied options.
    async fn list_raw(&self, opts: ListOptions) -> StorageResult<Page<serde_json::Value>>;
    /// Streams raw JSON values according to the supplied options.
    fn stream_raw(&self, opts: ListOptions) -> BoxStream<'_, StorageResult<serde_json::Value>>;
    /// Exports the complete dataset as JSON.
    async fn export_json(&self, path: &std::path::Path) -> StorageResult<()>;
    /// Exports the complete dataset as CSV.
    async fn export_csv(&self, path: &std::path::Path) -> StorageResult<()>;
    /// Returns dataset metadata.
    async fn info(&self) -> StorageResult<DatasetInfo>;
}

/// Typed convenience operations available on every [`Dataset`].
#[async_trait::async_trait]
pub trait DatasetExt: Dataset {
    /// Serializes and appends one item.
    async fn push<T: Serialize + Send + Sync>(&self, item: &T) -> StorageResult<()> {
        self.push_json(serde_json::to_value(item)?).await
    }

    /// Serializes and appends a batch of items.
    async fn push_batch<T: Serialize + Send + Sync>(&self, items: &[T]) -> StorageResult<()> {
        let items = items
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?;
        self.push_json_batch(items).await
    }

    /// Lists and deserializes typed items.
    async fn list<T: DeserializeOwned>(&self, opts: ListOptions) -> StorageResult<Page<T>> {
        let page = self.list_raw(opts).await?;
        Ok(Page {
            items: page
                .items
                .into_iter()
                .map(serde_json::from_value)
                .collect::<Result<Vec<_>, _>>()?,
            total: page.total,
            offset: page.offset,
            limit: page.limit,
        })
    }

    /// Streams and deserializes typed items.
    fn stream<T: DeserializeOwned + Send + 'static>(
        &self,
        opts: ListOptions,
    ) -> BoxStream<'_, StorageResult<T>> {
        Box::pin(self.stream_raw(opts).map(|result| {
            result.and_then(|value| serde_json::from_value(value).map_err(StorageError::from))
        }))
    }
}

impl<D: Dataset + ?Sized> DatasetExt for D {}
