//! Storage wrappers that report backend health to autoscaling.

use std::{path::Path, sync::Arc, time::Duration};

use futures_util::{StreamExt, stream::BoxStream};

use super::{
    AddOptions, BatchAddHandle, Dataset, DatasetInfo, KeyList, KeyValueStore, KvEntry, Lease,
    LeaseId, ListKeysOptions, ListOptions, Page, QueueOpInfo, ReclaimOptions, RequestQueue,
    RequestSource, StorageClient, StorageResult,
};
use crate::{autoscale::ClientLoadSignalHandle, request::Request};

fn observe<T>(handle: &ClientLoadSignalHandle, result: StorageResult<T>) -> StorageResult<T> {
    match &result {
        Ok(_) => handle.record_healthy(),
        Err(error) if error.is_rate_limited() => handle.record_rate_limited(),
        Err(_) => {}
    }
    result
}

/// A storage client wrapper that reports healthy and rate-limited operations to autoscaling.
pub struct RateLimitReportingClient {
    inner: Arc<dyn StorageClient>,
    handle: ClientLoadSignalHandle,
}

impl RateLimitReportingClient {
    /// Wraps `inner` with rate-limit reporting and returns it as a storage client.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        inner: Arc<dyn StorageClient>,
        handle: ClientLoadSignalHandle,
    ) -> Arc<dyn StorageClient> {
        Arc::new(Self { inner, handle })
    }
}

#[async_trait::async_trait]
impl StorageClient for RateLimitReportingClient {
    async fn open_dataset(&self, name: Option<&str>) -> StorageResult<Arc<dyn Dataset>> {
        let result = self.inner.open_dataset(name).await;
        observe(&self.handle, result).map(|inner| {
            Arc::new(ReportingDataset {
                inner,
                handle: self.handle.clone(),
            }) as Arc<dyn Dataset>
        })
    }

    async fn open_key_value_store(
        &self,
        name: Option<&str>,
    ) -> StorageResult<Arc<dyn KeyValueStore>> {
        let result = self.inner.open_key_value_store(name).await;
        observe(&self.handle, result).map(|inner| {
            Arc::new(ReportingKvs {
                inner,
                handle: self.handle.clone(),
            }) as Arc<dyn KeyValueStore>
        })
    }

    async fn open_request_queue(&self, name: Option<&str>) -> StorageResult<Arc<dyn RequestQueue>> {
        let result = self.inner.open_request_queue(name).await;
        observe(&self.handle, result).map(|inner| {
            Arc::new(ReportingQueue {
                inner,
                handle: self.handle.clone(),
            }) as Arc<dyn RequestQueue>
        })
    }

    async fn purge(&self) -> StorageResult<()> {
        let result = self.inner.purge().await;
        observe(&self.handle, result)
    }
}

struct ReportingDataset {
    inner: Arc<dyn Dataset>,
    handle: ClientLoadSignalHandle,
}

#[async_trait::async_trait]
impl Dataset for ReportingDataset {
    async fn push_json(&self, item: serde_json::Value) -> StorageResult<()> {
        let result = self.inner.push_json(item).await;
        observe(&self.handle, result)
    }

    async fn push_json_batch(&self, items: Vec<serde_json::Value>) -> StorageResult<()> {
        let result = self.inner.push_json_batch(items).await;
        observe(&self.handle, result)
    }

    async fn list_raw(&self, opts: ListOptions) -> StorageResult<Page<serde_json::Value>> {
        let result = self.inner.list_raw(opts).await;
        observe(&self.handle, result)
    }

    fn stream_raw(&self, opts: ListOptions) -> BoxStream<'_, StorageResult<serde_json::Value>> {
        let handle = self.handle.clone();
        Box::pin(
            self.inner
                .stream_raw(opts)
                .map(move |result| observe(&handle, result)),
        )
    }

    async fn export_json(&self, path: &Path) -> StorageResult<()> {
        let result = self.inner.export_json(path).await;
        observe(&self.handle, result)
    }

    async fn export_csv(&self, path: &Path) -> StorageResult<()> {
        let result = self.inner.export_csv(path).await;
        observe(&self.handle, result)
    }

    async fn info(&self) -> StorageResult<DatasetInfo> {
        let result = self.inner.info().await;
        observe(&self.handle, result)
    }
}

struct ReportingKvs {
    inner: Arc<dyn KeyValueStore>,
    handle: ClientLoadSignalHandle,
}

#[async_trait::async_trait]
impl KeyValueStore for ReportingKvs {
    async fn get_bytes(&self, key: &str) -> StorageResult<Option<KvEntry>> {
        let result = self.inner.get_bytes(key).await;
        observe(&self.handle, result)
    }

    async fn set_bytes(
        &self,
        key: &str,
        bytes: bytes::Bytes,
        content_type: &str,
    ) -> StorageResult<()> {
        let result = self.inner.set_bytes(key, bytes, content_type).await;
        observe(&self.handle, result)
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        let result = self.inner.delete(key).await;
        observe(&self.handle, result)
    }

    async fn list_keys(&self, opts: ListKeysOptions) -> StorageResult<KeyList> {
        let result = self.inner.list_keys(opts).await;
        observe(&self.handle, result)
    }
}

struct ReportingQueue {
    inner: Arc<dyn RequestQueue>,
    handle: ClientLoadSignalHandle,
}

#[async_trait::async_trait]
impl RequestQueue for ReportingQueue {
    async fn add(&self, req: Request, opts: AddOptions) -> StorageResult<QueueOpInfo> {
        let result = self.inner.add(req, opts).await;
        observe(&self.handle, result)
    }

    async fn add_batch(
        &self,
        reqs: Vec<RequestSource>,
        opts: AddOptions,
    ) -> StorageResult<BatchAddHandle> {
        let result = self.inner.add_batch(reqs, opts).await;
        observe(&self.handle, result)
    }

    async fn fetch_next(&self) -> StorageResult<Option<Lease>> {
        let result = self.inner.fetch_next().await;
        observe(&self.handle, result)
    }

    async fn mark_handled(&self, lease: Lease) -> StorageResult<()> {
        let result = self.inner.mark_handled(lease).await;
        observe(&self.handle, result)
    }

    async fn reclaim(&self, lease: Lease, opts: ReclaimOptions) -> StorageResult<()> {
        let result = self.inner.reclaim(lease, opts).await;
        observe(&self.handle, result)
    }

    async fn renew(&self, lease_id: &LeaseId, extend_by: Duration) -> StorageResult<()> {
        let result = self.inner.renew(lease_id, extend_by).await;
        observe(&self.handle, result)
    }

    async fn abandon(&self, lease: Lease) -> StorageResult<()> {
        let result = self.inner.abandon(lease).await;
        observe(&self.handle, result)
    }

    async fn is_empty(&self) -> StorageResult<bool> {
        let result = self.inner.is_empty().await;
        observe(&self.handle, result)
    }

    async fn is_finished(&self) -> StorageResult<bool> {
        let result = self.inner.is_finished().await;
        observe(&self.handle, result)
    }

    async fn handled_count(&self) -> StorageResult<u64> {
        let result = self.inner.handled_count().await;
        observe(&self.handle, result)
    }

    async fn pending_count(&self) -> StorageResult<u64> {
        let result = self.inner.pending_count().await;
        observe(&self.handle, result)
    }
}
