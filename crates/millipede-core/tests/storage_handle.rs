//! Storage-handle delegation and sharing integration tests.

use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{stream, stream::BoxStream};
use millipede_core::{
    request::Request,
    storage::{
        AddOptions, BatchAddHandle, Dataset, DatasetInfo, KeyList, KeyValueStore, KvEntry, Lease,
        LeaseId, ListKeysOptions, ListOptions, Page, QueueOpInfo, ReclaimOptions, RequestQueue,
        StorageClient, StorageHandle, StorageResult,
    },
};

struct FakeDataset;
#[async_trait::async_trait]
impl Dataset for FakeDataset {
    async fn push_json(&self, _: serde_json::Value) -> StorageResult<()> {
        Ok(())
    }
    async fn push_json_batch(&self, _: Vec<serde_json::Value>) -> StorageResult<()> {
        Ok(())
    }
    async fn list_raw(&self, opts: ListOptions) -> StorageResult<Page<serde_json::Value>> {
        Ok(Page {
            items: Vec::new(),
            total: 0,
            offset: opts.offset,
            limit: opts.limit,
        })
    }
    fn stream_raw(&self, _: ListOptions) -> BoxStream<'_, StorageResult<serde_json::Value>> {
        Box::pin(stream::empty())
    }
    async fn export_json(&self, _: &Path) -> StorageResult<()> {
        Ok(())
    }
    async fn export_csv(&self, _: &Path) -> StorageResult<()> {
        Ok(())
    }
    async fn info(&self) -> StorageResult<DatasetInfo> {
        let now = time::OffsetDateTime::now_utc();
        Ok(DatasetInfo::new("fake".into(), 0, now, now))
    }
}

struct FakeKvs;
#[async_trait::async_trait]
impl KeyValueStore for FakeKvs {
    async fn get_bytes(&self, _: &str) -> StorageResult<Option<KvEntry>> {
        Ok(None)
    }
    async fn set_bytes(&self, _: &str, _: bytes::Bytes, _: &str) -> StorageResult<()> {
        Ok(())
    }
    async fn delete(&self, _: &str) -> StorageResult<()> {
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

struct FakeQueue;
#[async_trait::async_trait]
impl RequestQueue for FakeQueue {
    async fn add(&self, req: Request, _: AddOptions) -> StorageResult<QueueOpInfo> {
        Ok(millipede_core::storage::ProcessedRequest {
            request_id: req.id,
            unique_key: req.unique_key,
            was_already_present: false,
            was_already_handled: false,
        })
    }
    async fn add_batch(
        &self,
        _: Vec<millipede_core::storage::RequestSource>,
        _: AddOptions,
    ) -> StorageResult<BatchAddHandle> {
        Ok(BatchAddHandle::ready(Vec::new()))
    }
    async fn fetch_next(&self) -> StorageResult<Option<Lease>> {
        Ok(None)
    }
    async fn mark_handled(&self, _: Lease) -> StorageResult<()> {
        Ok(())
    }
    async fn reclaim(&self, _: Lease, _: ReclaimOptions) -> StorageResult<()> {
        Ok(())
    }
    async fn renew(&self, _: &LeaseId, _: Duration) -> StorageResult<()> {
        Ok(())
    }
    async fn abandon(&self, _: Lease) -> StorageResult<()> {
        Ok(())
    }
    async fn is_empty(&self) -> StorageResult<bool> {
        Ok(true)
    }
    async fn is_finished(&self) -> StorageResult<bool> {
        Ok(true)
    }
    async fn handled_count(&self) -> StorageResult<u64> {
        Ok(0)
    }
    async fn pending_count(&self) -> StorageResult<u64> {
        Ok(0)
    }
}

struct FakeStorageClient {
    dataset: Arc<dyn Dataset>,
    kvs: Arc<dyn KeyValueStore>,
    queue: Arc<dyn RequestQueue>,
    opened: Arc<Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl StorageClient for FakeStorageClient {
    async fn open_dataset(&self, name: Option<&str>) -> StorageResult<Arc<dyn Dataset>> {
        self.opened
            .lock()
            .unwrap()
            .push(format!("dataset:{}", name.unwrap_or("default")));
        Ok(Arc::clone(&self.dataset))
    }
    async fn open_key_value_store(
        &self,
        name: Option<&str>,
    ) -> StorageResult<Arc<dyn KeyValueStore>> {
        self.opened
            .lock()
            .unwrap()
            .push(format!("kvs:{}", name.unwrap_or("default")));
        Ok(Arc::clone(&self.kvs))
    }
    async fn open_request_queue(&self, name: Option<&str>) -> StorageResult<Arc<dyn RequestQueue>> {
        self.opened
            .lock()
            .unwrap()
            .push(format!("queue:{}", name.unwrap_or("default")));
        Ok(Arc::clone(&self.queue))
    }
    async fn purge(&self) -> StorageResult<()> {
        Ok(())
    }
}

type Fixture = (
    StorageHandle,
    Arc<dyn StorageClient>,
    Arc<dyn Dataset>,
    Arc<dyn KeyValueStore>,
    Arc<dyn RequestQueue>,
    Arc<Mutex<Vec<String>>>,
);

fn fixture() -> Fixture {
    let dataset: Arc<dyn Dataset> = Arc::new(FakeDataset);
    let kvs: Arc<dyn KeyValueStore> = Arc::new(FakeKvs);
    let queue: Arc<dyn RequestQueue> = Arc::new(FakeQueue);
    let opened = Arc::new(Mutex::new(Vec::new()));
    let client: Arc<dyn StorageClient> = Arc::new(FakeStorageClient {
        dataset: Arc::clone(&dataset),
        kvs: Arc::clone(&kvs),
        queue: Arc::clone(&queue),
        opened: Arc::clone(&opened),
    });
    (
        StorageHandle::new(
            Arc::clone(&client),
            Arc::clone(&dataset),
            Arc::clone(&kvs),
            Arc::clone(&queue),
        ),
        client,
        dataset,
        kvs,
        queue,
        opened,
    )
}

#[test]
fn accessors_return_passed_handles() {
    let (handle, client, dataset, kvs, queue, _) = fixture();
    assert!(Arc::ptr_eq(handle.client(), &client));
    assert!(Arc::ptr_eq(handle.dataset(), &dataset));
    assert!(Arc::ptr_eq(handle.key_value_store(), &kvs));
    assert!(Arc::ptr_eq(handle.request_queue(), &queue));
}

#[tokio::test]
async fn named_opening_delegates_names() {
    let (handle, _, _, _, _, opened) = fixture();
    handle.dataset_named("items").await.unwrap();
    handle.kvs_named("state").await.unwrap();
    assert_eq!(*opened.lock().unwrap(), ["dataset:items", "kvs:state"]);
}

#[test]
fn clone_is_shallow() {
    let (handle, client, dataset, kvs, queue, _) = fixture();
    let cloned = handle.clone();
    assert!(Arc::ptr_eq(cloned.client(), &client));
    assert!(Arc::ptr_eq(cloned.dataset(), &dataset));
    assert!(Arc::ptr_eq(cloned.key_value_store(), &kvs));
    assert!(Arc::ptr_eq(cloned.request_queue(), &queue));
}
