//! Storage trait object-safety and typed-extension integration tests.

use futures_util::{StreamExt, stream};
use millipede_core::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct VecDataset(Mutex<Vec<serde_json::Value>>);

#[async_trait::async_trait]
impl Dataset for VecDataset {
    async fn push_json(&self, item: serde_json::Value) -> StorageResult<()> {
        self.0.lock().unwrap().push(item);
        Ok(())
    }

    async fn push_json_batch(&self, items: Vec<serde_json::Value>) -> StorageResult<()> {
        self.0.lock().unwrap().extend(items);
        Ok(())
    }

    async fn list_raw(&self, opts: ListOptions) -> StorageResult<Page<serde_json::Value>> {
        let mut items = self.0.lock().unwrap().clone();
        if opts.desc {
            items.reverse();
        }
        let total = items.len() as u64;
        let items = items
            .into_iter()
            .skip(opts.offset as usize)
            .take(opts.limit.unwrap_or(u64::MAX) as usize)
            .collect();
        Ok(Page {
            items,
            total,
            offset: opts.offset,
            limit: opts.limit,
        })
    }

    fn stream_raw(
        &self,
        opts: ListOptions,
    ) -> futures_util::stream::BoxStream<'_, StorageResult<serde_json::Value>> {
        let mut items = self.0.lock().unwrap().clone();
        if opts.desc {
            items.reverse();
        }
        Box::pin(stream::iter(
            items
                .into_iter()
                .skip(opts.offset as usize)
                .take(opts.limit.unwrap_or(u64::MAX) as usize)
                .map(Ok),
        ))
    }

    async fn export_json(&self, _: &std::path::Path) -> StorageResult<()> {
        Err(StorageError::Unsupported("test"))
    }

    async fn export_csv(&self, _: &std::path::Path) -> StorageResult<()> {
        Err(StorageError::Unsupported("test"))
    }

    async fn info(&self) -> StorageResult<DatasetInfo> {
        Err(StorageError::Unsupported("test"))
    }
}

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

#[derive(Default)]
struct NoopQueue {
    forefront: Mutex<Option<bool>>,
}

#[async_trait::async_trait]
impl RequestQueue for NoopQueue {
    async fn add(&self, req: Request, opts: AddOptions) -> StorageResult<QueueOpInfo> {
        *self.forefront.lock().unwrap() = Some(opts.forefront);
        Ok(processed(req))
    }

    async fn add_batch(
        &self,
        reqs: Vec<RequestSource>,
        _: AddOptions,
    ) -> StorageResult<BatchAddHandle> {
        let added = reqs
            .into_iter()
            .map(|source| match source {
                RequestSource::Request(request) => processed(request),
                _ => unreachable!(),
            })
            .collect();
        Ok(BatchAddHandle::ready(added))
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

    async fn renew(&self, lease_id: &LeaseId, _: std::time::Duration) -> StorageResult<()> {
        Err(StorageError::LeaseNotFound {
            lease_id: lease_id.clone(),
        })
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

fn processed(request: Request) -> ProcessedRequest {
    ProcessedRequest {
        request_id: request.id,
        unique_key: request.unique_key,
        was_already_present: false,
        was_already_handled: false,
    }
}

struct NoopClient;

#[async_trait::async_trait]
impl StorageClient for NoopClient {
    async fn open_dataset(&self, _: Option<&str>) -> StorageResult<Arc<dyn Dataset>> {
        Ok(Arc::new(VecDataset::default()))
    }

    async fn open_key_value_store(&self, _: Option<&str>) -> StorageResult<Arc<dyn KeyValueStore>> {
        Ok(Arc::new(MapKvs::default()))
    }

    async fn open_request_queue(&self, _: Option<&str>) -> StorageResult<Arc<dyn RequestQueue>> {
        Ok(Arc::new(NoopQueue::default()))
    }

    async fn purge(&self) -> StorageResult<()> {
        Ok(())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct MyStruct {
    a: String,
    n: u32,
}

#[test]
fn storage_traits_are_object_safe() {
    let _: Arc<dyn Dataset> = Arc::new(VecDataset::default());
    let _: Arc<dyn KeyValueStore> = Arc::new(MapKvs::default());
    let _: Arc<dyn RequestQueue> = Arc::new(NoopQueue::default());
    let _: Arc<dyn StorageClient> = Arc::new(NoopClient);
}

#[tokio::test]
async fn extension_traits_round_trip_through_trait_objects() {
    let dataset: Arc<dyn Dataset> = Arc::new(VecDataset::default());
    let item = MyStruct {
        a: "value".into(),
        n: 7,
    };
    dataset.push(&item).await.unwrap();
    let page = dataset
        .list::<MyStruct>(ListOptions::default())
        .await
        .unwrap();
    assert_eq!(page.items, [item]);

    let streamed: Vec<_> = dataset
        .stream::<MyStruct>(ListOptions::default())
        .collect()
        .await;
    assert_eq!(streamed.len(), 1);
    assert_eq!(streamed[0].as_ref().unwrap(), &page.items[0]);

    let kvs: Arc<dyn KeyValueStore> = Arc::new(MapKvs::default());
    kvs.set("item", &page.items[0]).await.unwrap();
    assert_eq!(
        kvs.get::<MyStruct>("item").await.unwrap(),
        Some(MyStruct {
            a: "value".into(),
            n: 7,
        })
    );
}

#[tokio::test]
async fn ready_batch_and_builder_enqueue_preserve_payloads_and_forefront() {
    let mut added = vec![
        processed(Request::get("https://example.com/one").build().unwrap()),
        processed(Request::get("https://example.com/two").build().unwrap()),
        processed(Request::get("https://example.com/three").build().unwrap()),
    ];
    added[1].was_already_present = true;
    added[2].was_already_present = true;
    added[2].was_already_handled = true;
    let completed = BatchAddHandle::ready(added.clone()).wait().await.unwrap();
    assert_eq!(completed.processed.len(), added.len());
    for (actual, expected) in completed.processed.iter().zip(&added) {
        assert_eq!(actual.request_id, expected.request_id);
        assert_eq!(actual.unique_key, expected.unique_key);
        assert_eq!(actual.was_already_present, expected.was_already_present);
        assert_eq!(actual.was_already_handled, expected.was_already_handled);
    }

    let queue = NoopQueue::default();
    Request::get("https://example.com/forefront")
        .forefront(true)
        .enqueue(&queue)
        .await
        .unwrap();
    assert_eq!(*queue.forefront.lock().unwrap(), Some(true));
}

#[tokio::test]
async fn builder_enqueue_uses_default_non_forefront_option() {
    let queue = NoopQueue::default();
    Request::get("https://example.com/default")
        .enqueue(&queue)
        .await
        .unwrap();
    assert_eq!(*queue.forefront.lock().unwrap(), Some(false));
}

#[tokio::test]
async fn renewing_an_unknown_lease_returns_the_matching_lease_id() {
    let queue = NoopQueue::default();
    let lease_id = LeaseId::new(42);

    let error = queue
        .renew(&lease_id, std::time::Duration::from_secs(30))
        .await
        .unwrap_err();

    match error {
        StorageError::LeaseNotFound {
            lease_id: returned_id,
        } => assert_eq!(returned_id, lease_id),
        other => panic!("expected LeaseNotFound, got {other:?}"),
    }
}
