//! Storage rate-limit reporting integration tests.

use std::{sync::Arc, time::Duration};

use millipede_core::{
    autoscale::{ClientLoadSignal, LoadSignal},
    request::Request,
    storage::{
        AddOptions, BatchAddHandle, Dataset, KeyValueStore, Lease, LeaseId, ProcessedRequest,
        ReclaimOptions, RequestQueue, RequestSource, StorageClient, StorageError, StorageResult,
    },
};

struct RateLimitedQueue;

#[async_trait::async_trait]
impl RequestQueue for RateLimitedQueue {
    async fn add(&self, _req: Request, _opts: AddOptions) -> StorageResult<ProcessedRequest> {
        Err(StorageError::RateLimited { retry_after: None })
    }

    async fn add_batch(
        &self,
        _reqs: Vec<RequestSource>,
        _opts: AddOptions,
    ) -> StorageResult<BatchAddHandle> {
        unimplemented!()
    }

    async fn fetch_next(&self) -> StorageResult<Option<Lease>> {
        unimplemented!()
    }

    async fn mark_handled(&self, _lease: Lease) -> StorageResult<()> {
        unimplemented!()
    }

    async fn reclaim(&self, _lease: Lease, _opts: ReclaimOptions) -> StorageResult<()> {
        unimplemented!()
    }

    async fn renew(&self, _lease_id: &LeaseId, _extend_by: Duration) -> StorageResult<()> {
        unimplemented!()
    }

    async fn abandon(&self, _lease: Lease) -> StorageResult<()> {
        unimplemented!()
    }

    async fn is_empty(&self) -> StorageResult<bool> {
        unimplemented!()
    }

    async fn is_finished(&self) -> StorageResult<bool> {
        unimplemented!()
    }

    async fn handled_count(&self) -> StorageResult<u64> {
        unimplemented!()
    }

    async fn pending_count(&self) -> StorageResult<u64> {
        unimplemented!()
    }
}

struct HealthyQueue;

#[async_trait::async_trait]
impl RequestQueue for HealthyQueue {
    async fn add(&self, req: Request, _opts: AddOptions) -> StorageResult<ProcessedRequest> {
        Ok(ProcessedRequest {
            request_id: req.id,
            unique_key: req.unique_key,
            was_already_present: false,
            was_already_handled: false,
        })
    }

    async fn add_batch(
        &self,
        _reqs: Vec<RequestSource>,
        _opts: AddOptions,
    ) -> StorageResult<BatchAddHandle> {
        unimplemented!()
    }

    async fn fetch_next(&self) -> StorageResult<Option<Lease>> {
        unimplemented!()
    }

    async fn mark_handled(&self, _lease: Lease) -> StorageResult<()> {
        unimplemented!()
    }

    async fn reclaim(&self, _lease: Lease, _opts: ReclaimOptions) -> StorageResult<()> {
        unimplemented!()
    }

    async fn renew(&self, _lease_id: &LeaseId, _extend_by: Duration) -> StorageResult<()> {
        unimplemented!()
    }

    async fn abandon(&self, _lease: Lease) -> StorageResult<()> {
        unimplemented!()
    }

    async fn is_empty(&self) -> StorageResult<bool> {
        unimplemented!()
    }

    async fn is_finished(&self) -> StorageResult<bool> {
        unimplemented!()
    }

    async fn handled_count(&self) -> StorageResult<u64> {
        unimplemented!()
    }

    async fn pending_count(&self) -> StorageResult<u64> {
        unimplemented!()
    }
}

struct FakeClient(Arc<dyn RequestQueue>);

#[async_trait::async_trait]
impl StorageClient for FakeClient {
    async fn open_dataset(&self, _name: Option<&str>) -> StorageResult<Arc<dyn Dataset>> {
        unimplemented!()
    }

    async fn open_key_value_store(
        &self,
        _name: Option<&str>,
    ) -> StorageResult<Arc<dyn KeyValueStore>> {
        unimplemented!()
    }

    async fn open_request_queue(
        &self,
        _name: Option<&str>,
    ) -> StorageResult<Arc<dyn RequestQueue>> {
        Ok(Arc::clone(&self.0))
    }

    async fn purge(&self) -> StorageResult<()> {
        unimplemented!()
    }
}

#[tokio::test]
async fn rate_limited_queue_operation_reports_overload() {
    let signal = ClientLoadSignal::new();
    let wrapped = signal.instrument_storage(Arc::new(FakeClient(Arc::new(RateLimitedQueue))));
    let queue = wrapped.open_request_queue(None).await.unwrap();
    let _ = queue
        .add(
            Request::get("https://example.com/").build().unwrap(),
            AddOptions::default(),
        )
        .await;

    let samples = LoadSignal::sample(&signal, Duration::from_secs(60));
    assert!(samples.iter().any(|sample| sample.overloaded));
}

#[tokio::test]
async fn successful_queue_operation_reports_healthy() {
    let signal = ClientLoadSignal::new();
    let wrapped = signal.instrument_storage(Arc::new(FakeClient(Arc::new(HealthyQueue))));
    let queue = wrapped.open_request_queue(None).await.unwrap();
    let samples_before_add = LoadSignal::sample(&signal, Duration::from_secs(60));
    queue
        .add(
            Request::get("https://example.com/").build().unwrap(),
            AddOptions::default(),
        )
        .await
        .unwrap();

    let samples_after_add = LoadSignal::sample(&signal, Duration::from_secs(60));
    assert_eq!(samples_after_add.len(), samples_before_add.len() + 1);
    assert!(!samples_after_add.last().unwrap().overloaded);
}
