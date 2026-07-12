//! Request queue behavior tests.

use millipede_core::{
    request::Request,
    storage::{AddOptions, ReclaimOptions, RequestQueue, RequestSource, StorageError},
};
use millipede_storage_memory::{MemoryQueuePolicy, MemoryRequestQueue};
use std::time::Duration;

fn req(url: &str) -> Request {
    Request::get(url).build().unwrap()
}

#[tokio::test]
async fn dedup_tracks_pending_and_handled() {
    let queue = MemoryRequestQueue::new("dedup");
    let request = req("https://example.com/item");
    assert!(
        !queue
            .add(request.clone(), AddOptions::default())
            .await
            .unwrap()
            .was_already_present
    );
    let duplicate = queue
        .add(request.clone(), AddOptions::default())
        .await
        .unwrap();
    assert!(duplicate.was_already_present);
    assert!(!duplicate.was_already_handled);
    assert_eq!(queue.pending_count().await.unwrap(), 1);
    queue
        .mark_handled(queue.fetch_next().await.unwrap().unwrap())
        .await
        .unwrap();
    let duplicate = queue.add(request, AddOptions::default()).await.unwrap();
    assert!(duplicate.was_already_present && duplicate.was_already_handled);
    assert_eq!(queue.pending_count().await.unwrap(), 0);
}

#[tokio::test]
async fn forefront_precedes_fifo() {
    let queue = MemoryRequestQueue::new("front");
    for url in ["https://example.com/1", "https://example.com/2"] {
        queue.add(req(url), AddOptions::default()).await.unwrap();
    }
    let mut forefront = AddOptions::default();
    forefront.forefront = true;
    queue
        .add(req("https://example.com/3"), forefront)
        .await
        .unwrap();
    for path in ["/3", "/1", "/2"] {
        let lease = queue.fetch_next().await.unwrap().unwrap();
        assert_eq!(lease.request.url.path(), path);
        queue.mark_handled(lease).await.unwrap();
    }
}

#[tokio::test]
async fn lease_lifecycle_and_renewal() {
    let queue = MemoryRequestQueue::new("lease");
    queue
        .add(req("https://example.com/1"), AddOptions::default())
        .await
        .unwrap();
    let lease = queue.fetch_next().await.unwrap().unwrap();
    let lease_id = lease.lease_id.clone();
    assert!(queue.is_empty().await.unwrap());
    assert!(!queue.is_finished().await.unwrap());
    queue
        .renew(&lease_id, Duration::from_secs(30))
        .await
        .unwrap();
    queue.mark_handled(lease).await.unwrap();
    assert_eq!(queue.handled_count().await.unwrap(), 1);
    assert!(queue.is_finished().await.unwrap());
    assert!(matches!(
        queue.renew(&lease_id, Duration::from_secs(1)).await,
        Err(StorageError::LeaseNotFound { .. })
    ));
}

#[tokio::test]
async fn reclaim_controls_retry_and_position() {
    let queue = MemoryRequestQueue::new("reclaim");
    let first = req("https://example.com/first");
    let key = first.unique_key.clone();
    queue.add(first, AddOptions::default()).await.unwrap();
    let lease = queue.fetch_next().await.unwrap().unwrap();
    queue
        .reclaim(lease, ReclaimOptions::default())
        .await
        .unwrap();
    let lease = queue.fetch_next().await.unwrap().unwrap();
    assert_eq!(lease.request.unique_key, key);
    assert_eq!(lease.request.retry_count, 1);
    let mut no_increment = ReclaimOptions::default();
    no_increment.increment_retry = false;
    queue.reclaim(lease, no_increment).await.unwrap();
    queue
        .add(req("https://example.com/second"), AddOptions::default())
        .await
        .unwrap();
    let lease = queue.fetch_next().await.unwrap().unwrap();
    assert_eq!(lease.request.retry_count, 1);
    let mut forefront = ReclaimOptions::default();
    forefront.forefront = true;
    forefront.increment_retry = false;
    queue.reclaim(lease, forefront).await.unwrap();
    assert_eq!(
        queue
            .fetch_next()
            .await
            .unwrap()
            .unwrap()
            .request
            .unique_key,
        key
    );
}

#[tokio::test]
async fn abandon_does_not_increment_retry() {
    let queue = MemoryRequestQueue::new("abandon");
    queue
        .add(req("https://example.com/1"), AddOptions::default())
        .await
        .unwrap();
    queue
        .abandon(queue.fetch_next().await.unwrap().unwrap())
        .await
        .unwrap();
    assert_eq!(
        queue
            .fetch_next()
            .await
            .unwrap()
            .unwrap()
            .request
            .retry_count,
        0
    );
}

#[tokio::test]
async fn add_batch_is_inline_and_flags_duplicate() {
    let queue = MemoryRequestQueue::new("batch");
    let duplicate = req("https://example.com/1");
    let handle = queue
        .add_batch(
            vec![
                duplicate.clone().into(),
                req("https://example.com/2").into(),
                RequestSource::Request(duplicate),
            ],
            AddOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(handle.added.len(), 3);
    assert!(handle.added[2].was_already_present);
    assert_eq!(handle.wait().await.unwrap().processed.len(), 3);
}

#[tokio::test]
async fn domain_round_robin_alternates() {
    let queue = MemoryRequestQueue::with_policy("fair", MemoryQueuePolicy::DomainRoundRobin);
    for url in ["https://a.com/1", "https://a.com/2", "https://b.com/1"] {
        queue.add(req(url), AddOptions::default()).await.unwrap();
    }
    for host in ["a.com", "b.com", "a.com"] {
        let lease = queue.fetch_next().await.unwrap().unwrap();
        assert_eq!(lease.request.url.host_str().unwrap(), host);
        queue.mark_handled(lease).await.unwrap();
    }
}
