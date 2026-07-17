//! File-system request queue contract tests.

use millipede_core::{
    request::Request,
    storage::{
        AddOptions, LeaseId, ReclaimOptions, RequestQueue, RequestSource, StorageClient,
        StorageError,
    },
};
use millipede_storage_fs::FsStorageClient;
use serde_json::json;
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn req(url: &str) -> Request {
    Request::get(url).build().unwrap()
}

async fn queue(root: &tempfile::TempDir, name: &str) -> Arc<dyn RequestQueue> {
    FsStorageClient::new(root.path())
        .open_request_queue(Some(name))
        .await
        .unwrap()
}

#[tokio::test]
async fn fifo_order() {
    let root = tempfile::tempdir().unwrap();
    let queue = queue(&root, "fifo").await;
    for url in ["https://example.com/1", "https://example.com/2"] {
        queue.add(req(url), AddOptions::default()).await.unwrap();
    }
    for path in ["/1", "/2"] {
        let lease = queue.fetch_next().await.unwrap().unwrap();
        assert_eq!(lease.request.url.path(), path);
        queue.mark_handled(lease).await.unwrap();
    }
}

#[tokio::test]
async fn forefront_jumps_the_queue() {
    let root = tempfile::tempdir().unwrap();
    let queue = queue(&root, "front").await;
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
async fn unique_key_dedup_tracks_pending_and_handled() {
    let root = tempfile::tempdir().unwrap();
    let queue = queue(&root, "dedup").await;
    let request = req("https://example.com/item");
    let added = queue
        .add(request.clone(), AddOptions::default())
        .await
        .unwrap();
    assert!(!added.was_already_present);

    let duplicate = queue
        .add(request.clone(), AddOptions::default())
        .await
        .unwrap();
    assert!(duplicate.was_already_present);
    assert!(!duplicate.was_already_handled);
    assert_eq!(queue.pending_count().await.unwrap(), 1);

    let lease = queue.fetch_next().await.unwrap().unwrap();
    queue.mark_handled(lease).await.unwrap();
    let duplicate = queue.add(request, AddOptions::default()).await.unwrap();
    assert!(duplicate.was_already_present);
    assert!(duplicate.was_already_handled);
}

#[tokio::test]
async fn reclaim_and_abandon_preserve_request_state_and_retry_rules() {
    let root = tempfile::tempdir().unwrap();
    let queue = queue(&root, "retry").await;
    queue
        .add(req("https://example.com/1"), AddOptions::default())
        .await
        .unwrap();

    let mut lease = queue.fetch_next().await.unwrap().unwrap();
    lease.request.session_rotation_count = 3;
    lease
        .request
        .error_messages
        .push("temporary failure".into());
    queue
        .reclaim(lease, ReclaimOptions::default())
        .await
        .unwrap();

    let mut lease = queue.fetch_next().await.unwrap().unwrap();
    assert_eq!(lease.request.retry_count, 1);
    assert_eq!(lease.request.session_rotation_count, 3);
    assert_eq!(lease.request.error_messages, ["temporary failure"]);
    lease.request.session_rotation_count = 4;
    lease.request.error_messages.push("worker shutdown".into());
    queue.abandon(lease).await.unwrap();

    let lease = queue.fetch_next().await.unwrap().unwrap();
    assert_eq!(lease.request.retry_count, 1);
    assert_eq!(lease.request.session_rotation_count, 4);
    assert_eq!(
        lease.request.error_messages,
        ["temporary failure", "worker shutdown"]
    );
}

#[tokio::test]
async fn reclaim_position_and_increment_are_configurable() {
    let root = tempfile::tempdir().unwrap();
    let queue = queue(&root, "reclaim-position").await;
    queue
        .add(req("https://example.com/first"), AddOptions::default())
        .await
        .unwrap();
    queue
        .add(req("https://example.com/second"), AddOptions::default())
        .await
        .unwrap();
    let lease = queue.fetch_next().await.unwrap().unwrap();
    let mut options = ReclaimOptions::default();
    options.forefront = true;
    options.increment_retry = false;
    queue.reclaim(lease, options).await.unwrap();
    let lease = queue.fetch_next().await.unwrap().unwrap();
    assert_eq!(lease.request.url.path(), "/first");
    assert_eq!(lease.request.retry_count, 0);
}

#[tokio::test]
async fn renewal_rejects_unknown_or_completed_leases() {
    let root = tempfile::tempdir().unwrap();
    let queue = queue(&root, "renew").await;
    assert!(matches!(
        queue.renew(&LeaseId::new(42), Duration::from_secs(1)).await,
        Err(StorageError::LeaseNotFound { .. })
    ));

    queue
        .add(req("https://example.com/1"), AddOptions::default())
        .await
        .unwrap();
    let lease = queue.fetch_next().await.unwrap().unwrap();
    let lease_id = lease.lease_id.clone();
    queue
        .renew(&lease_id, Duration::from_secs(30))
        .await
        .unwrap();
    queue.mark_handled(lease).await.unwrap();
    assert!(matches!(
        queue.renew(&lease_id, Duration::from_secs(1)).await,
        Err(StorageError::LeaseNotFound { .. })
    ));
}

#[tokio::test]
async fn counts_remain_consistent_across_a_mixed_workload() {
    let root = tempfile::tempdir().unwrap();
    let queue = queue(&root, "counts").await;
    for index in 0..4 {
        queue
            .add(
                req(&format!("https://example.com/{index}")),
                AddOptions::default(),
            )
            .await
            .unwrap();
    }
    assert_eq!(queue.pending_count().await.unwrap(), 4);
    assert_eq!(queue.handled_count().await.unwrap(), 0);

    let handled = queue.fetch_next().await.unwrap().unwrap();
    let reclaimed = queue.fetch_next().await.unwrap().unwrap();
    assert_eq!(queue.pending_count().await.unwrap(), 2);
    assert!(!queue.is_finished().await.unwrap());
    queue.mark_handled(handled).await.unwrap();
    queue
        .reclaim(reclaimed, ReclaimOptions::default())
        .await
        .unwrap();
    assert_eq!(queue.pending_count().await.unwrap(), 3);
    assert_eq!(queue.handled_count().await.unwrap(), 1);

    while let Some(lease) = queue.fetch_next().await.unwrap() {
        queue.mark_handled(lease).await.unwrap();
    }
    assert_eq!(queue.pending_count().await.unwrap(), 0);
    assert_eq!(queue.handled_count().await.unwrap(), 4);
    assert!(queue.is_empty().await.unwrap());
    assert!(queue.is_finished().await.unwrap());
}

#[tokio::test]
async fn add_batch_is_inline_and_flags_duplicates() {
    let root = tempfile::tempdir().unwrap();
    let queue = queue(&root, "batch").await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_adds_and_fetches_do_not_deadlock_or_double_lease() {
    let root = tempfile::tempdir().unwrap();
    let queue = queue(&root, "concurrent").await;
    let producer_queue = Arc::clone(&queue);
    let producer = async move {
        for index in 0..200 {
            producer_queue
                .add(
                    req(&format!("https://example.com/item/{}", index % 100)),
                    AddOptions::default(),
                )
                .await
                .unwrap();
            tokio::task::yield_now().await;
        }
    };
    let handled_count = Arc::new(AtomicUsize::new(0));
    let mut consumers = Vec::new();
    for _ in 0..4 {
        let consumer_queue = Arc::clone(&queue);
        let handled_count = Arc::clone(&handled_count);
        consumers.push(tokio::spawn(async move {
            let mut handled = Vec::new();
            while handled_count.load(Ordering::Acquire) < 100 {
                if let Some(lease) = consumer_queue.fetch_next().await.unwrap() {
                    handled.push(lease.request.unique_key.clone());
                    consumer_queue.mark_handled(lease).await.unwrap();
                    handled_count.fetch_add(1, Ordering::AcqRel);
                } else {
                    tokio::task::yield_now().await;
                }
            }
            handled
        }));
    }
    let consumers = async move {
        let mut handled = Vec::new();
        for consumer in consumers {
            handled.extend(consumer.await.unwrap());
        }
        handled
    };
    let (_, handled) = tokio::join!(producer, consumers);
    assert_eq!(handled.len(), 100);
    assert_eq!(handled.iter().collect::<HashSet<_>>().len(), 100);
    assert_eq!(queue.handled_count().await.unwrap(), 100);
    assert!(queue.is_finished().await.unwrap());
}

#[tokio::test]
async fn opens_crawlee_camel_case_fixture_without_state_cache() {
    let root = tempfile::tempdir().unwrap();
    let requests_path = root.path().join("request_queues/interop/requests");
    std::fs::create_dir_all(&requests_path).unwrap();
    let pending = req("https://example.com/pending");
    let handled = req("https://example.com/handled");
    std::fs::write(
        requests_path.join(format!("{}.json", pending.id)),
        serde_json::to_vec_pretty(&json!({
            "id": pending.id,
            "url": pending.url,
            "uniqueKey": pending.unique_key,
            "method": "GET",
            "retryCount": 0,
            "orderNo": 17,
            "json": pending,
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(
        requests_path.join(format!("{}.json", handled.id)),
        serde_json::to_vec_pretty(&json!({
            "id": handled.id,
            "url": handled.url,
            "uniqueKey": handled.unique_key,
            "method": "GET",
            "retryCount": 0,
            "orderNo": null,
            "json": handled,
        }))
        .unwrap(),
    )
    .unwrap();
    assert!(
        !root
            .path()
            .join("request_queues/interop/state.json")
            .exists()
    );

    let queue = queue(&root, "interop").await;
    assert_eq!(queue.pending_count().await.unwrap(), 1);
    assert_eq!(queue.handled_count().await.unwrap(), 1);
    let lease = queue.fetch_next().await.unwrap().unwrap();
    assert_eq!(lease.request.url.path(), "/pending");
    queue.mark_handled(lease).await.unwrap();

    let duplicate = queue
        .add(req("https://example.com/handled"), AddOptions::default())
        .await
        .unwrap();
    assert!(duplicate.was_already_present);
    assert!(duplicate.was_already_handled);
}

#[tokio::test]
async fn authentic_crawlee_fixture_opens_resumes_reads_and_dedupes() {
    let root = tempfile::tempdir().unwrap();
    let requests_path = root.path().join("request_queues/crawlee/requests");
    std::fs::create_dir_all(&requests_path).unwrap();
    let pending_url = "https://example.com/from-crawlee";
    let handled_url = "https://example.com/already-handled";
    std::fs::write(
        requests_path.join("crawleePending.json"),
        serde_json::to_vec_pretty(&json!({
            "id": "crawleePending",
            "url": pending_url,
            "uniqueKey": "crawlee-pending-key",
            "method": "POST",
            "headers": { "content-type": "text/plain", "x-source": "crawlee" },
            "payload": "fixture body",
            "userData": { "label": "imported", "source": "fixture" },
            "retryCount": 2,
            "noRetry": true,
            "orderNo": 17
        }))
        .unwrap(),
    )
    .unwrap();
    std::fs::write(
        requests_path.join("crawleeHandled.json"),
        serde_json::to_vec_pretty(&json!({
            "id": "crawleeHandled",
            "url": handled_url,
            "uniqueKey": handled_url,
            "method": "GET",
            "retryCount": 0,
            "orderNo": null
        }))
        .unwrap(),
    )
    .unwrap();

    let first_client = FsStorageClient::new(root.path());
    let first = first_client
        .open_request_queue(Some("crawlee"))
        .await
        .unwrap();
    assert_eq!(first.pending_count().await.unwrap(), 1);
    assert_eq!(first.handled_count().await.unwrap(), 1);
    let first_lease = first.fetch_next().await.unwrap().unwrap();
    assert_eq!(first_lease.request.id.to_string(), "crawleePending");
    assert_eq!(first_lease.request.url.as_str(), pending_url);
    assert_eq!(first_lease.request.unique_key, "crawlee-pending-key");
    assert_eq!(first_lease.request.method.as_str(), "POST");
    assert_eq!(first_lease.request.headers["x-source"], "crawlee");
    assert_eq!(
        first_lease.request.body,
        Some(millipede_core::request::RequestBody::Bytes(
            b"fixture body".to_vec()
        ))
    );
    assert_eq!(first_lease.request.retry_count, 2);
    assert!(first_lease.request.no_retry);
    assert_eq!(first_lease.request.label.as_deref(), Some("imported"));
    assert_eq!(
        first_lease
            .request
            .user_data
            .get("source")
            .and_then(|value| value.as_str()),
        Some("fixture")
    );
    drop(first_lease);
    drop(first);
    drop(first_client);

    let resumed = queue(&root, "crawlee").await;
    let resumed_lease = resumed.fetch_next().await.unwrap().unwrap();
    assert_eq!(resumed_lease.request.url.as_str(), pending_url);
    resumed.mark_handled(resumed_lease).await.unwrap();
    drop(resumed);

    let reopened = queue(&root, "crawlee").await;
    assert_eq!(reopened.pending_count().await.unwrap(), 0);
    assert_eq!(reopened.handled_count().await.unwrap(), 2);
    let pending_duplicate = Request::get(pending_url)
        .unique_key("crawlee-pending-key")
        .build()
        .unwrap();
    for request in [pending_duplicate, req(handled_url)] {
        let duplicate = reopened.add(request, AddOptions::default()).await.unwrap();
        assert!(duplicate.was_already_present);
        assert!(duplicate.was_already_handled);
    }
    assert!(reopened.is_finished().await.unwrap());
}

#[tokio::test]
async fn accepts_a_bare_serialized_request_fixture() {
    let root = tempfile::tempdir().unwrap();
    let requests_path = root.path().join("request_queues/bare/requests");
    std::fs::create_dir_all(&requests_path).unwrap();
    let request = req("https://example.com/bare");
    std::fs::write(
        requests_path.join(format!("{}.json", request.id)),
        serde_json::to_vec_pretty(&request).unwrap(),
    )
    .unwrap();

    let queue = queue(&root, "bare").await;
    assert_eq!(queue.pending_count().await.unwrap(), 1);
    assert_eq!(queue.fetch_next().await.unwrap().unwrap().request, request);
}
