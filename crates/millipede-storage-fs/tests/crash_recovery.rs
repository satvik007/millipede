//! Portable abrupt-drop recovery tests for the file-system request queue.

use millipede_core::{
    request::Request,
    storage::{AddOptions, StorageClient},
};
use millipede_storage_fs::FsStorageClient;
use std::collections::{HashMap, HashSet};

fn req(index: usize) -> Request {
    Request::get(format!("https://example.com/{index}"))
        .build()
        .unwrap()
}

#[tokio::test]
async fn rescanning_request_files_recovers_outstanding_leases_without_retries() {
    let root = tempfile::tempdir().unwrap();
    let leased_before_crash = {
        let client = FsStorageClient::new(root.path());
        let queue = client.open_request_queue(Some("resume")).await.unwrap();
        for index in 0..10 {
            queue.add(req(index), AddOptions::default()).await.unwrap();
        }
        for _ in 0..4 {
            let lease = queue.fetch_next().await.unwrap().unwrap();
            queue.mark_handled(lease).await.unwrap();
        }
        let first = queue.fetch_next().await.unwrap().unwrap();
        let second = queue.fetch_next().await.unwrap().unwrap();
        HashMap::from([
            (first.request.unique_key.clone(), first.request.retry_count),
            (
                second.request.unique_key.clone(),
                second.request.retry_count,
            ),
        ])
        // No abandon and no graceful shutdown: the queue, client, and leases
        // are all abruptly dropped at the end of this scope.
    };

    std::fs::remove_file(root.path().join("request_queues/resume/state.json")).unwrap();

    let client = FsStorageClient::new(root.path());
    let queue = client.open_request_queue(Some("resume")).await.unwrap();
    assert_eq!(queue.pending_count().await.unwrap(), 6);
    assert_eq!(queue.handled_count().await.unwrap(), 4);
    assert!(!queue.is_finished().await.unwrap());

    let mut recovered = HashSet::new();
    while let Some(lease) = queue.fetch_next().await.unwrap() {
        if let Some(previous_retry_count) = leased_before_crash.get(&lease.request.unique_key) {
            assert_eq!(lease.request.retry_count, *previous_retry_count);
            recovered.insert(lease.request.unique_key.clone());
        }
        queue.mark_handled(lease).await.unwrap();
    }
    assert_eq!(recovered.len(), 2);
    assert!(queue.is_finished().await.unwrap());
    assert_eq!(queue.pending_count().await.unwrap(), 0);
    assert_eq!(queue.handled_count().await.unwrap(), 10);
}

#[tokio::test]
async fn fresh_clients_resume_to_completion_without_duplicates() {
    let root = tempfile::tempdir().unwrap();
    let first_run = FsStorageClient::new(root.path());
    let queue = first_run.open_request_queue(None).await.unwrap();
    for index in 0..24 {
        queue.add(req(index), AddOptions::default()).await.unwrap();
    }
    let mut first_run_handled = HashSet::new();
    for _ in 0..7 {
        let lease = queue.fetch_next().await.unwrap().unwrap();
        first_run_handled.insert(lease.request.unique_key.clone());
        queue.mark_handled(lease).await.unwrap();
    }
    let outstanding = queue.fetch_next().await.unwrap().unwrap();
    drop(outstanding);
    drop(queue);
    drop(first_run);

    let resumed_client = FsStorageClient::new(root.path());
    let resumed = resumed_client.open_request_queue(None).await.unwrap();
    let mut resumed_handled = HashSet::new();
    while let Some(lease) = resumed.fetch_next().await.unwrap() {
        assert!(!first_run_handled.contains(&lease.request.unique_key));
        assert!(resumed_handled.insert(lease.request.unique_key.clone()));
        resumed.mark_handled(lease).await.unwrap();
    }
    assert_eq!(first_run_handled.len() + resumed_handled.len(), 24);
    assert_eq!(resumed.handled_count().await.unwrap(), 24);
    assert!(resumed.is_finished().await.unwrap());
}
