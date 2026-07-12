//! Demonstrates Phase 1 deduplication and lease-based worker consumption.

use millipede::{AddOptions, ReclaimOptions, Request, StorageClient, StorageError};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let storage = Arc::new(millipede::MemoryStorageClient::new());
    let queue = storage.open_request_queue(None).await?;

    // Queue identity is the request's unique key, so these 60 additions become 40 entries.
    let mut duplicates_skipped = 0;
    for i in 0..60 {
        let url = format!("https://example.com/page/{}", i % 40);
        let req = Request::get(url).build()?;
        let info = queue.add(req, AddOptions::default()).await?;
        duplicates_skipped += u64::from(info.was_already_present);
    }
    println!(
        "added={} duplicates_skipped={duplicates_skipped}",
        60 - duplicates_skipped
    );

    // Forefront requests precede FIFO work; whichever worker wins fetch_next receives this first.
    let priority = Request::get("https://example.com/priority").build()?;
    let mut priority_options = AddOptions::default();
    priority_options.forefront = true;
    queue.add(priority, priority_options).await?;
    println!("/priority queued at forefront; the first worker to fetch receives it first");

    let handled = Arc::new(Mutex::new(0_u64));
    let retries_observed = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::new();
    for worker_id in 0..8 {
        let queue = Arc::clone(&queue);
        let handled = Arc::clone(&handled);
        let retries_observed = Arc::clone(&retries_observed);
        workers.push(tokio::spawn(async move {
            let mut worker_handled = 0;
            loop {
                match queue.fetch_next().await? {
                    Some(lease) => {
                        let path = lease.request.url.path().to_owned();
                        let mut hasher = DefaultHasher::new();
                        lease.request.url.as_str().hash(&mut hasher);
                        tokio::time::sleep(Duration::from_millis(2 + hasher.finish() % 8)).await;

                        // A transient failure returns the lease and increments retry_count.
                        if path == "/page/7" && lease.request.retry_count == 0 {
                            queue.reclaim(lease, ReclaimOptions::default()).await?;
                            continue;
                        }
                        if path == "/page/7" && lease.request.retry_count == 1 {
                            retries_observed.fetch_add(1, Ordering::Relaxed);
                        }

                        // Successful work consumes the lease permanently.
                        queue.mark_handled(lease).await?;
                        worker_handled += 1;
                        *handled.lock().expect("handled counter mutex poisoned") += 1;
                    }
                    None if queue.is_finished().await? => break,
                    None => tokio::task::yield_now().await,
                }
            }
            println!("worker {worker_id}: handled={worker_handled}");
            Ok::<u64, StorageError>(worker_handled)
        }));
    }

    let mut joined_handled = 0;
    for worker in workers {
        joined_handled += worker.await??;
    }
    let unique_handled = queue.handled_count().await?;
    let retries = retries_observed.load(Ordering::Relaxed);
    assert_eq!(unique_handled, 41);
    assert_eq!(joined_handled, 41);
    assert_eq!(*handled.lock().expect("handled counter mutex poisoned"), 41);
    assert_eq!(retries, 1);
    assert!(queue.is_finished().await?);
    assert_eq!(queue.pending_count().await?, 0);
    println!("unique_handled={unique_handled}");
    println!("retries_observed={retries} (/page/7 retry_count=1)");
    Ok(())
}
