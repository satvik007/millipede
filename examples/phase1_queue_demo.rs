//! Demonstrates Phase 1 request deduplication and lease-based worker consumption.

use millipede::prelude::{AddOptions, MemoryRequestQueue, Request, RequestQueue, StorageResult};
use std::sync::Arc;

#[tokio::main]
async fn main() -> StorageResult<()> {
    let queue = Arc::new(MemoryRequestQueue::new("phase1-demo"));
    for url in [
        "https://example.com/one",
        "https://example.com/two",
        "https://example.com/three",
        "https://example.com/two",
    ] {
        let request = Request::get(url).build().expect("demo URL is valid");
        let info = queue.add(request, AddOptions::default()).await?;
        println!("add {url}: already present = {}", info.was_already_present);
    }

    let mut workers = Vec::new();
    for worker_id in 0..8 {
        let queue = Arc::clone(&queue);
        workers.push(tokio::spawn(async move {
            loop {
                if let Some(lease) = queue.fetch_next().await? {
                    println!("worker {worker_id} handled {}", lease.request.url);
                    queue.mark_handled(lease).await?;
                } else if queue.is_finished().await? {
                    return StorageResult::Ok(());
                } else {
                    tokio::task::yield_now().await;
                }
            }
        }));
    }

    for worker in workers {
        worker.await.expect("demo worker must not panic")?;
    }
    println!("handled {} unique requests", queue.handled_count().await?);
    Ok(())
}
