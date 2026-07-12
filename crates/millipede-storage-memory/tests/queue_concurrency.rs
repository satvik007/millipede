//! Concurrent request queue contention test.

use millipede_core::{
    request::Request,
    storage::{AddOptions, RequestQueue},
};
use millipede_storage_memory::MemoryRequestQueue;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};
use tokio::sync::watch;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_dedup_has_no_double_handling() {
    let queue = Arc::new(MemoryRequestQueue::new("concurrent"));
    let handled = Arc::new(Mutex::new(Vec::new()));
    let (done_tx, done_rx) = watch::channel(false);
    let mut producers = Vec::new();
    for i in 0..100 {
        let queue = Arc::clone(&queue);
        producers.push(tokio::spawn(async move {
            for j in 0..20 {
                let index = (i * 20 + j) % 1000;
                let request = Request::get(format!("https://example.com/item/{index}"))
                    .build()
                    .unwrap();
                queue.add(request, AddOptions::default()).await.unwrap();
            }
        }));
    }
    let mut consumers = Vec::new();
    for _ in 0..10 {
        let queue = Arc::clone(&queue);
        let handled = Arc::clone(&handled);
        let done = done_rx.clone();
        consumers.push(tokio::spawn(async move {
            loop {
                if let Some(lease) = queue.fetch_next().await.unwrap() {
                    handled
                        .lock()
                        .unwrap()
                        .push(lease.request.unique_key.clone());
                    queue.mark_handled(lease).await.unwrap();
                } else if *done.borrow() && queue.is_finished().await.unwrap() {
                    break;
                } else {
                    tokio::task::yield_now().await;
                }
            }
        }));
    }
    for producer in producers {
        producer.await.unwrap();
    }
    done_tx.send(true).unwrap();
    for consumer in consumers {
        consumer.await.unwrap();
    }
    {
        let handled = handled.lock().unwrap();
        assert_eq!(handled.len(), 1000);
        assert_eq!(handled.iter().collect::<HashSet<_>>().len(), 1000);
    }
    assert_eq!(queue.handled_count().await.unwrap(), 1000);
    assert!(queue.is_finished().await.unwrap());
    assert_eq!(queue.pending_count().await.unwrap(), 0);
}
