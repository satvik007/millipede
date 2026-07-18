//! In-memory request queue micro-benchmarks.
#![allow(missing_docs)]

use criterion::{BatchSize, Criterion, Throughput, black_box, criterion_group, criterion_main};
use millipede_core::{
    request::Request,
    storage::{AddOptions, RequestQueue},
};
use millipede_storage_memory::MemoryRequestQueue;

const REQUEST_COUNT: usize = 1_000;

fn request(index: usize) -> Request {
    Request::get(format!("https://example.com/item/{index}"))
        .build()
        .unwrap()
}

async fn prefill(queue: &MemoryRequestQueue) {
    for index in 0..REQUEST_COUNT {
        let _ = queue
            .add(request(index), AddOptions::default())
            .await
            .unwrap();
    }
}

fn prefilled_queue() -> MemoryRequestQueue {
    let queue = MemoryRequestQueue::new("lease-cycle");
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(prefill(&queue));
    });
    queue
}

fn queue_ops(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("queue_ops");

    group.throughput(Throughput::Elements(REQUEST_COUNT as u64));
    group.bench_function("enqueue_1000_unique", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let requests = (0..REQUEST_COUNT).map(request).collect::<Vec<_>>();
                (MemoryRequestQueue::new("enqueue"), requests)
            },
            |(queue, requests)| async move {
                for request in requests {
                    let _ = black_box(queue.add(request, AddOptions::default()).await.unwrap());
                }
                queue
            },
            BatchSize::SmallInput,
        );
    });

    group.throughput(Throughput::Elements(1));
    group.bench_function("dedup_hit", |b| {
        b.to_async(&runtime).iter_batched(
            || (prefilled_queue(), request(0)),
            |(queue, duplicate)| async move {
                let _ = black_box(queue.add(duplicate, AddOptions::default()).await.unwrap());
                queue
            },
            BatchSize::SmallInput,
        );
    });

    group.throughput(Throughput::Elements(REQUEST_COUNT as u64));
    group.bench_function("lease_cycle", |b| {
        b.to_async(&runtime).iter_batched(
            prefilled_queue,
            |queue| async move {
                for _ in 0..REQUEST_COUNT {
                    let lease = queue.fetch_next().await.unwrap().unwrap();
                    queue.mark_handled(lease).await.unwrap();
                }
                queue
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, queue_ops);
criterion_main!(benches);
