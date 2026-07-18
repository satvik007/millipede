//! Per-request crawler engine overhead benchmark.
#![allow(missing_docs)]

use std::sync::Arc;

use criterion::{BatchSize, Criterion, Throughput, black_box, criterion_group, criterion_main};
use millipede_core::{
    crawler::{BasicContext, BasicKind, Crawler},
    request::Request,
};
use millipede_storage_memory::MemoryStorageClient;

const REQUEST_COUNT: usize = 200;

fn requests() -> Vec<Request> {
    (0..REQUEST_COUNT)
        .map(|index| {
            Request::get(format!("https://example.com/item/{index}"))
                .build()
                .unwrap()
        })
        .collect()
}

fn fresh_crawler() -> Crawler<BasicKind> {
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            Crawler::builder(BasicKind)
                .storage_client(Arc::new(MemoryStorageClient::new()))
                .request_handler(|_context: BasicContext| async { Ok(()) })
                .max_concurrency(4)
                .build()
                .await
                .unwrap()
        })
    })
}

fn engine_overhead(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let synthetic_requests = requests();
    let mut group = c.benchmark_group("engine_overhead");
    group.throughput(Throughput::Elements(REQUEST_COUNT as u64));

    group.bench_function("run_200", |b| {
        b.to_async(&runtime).iter_batched(
            || (fresh_crawler(), synthetic_requests.clone()),
            |(crawler, requests)| async move {
                let statistics = black_box(crawler.run(requests).await.unwrap());
                (crawler, statistics)
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, engine_overhead);
criterion_main!(benches);
