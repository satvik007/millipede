//! Demonstrates per-domain delay, deterministic random jitter, and crawl concurrency/rate caps.
//!
//! Run with: `cargo run -p millipede --example rate_limit`

use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use millipede::{Crawler, HttpContext, HttpKind, MemoryStorageClient};
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::any};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_string("rate-limited page"))
        .mount(&server)
        .await;

    let entries = Arc::new(Mutex::new(Vec::<Instant>::new()));
    let kind = HttpKind::builder()
        .pre_navigation_hook(|ctx| {
            let mut hasher = DefaultHasher::new();
            ctx.request.url.as_str().hash(&mut hasher);
            let jitter_ms = hasher.finish() % 151;
            // This deterministic random jitter is example-layer behavior, not a library feature.
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                Ok(())
            })
        })
        .build()?;
    let crawler = Crawler::builder(kind)
        .max_concurrency(2)
        .same_domain_delay(Duration::from_millis(200))
        .max_tasks_per_minute(600)
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .request_handler({
            let entries = Arc::clone(&entries);
            move |ctx: HttpContext| {
                let entries = Arc::clone(&entries);
                async move {
                    // Entry is recorded first so the measurements exclude handler work.
                    entries
                        .lock()
                        .expect("handler-entry mutex poisoned")
                        .push(Instant::now());
                    println!("handled {}", ctx.request.url);
                    Ok(())
                }
            }
        })
        .build()
        .await?;

    let base = server.uri();
    let urls = (0..10)
        .map(|index| format!("{base}/page/{index}"))
        .collect::<Vec<_>>();
    let started = Instant::now();
    let stats = crawler.run(urls).await?;
    let wall_time = started.elapsed();

    let mut entries = entries
        .lock()
        .expect("handler-entry mutex poisoned")
        .clone();
    entries.sort_unstable();
    let spacings = entries
        .windows(2)
        .map(|pair| pair[1].duration_since(pair[0]))
        .collect::<Vec<_>>();
    println!("observed handler-entry spacing: {spacings:?}");
    println!("total wall time: {wall_time:?}");

    anyhow::ensure!(
        stats.requests_finished == 10 && stats.requests_failed == 0,
        "expected all ten pages to succeed, got {stats:#?}"
    );
    anyhow::ensure!(entries.len() == 10, "expected ten handler-entry samples");
    // Nine 200 ms same-domain slots imply about 1.8 s. The 1.4 s bound leaves broad scheduler
    // slack while still failing if the domain delay is not applied at all.
    anyhow::ensure!(
        wall_time >= Duration::from_millis(1_400),
        "crawl completed too quickly for the configured same-domain delay: {wall_time:?}"
    );
    Ok(())
}
