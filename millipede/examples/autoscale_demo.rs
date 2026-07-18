//! Demonstrates deterministic AIMD convergence against transient HTTP failures.
//!
//! One setback per roughly 25 successes with `increase_after_successes = 5` gives about five
//! additive increases per cycle. The equilibrium `x * 0.9 + 5 = x` therefore produces a
//! saw-tooth around 45–50, comfortably inside the required `(8, 200)` interval. Using a threshold
//! of one would let the roughly 220-success retry tail climb all the way to the ceiling; a threshold
//! of five limits that tail to about 45 additional slots and keeps the final value below about 95.

use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate, matchers::path_regex};

struct Flaky {
    seen: Mutex<HashSet<String>>,
}

impl Respond for Flaky {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let path = request.url.path().to_owned();
        let index = path
            .rsplit('/')
            .next()
            .expect("matched paths contain an index")
            .parse::<usize>()
            .expect("path matcher guarantees a numeric index");

        if index % 25 == 0 {
            let first_attempt = self
                .seen
                .lock()
                .expect("flaky responder mutex poisoned")
                .insert(path);
            if first_attempt {
                return ResponseTemplate::new(500);
            }
        }

        ResponseTemplate::new(200).set_body_string("ok")
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    Mock::given(path_regex(r"^/page/\d+$"))
        .respond_with(Flaky {
            seen: Mutex::new(HashSet::new()),
        })
        .mount(&server)
        .await;

    let uri = server.uri();
    let start_urls: Vec<String> = (0..5000).map(|i| format!("{uri}/page/{i}")).collect();

    let crawler = millipede::Crawler::builder(millipede::HttpKind::builder().build()?)
        .storage_client(Arc::new(millipede::MemoryStorageClient::new()))
        .max_concurrency(200)
        .autoscale_mode(millipede::AutoscaleMode::Aimd {
            increase_after_successes: 5,
            decrease_factor: 0.9,
        })
        .min_concurrency(4)
        .desired_concurrency(4)
        .request_handler(|_ctx| async { Ok(()) })
        .failed_request_handler(|ctx: millipede::FailedRequestContext| async move {
            eprintln!("failed to crawl {}: {}", ctx.request.url, ctx.error);
            Ok(())
        })
        .build()
        .await?;

    let handle = crawler.handle();
    let samples = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(AtomicBool::new(false));
    let sampler_samples = Arc::clone(&samples);
    let sampler_done = Arc::clone(&done);
    let sampler = tokio::spawn(async move {
        let mut ticks = 0_u64;
        while !sampler_done.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if let Some(snapshot) = handle.autoscaler_snapshot() {
                sampler_samples
                    .lock()
                    .expect("sampler mutex poisoned")
                    .push(snapshot.desired_concurrency);
                ticks += 1;
                if ticks % 20 == 0 {
                    println!(
                        "desired={} [min={}, max={}]",
                        snapshot.desired_concurrency,
                        snapshot.min_concurrency,
                        snapshot.max_concurrency,
                    );
                }
            }
        }
    });

    let stats = crawler.run(start_urls).await?;
    let snapshot = crawler.autoscaler_snapshot();
    done.store(true, Ordering::Release);
    sampler.await?;

    let samples = samples.lock().expect("sampler mutex poisoned");
    anyhow::ensure!(!samples.is_empty(), "sampler did not record any snapshots");
    let last_quarter = &samples[samples.len() * 3 / 4..];
    let last_quarter_mean = last_quarter.iter().sum::<usize>() as f64 / last_quarter.len() as f64;
    let sampler_max = samples.iter().copied().max().unwrap_or_default();

    println!(
        "requests_finished={} requests_failed={} requests_retries={} final_desired={} \
         sampled_mean={last_quarter_mean:.1} sampled_max={sampler_max}",
        stats.requests_finished,
        stats.requests_failed,
        stats.requests_retries,
        snapshot.desired_concurrency,
    );

    anyhow::ensure!(
        stats.requests_finished == 5000,
        "expected 5000 finished requests, got {}",
        stats.requests_finished
    );
    anyhow::ensure!(
        stats.requests_failed == 0,
        "expected no failed requests, got {}",
        stats.requests_failed
    );
    anyhow::ensure!(
        stats.requests_retries >= 200,
        "expected at least 200 retries, got {}",
        stats.requests_retries
    );
    anyhow::ensure!(
        snapshot.desired_concurrency > 8 && snapshot.desired_concurrency < 200,
        "final desired concurrency {} is outside (8, 200)",
        snapshot.desired_concurrency
    );
    anyhow::ensure!(!snapshot.is_fixed, "autoscaler unexpectedly remained fixed");
    anyhow::ensure!(
        last_quarter_mean > 8.0 && last_quarter_mean < 200.0,
        "last-quarter mean {last_quarter_mean:.1} is outside (8, 200)"
    );
    anyhow::ensure!(
        sampler_max <= 200,
        "sampled desired concurrency exceeded the ceiling: {sampler_max}"
    );

    Ok(())
}
