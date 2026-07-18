//! Public retry-strategy integration behavior.

use futures_util::future::BoxFuture;
use http::StatusCode;
use millipede_core::prelude::*;
use millipede_storage_memory::MemoryStorageClient;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

#[derive(Clone)]
struct TestContext {
    request: Arc<Request>,
}

#[derive(Clone, Default)]
struct TestKind {
    overrides: Arc<Mutex<Vec<Option<String>>>>,
    observation: AttemptObservation,
}

impl CrawlerKind for TestKind {
    type Context = TestContext;

    fn execute<'a>(
        &'a self,
        env: RequestEnv<'a>,
    ) -> BoxFuture<'a, Result<Self::Context, CrawlError>> {
        self.overrides
            .lock()
            .unwrap()
            .push(env.overrides.user_agent_profile.clone());
        Box::pin(async move {
            Ok(TestContext {
                request: env.request,
            })
        })
    }

    fn observe(&self, _: &Self::Context) -> AttemptObservation {
        self.observation.clone()
    }

    fn cleanup(&self, _: RequestOutcome<Self::Context>) -> BoxFuture<'_, Result<(), CrawlError>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Copy)]
enum Script {
    RetryUa,
    Stop,
    Retry,
    Backoff,
    Rotate,
}

type SeenAttempts = Arc<Mutex<Vec<(u32, Option<StatusCode>)>>>;

struct RecordingStrategy {
    seen: SeenAttempts,
    cap: u32,
    script: Script,
}

impl RetryStrategy for RecordingStrategy {
    fn max_retries(&self) -> u32 {
        self.cap
    }
    fn on_retry(&self, outcome: &AttemptOutcome<'_>) -> RetryDirective {
        self.seen
            .lock()
            .unwrap()
            .push((outcome.attempt, outcome.status));
        match self.script {
            Script::RetryUa => RetryDirective::retry().user_agent_profile("Alt-UA/1.0"),
            Script::Stop => RetryDirective::stop(),
            Script::Retry => RetryDirective::retry(),
            Script::Backoff => RetryDirective::retry().backoff(Duration::from_secs(5)),
            Script::Rotate => RetryDirective::retry().session_action(SessionRetryAction::Rotate),
        }
    }
}

fn storage() -> Arc<dyn StorageClient> {
    Arc::new(MemoryStorageClient::new())
}

fn status_error() -> CrawlError {
    CrawlError::retry(HttpStatusError::new(StatusCode::INTERNAL_SERVER_ERROR))
}

#[tokio::test]
async fn directive_overrides_flow_between_attempts() -> Result<(), Box<dyn std::error::Error>> {
    let kind = TestKind::default();
    let overrides = kind.overrides.clone();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let crawler = Crawler::builder(kind)
        .storage_client(storage())
        .retry_strategy(RecordingStrategy {
            seen: seen.clone(),
            cap: 5,
            script: Script::RetryUa,
        })
        .request_handler(|ctx: TestContext| async move {
            if ctx.request.retry_count < 2 {
                Err(status_error())
            } else {
                Ok(())
            }
        })
        .build()
        .await?;
    let stats = crawler.run(["http://example.local/"]).await?;
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            (0, Some(StatusCode::INTERNAL_SERVER_ERROR)),
            (1, Some(StatusCode::INTERNAL_SERVER_ERROR))
        ]
    );
    assert_eq!(
        *overrides.lock().unwrap(),
        vec![None, Some("Alt-UA/1.0".into()), Some("Alt-UA/1.0".into())]
    );
    assert_eq!(stats.requests_finished, 1);
    assert_eq!(stats.requests_retries, 2);
    Ok(())
}

#[tokio::test]
async fn stop_directive_prevents_retry() -> Result<(), Box<dyn std::error::Error>> {
    let attempts = Arc::new(AtomicUsize::new(0));
    let failures = Arc::new(AtomicUsize::new(0));
    let crawler = Crawler::builder(TestKind::default())
        .storage_client(storage())
        .retry_strategy(RecordingStrategy {
            seen: Default::default(),
            cap: 5,
            script: Script::Stop,
        })
        .request_handler({
            let attempts = attempts.clone();
            move |_: TestContext| {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(status_error())
                }
            }
        })
        .failed_request_handler({
            let failures = failures.clone();
            move |_: FailedRequestContext| {
                let failures = failures.clone();
                async move {
                    failures.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
        })
        .build()
        .await?;
    let stats = crawler.run(["http://example.local/"]).await?;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(failures.load(Ordering::SeqCst), 1);
    assert_eq!(stats.requests_failed, 1);
    Ok(())
}

#[tokio::test]
async fn strategy_retry_cap_is_enforced() -> Result<(), Box<dyn std::error::Error>> {
    let attempts = Arc::new(AtomicUsize::new(0));
    let crawler = Crawler::builder(TestKind::default())
        .storage_client(storage())
        .retry_strategy(RecordingStrategy {
            seen: Default::default(),
            cap: 1,
            script: Script::Retry,
        })
        .request_handler({
            let attempts = attempts.clone();
            move |_: TestContext| {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(status_error())
                }
            }
        })
        .build()
        .await?;
    let stats = crawler.run(["http://example.local/"]).await?;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(stats.requests_retries, 1);
    assert_eq!(stats.requests_failed, 1);
    Ok(())
}

#[tokio::test]
async fn request_retry_cap_overrides_strategy_cap() -> Result<(), Box<dyn std::error::Error>> {
    let attempts = Arc::new(AtomicUsize::new(0));
    let crawler = Crawler::builder(TestKind::default())
        .storage_client(storage())
        .retry_strategy(RecordingStrategy {
            seen: Default::default(),
            cap: 1,
            script: Script::Retry,
        })
        .request_handler({
            let attempts = attempts.clone();
            move |_: TestContext| {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(status_error())
                }
            }
        })
        .build()
        .await?;
    let request = Request::get("http://example.local/")
        .max_retries(3)
        .build()?;
    let stats = crawler.run([request]).await?;
    assert_eq!(attempts.load(Ordering::SeqCst), 4);
    assert_eq!(stats.requests_retries, 3);
    assert_eq!(stats.requests_failed, 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn backoff_delays_next_attempt() -> Result<(), Box<dyn std::error::Error>> {
    let started = tokio::time::Instant::now();
    let crawler = Crawler::builder(TestKind::default())
        .storage_client(storage())
        .retry_strategy(RecordingStrategy {
            seen: Default::default(),
            cap: 1,
            script: Script::Backoff,
        })
        .request_handler(|ctx: TestContext| async move {
            if ctx.request.retry_count == 0 {
                Err(status_error())
            } else {
                Ok(())
            }
        })
        .build()
        .await?;
    let _ = crawler.run(["http://example.local/"]).await?;
    assert!(started.elapsed() >= Duration::from_secs(5));
    Ok(())
}

#[tokio::test]
async fn rotate_counts_as_session_rotation() -> Result<(), Box<dyn std::error::Error>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let crawler = Crawler::builder(TestKind::default())
        .storage_client(storage())
        .max_session_rotations(1)
        .retry_strategy(RecordingStrategy {
            seen: seen.clone(),
            cap: 5,
            script: Script::Rotate,
        })
        .request_handler(|_: TestContext| async move {
            Err(CrawlError::retry(anyhow::anyhow!("again")))
        })
        .build()
        .await?;
    let mut results = crawler.results();
    let stats = crawler.run(["http://example.local/"]).await?;
    let handled = results.recv().await?;
    assert_eq!(stats.requests_failed, 1);
    assert_eq!(handled.request.session_rotation_count, 1);
    assert_eq!(handled.request.retry_count, 0);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![(0, None), (0, None)],
        "attempt must equal retry_count and exclude session rotations"
    );
    Ok(())
}

#[tokio::test]
async fn observation_feeds_result_and_statistics() -> Result<(), Box<dyn std::error::Error>> {
    let loaded = url::Url::parse("http://example.local/final")?;
    let mut observation = AttemptObservation::default();
    observation.status = Some(StatusCode::OK);
    observation.loaded_url = Some(loaded.clone());
    observation.response_bytes = Some(123);
    let kind = TestKind {
        observation,
        ..Default::default()
    };
    let crawler = Crawler::builder(kind)
        .storage_client(storage())
        .request_handler(|_: TestContext| async { Ok(()) })
        .build()
        .await?;
    let mut results = crawler.results();
    let stats = crawler.run(["http://example.local/"]).await?;
    let handled = results.recv().await?;
    assert_eq!(handled.response_status, Some(StatusCode::OK));
    assert_eq!(handled.loaded_url, Some(loaded));
    assert_eq!(stats.status_codes.get(&200), Some(&1));
    Ok(())
}
