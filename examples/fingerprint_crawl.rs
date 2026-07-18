//! Demonstrates deterministic browser-like headers, anti-bot detection, normalized error
//! statistics, and failure snapshots against a fully offline mock site. Run it with
//! `cargo run -p millipede --features http,fingerprint,storage-memory --example fingerprint_crawl`.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use millipede::StorageClient;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate, matchers::path};

const DETECTED_CHALLENGE_BODY: &str = "<html><title>Just a moment...</title><body>Checking your browser before accessing the site.</body></html>";
const CHALLENGE_BODY: &str =
    "<html><body>The Cloudflare challenge route recovered after session rotation.</body></html>";

struct ChallengeThenRecovery {
    attempts: AtomicUsize,
}

impl Respond for ChallengeThenRecovery {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            ResponseTemplate::new(200).set_body_raw(DETECTED_CHALLENGE_BODY, "text/html")
        } else {
            ResponseTemplate::new(200).set_body_raw(CHALLENGE_BODY, "text/html")
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    Mock::given(path("/normal"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw("<html><body>Ready to crawl.</body></html>", "text/html"),
        )
        .mount(&server)
        .await;
    Mock::given(path("/challenge"))
        .respond_with(ChallengeThenRecovery {
            attempts: AtomicUsize::new(0),
        })
        .mount(&server)
        .await;

    let normal_url = format!("{}/normal", server.uri());
    let challenge_url = format!("{}/challenge", server.uri());
    let normal_request = millipede::Request::get(normal_url).build()?;
    let challenge_request = millipede::Request::get(challenge_url).build()?;
    let snapshot_key = format!(
        "{}.body",
        millipede::ErrorSnapshotter::base_key(&challenge_request)
    );
    let storage = Arc::new(millipede::MemoryStorageClient::new());
    let crawler = millipede::Crawler::builder(
        millipede::HttpKind::builder()
            .header_generator(true)
            .detect_anti_bot_default()
            .snapshot_errors_on_failure(true)
            .build()?,
    )
    .max_request_retries(0)
    .max_session_rotations(1)
    .storage_client(storage.clone())
    .request_handler(|ctx: millipede::HttpContext| async move {
        if ctx.request.url.path() == "/challenge" {
            return Err(millipede::CrawlError::non_retryable(anyhow::anyhow!(
                "intentional handler failure after recovering {}",
                ctx.request.url
            )));
        }
        Ok(())
    })
    .failed_request_handler(|ctx: millipede::FailedRequestContext| async move {
        eprintln!("failed to crawl {}: {}", ctx.request.url, ctx.error);
        Ok(())
    })
    .build()
    .await?;

    let stats = crawler.run([normal_request, challenge_request]).await?;
    let kvs = storage.open_key_value_store(Some("default")).await?;
    let snapshot = millipede::ErrorSnapshotter::new(kvs)
        .load(&snapshot_key)
        .await?
        .ok_or_else(|| anyhow::anyhow!("challenge response snapshot was not persisted"))?;
    anyhow::ensure!(
        snapshot.content_type == "text/html",
        "unexpected snapshot content type: {}",
        snapshot.content_type
    );
    anyhow::ensure!(
        snapshot.bytes.as_ref() == CHALLENGE_BODY.as_bytes(),
        "snapshot did not match the recovered challenge-route response body"
    );
    anyhow::ensure!(
        stats.requests_finished == 1 && stats.requests_failed == 1 && stats.requests_retries == 1,
        "unexpected final request counts: {stats:#?}"
    );
    anyhow::ensure!(
        stats
            .errors
            .keys()
            .any(|error| error.contains("intentional handler failure")),
        "terminal error groups did not include the handler failure: {:#?}",
        stats.errors
    );
    anyhow::ensure!(
        stats
            .retry_errors
            .keys()
            .any(|error| error.contains("anti-bot detected: Cloudflare")),
        "retry error groups did not include the anti-bot retry: {:#?}",
        stats.retry_errors
    );
    println!(
        "recovered snapshot: content_type={} bytes={}",
        snapshot.content_type,
        snapshot.bytes.len()
    );
    println!(
        "FinalStatistics: requests_finished={} requests_failed={} requests_retries={} errors={:#?} retry_errors={:#?}",
        stats.requests_finished,
        stats.requests_failed,
        stats.requests_retries,
        stats.errors,
        stats.retry_errors
    );

    let generator = millipede::HeaderGenerator::new();
    let first_headers = generator.generate("demo-session");
    let second_headers = generator.generate("demo-session");
    anyhow::ensure!(
        first_headers.user_agent == second_headers.user_agent,
        "the same session token produced different user agents"
    );
    println!("{}", first_headers.user_agent);
    println!("{}", second_headers.user_agent);
    Ok(())
}
