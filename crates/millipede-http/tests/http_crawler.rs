//! End-to-end tests for the HTTP crawler kind.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use http::StatusCode;
use millipede_core::{
    config::Configuration,
    crawler::Crawler,
    handler::FailedRequestContext,
    proxy::{ProxyBuckets, ProxyConfiguration, ProxyKind, ProxyRouteContext, ProxyStrategy},
    request::Request,
    retry_strategy::{AttemptOutcome, RetryDirective, RetryStrategy},
    session::{SESSION_POOL_PERSIST_KEY, SessionPool, SessionPoolOptions},
    storage::StorageClient,
};
use millipede_http::{HttpContext, HttpKind};
use millipede_storage_memory::MemoryStorageClient;
use url::Url;
use wiremock::{
    Mock, MockServer, Respond, ResponseTemplate,
    matchers::{any, header, path},
};

fn url(server: &MockServer, path: &str) -> Url {
    Url::parse(&format!("{}{path}", server.uri())).expect("mock URL must parse")
}

#[derive(Clone)]
struct RetryOnceResponder {
    arrivals: Arc<Mutex<Vec<Instant>>>,
}

impl Respond for RetryOnceResponder {
    fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
        let mut arrivals = self.arrivals.lock().expect("arrivals mutex poisoned");
        arrivals.push(Instant::now());
        if arrivals.len() == 1 {
            ResponseTemplate::new(429).insert_header("Retry-After", "3")
        } else {
            ResponseTemplate::new(200)
        }
    }
}

#[tokio::test]
async fn happy_path_observes_all_five_responses() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let mut starts = Vec::new();
    for index in 0..5 {
        let page = format!("/page-{index}");
        Mock::given(path(page.clone()))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        starts.push(url(&server, &page));
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let crawler = Crawler::builder(HttpKind::builder().build()?)
        .request_handler({
            let seen = Arc::clone(&seen);
            move |ctx: HttpContext| {
                let seen = Arc::clone(&seen);
                async move {
                    seen.lock()
                        .expect("seen mutex poisoned")
                        .push((ctx.request.url.clone(), ctx.response.status));
                    Ok(())
                }
            }
        })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;
    let mut results = crawler.results();
    let stats = crawler.run(starts).await?;

    assert_eq!(stats.requests_finished, 5);
    assert_eq!(seen.lock().expect("seen mutex poisoned").len(), 5);
    assert!(
        seen.lock()
            .expect("seen mutex poisoned")
            .iter()
            .all(|(_, status)| *status == StatusCode::OK)
    );
    for _ in 0..5 {
        assert_eq!(results.recv().await?.response_status, Some(StatusCode::OK));
    }
    Ok(())
}

#[tokio::test]
async fn roadmap_status_matrix() -> Result<(), Box<dyn std::error::Error>> {
    let flaky = MockServer::start().await;
    Mock::given(path("/flaky"))
        .respond_with(ResponseTemplate::new(429))
        .up_to_n_times(1)
        .mount(&flaky)
        .await;
    Mock::given(path("/flaky"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&flaky)
        .await;
    let crawler = Crawler::builder(HttpKind::builder().build()?)
        .request_handler(|_: HttpContext| async { Ok(()) })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;
    let stats = crawler.run([url(&flaky, "/flaky")]).await?;
    assert_eq!(stats.requests_finished, 1);
    assert!(stats.requests_retries >= 1);

    let missing = MockServer::start().await;
    Mock::given(path("/missing"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&missing)
        .await;
    let failures = Arc::new(Mutex::new(0_u32));
    let crawler = Crawler::builder(HttpKind::builder().build()?)
        .request_handler(|_: HttpContext| async { Ok(()) })
        .failed_request_handler({
            let failures = Arc::clone(&failures);
            move |_: FailedRequestContext| {
                let failures = Arc::clone(&failures);
                async move {
                    *failures.lock().expect("failure mutex poisoned") += 1;
                    Ok(())
                }
            }
        })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;
    let stats = crawler.run([url(&missing, "/missing")]).await?;
    assert_eq!(stats.requests_failed, 1);
    assert_eq!(*failures.lock().expect("failure mutex poisoned"), 1);
    missing.verify().await;

    let forbidden = MockServer::start().await;
    Mock::given(path("/forbidden"))
        .respond_with(ResponseTemplate::new(403))
        .up_to_n_times(1)
        .mount(&forbidden)
        .await;
    Mock::given(path("/forbidden"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&forbidden)
        .await;
    let crawler = Crawler::builder(HttpKind::builder().build()?)
        .request_handler(|_: HttpContext| async { Ok(()) })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;
    let mut results = crawler.results();
    let stats = crawler.run([url(&forbidden, "/forbidden")]).await?;
    let terminal = results.recv().await?;
    assert_eq!(stats.requests_finished, 1);
    assert_eq!(terminal.request.session_rotation_count, 1);
    assert_eq!(terminal.request.retry_count, 0);
    Ok(())
}

#[tokio::test]
async fn retry_after_reaches_failed_request_handler() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(path("/limited"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "3"))
        .mount(&server)
        .await;
    let captured = Arc::new(Mutex::new(None));
    let crawler = Crawler::builder(HttpKind::builder().build()?)
        .max_request_retries(0)
        .request_handler(|_: HttpContext| async { Ok(()) })
        .failed_request_handler({
            let captured = Arc::clone(&captured);
            move |ctx: FailedRequestContext| {
                let captured = Arc::clone(&captured);
                async move {
                    *captured.lock().expect("captured mutex poisoned") = ctx.error.retry_after();
                    Ok(())
                }
            }
        })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;

    let stats = crawler.run([url(&server, "/limited")]).await?;

    assert_eq!(
        *captured.lock().expect("captured mutex poisoned"),
        Some(Duration::from_secs(3))
    );
    assert_eq!(stats.requests_failed, 1);
    Ok(())
}

#[tokio::test]
async fn retry_after_delays_the_retry_end_to_end() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let arrivals = Arc::new(Mutex::new(Vec::new()));
    Mock::given(path("/limited"))
        .respond_with(RetryOnceResponder {
            arrivals: Arc::clone(&arrivals),
        })
        .mount(&server)
        .await;
    let crawler = Crawler::builder(HttpKind::builder().build()?)
        .max_request_retries(2)
        .request_handler(|_: HttpContext| async { Ok(()) })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;

    let stats = crawler.run([url(&server, "/limited")]).await?;

    let arrivals = arrivals.lock().expect("arrivals mutex poisoned");
    assert_eq!(arrivals.len(), 2);
    assert!(arrivals[1].duration_since(arrivals[0]) >= Duration::from_secs(3));
    assert_eq!(stats.requests_finished, 1);
    Ok(())
}

#[tokio::test]
async fn redirect_cookie_is_reused_by_enqueued_request() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(path("/set-cookie"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("Location", "/landing")
                .insert_header("Set-Cookie", "tok=42; Path=/"),
        )
        .mount(&server)
        .await;
    Mock::given(path("/landing"))
        .respond_with(ResponseTemplate::new(200).set_body_string("redirect-complete"))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/needs-cookie"))
        .and(header("cookie", "tok=42"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let needs_cookie = url(&server, "/needs-cookie");
    let crawler = Crawler::builder(
        HttpKind::builder()
            .session_pool(SessionPoolOptions::default().with_max_pool_size(1))
            .build()?,
    )
    .request_handler(move |ctx: HttpContext| {
        let needs_cookie = needs_cookie.clone();
        async move {
            if ctx.response.url.path() == "/landing"
                && ctx.response.body.as_ref() == b"redirect-complete"
            {
                ctx.enqueue.urls([needs_cookie]).await?;
            }
            Ok(())
        }
    })
    .storage_client(Arc::new(MemoryStorageClient::new()))
    .build()
    .await?;
    let stats = crawler.run([url(&server, "/set-cookie")]).await?;
    assert_eq!(stats.requests_finished, 2);
    assert_eq!(stats.requests_failed, 0);
    server.verify().await;
    Ok(())
}

struct SwapUserAgent;

impl RetryStrategy for SwapUserAgent {
    fn max_retries(&self) -> u32 {
        1
    }

    fn on_retry(&self, _: &AttemptOutcome<'_>) -> RetryDirective {
        RetryDirective::retry().user_agent_profile("Alt-UA/1.0")
    }
}

#[tokio::test]
async fn retry_strategy_swaps_user_agent() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(path("/ua"))
        .and(header("user-agent", "Alt-UA/1.0"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/ua"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&server)
        .await;
    let crawler = Crawler::builder(HttpKind::builder().build()?)
        .retry_strategy(SwapUserAgent)
        .request_handler(|_: HttpContext| async { Ok(()) })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;
    let stats = crawler.run([url(&server, "/ua")]).await?;
    assert_eq!(stats.requests_finished, 1);
    assert_eq!(stats.requests_retries, 1);
    server.verify().await;
    Ok(())
}

struct MediaStrategy;

impl ProxyStrategy for MediaStrategy {
    fn route(&self, context: &ProxyRouteContext<'_>) -> ProxyKind {
        if context.request.url.path().ends_with(".jpg") {
            ProxyKind::MediaAsset
        } else {
            ProxyKind::Default
        }
    }
}

#[tokio::test]
async fn proxy_strategy_routes_only_media_bucket() -> Result<(), Box<dyn std::error::Error>> {
    let site = MockServer::start().await;
    Mock::given(path("/page"))
        .respond_with(ResponseTemplate::new(200).set_body_string("direct"))
        .expect(1)
        .mount(&site)
        .await;
    let proxy = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(200).set_body_string("via-media"))
        .expect(1)
        .mount(&proxy)
        .await;
    let buckets = ProxyBuckets::new()
        .with_media(ProxyConfiguration::round_robin([Url::parse(&proxy.uri())?]));
    let crawler = Crawler::builder(
        HttpKind::builder()
            .proxy_buckets(buckets)
            .proxy_strategy(MediaStrategy)
            .build()?,
    )
    .request_handler(|_: HttpContext| async { Ok(()) })
    .storage_client(Arc::new(MemoryStorageClient::new()))
    .build()
    .await?;
    let stats = crawler
        .run([
            url(&site, "/page"),
            Url::parse("http://media-probe.invalid/x.jpg")?,
        ])
        .await?;
    assert_eq!(stats.requests_finished, 2);
    site.verify().await;
    proxy.verify().await;
    Ok(())
}

#[tokio::test]
async fn stop_persists_default_session_pool() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(path("/page"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let configuration = Configuration::builder().build()?;
    let default_kvs_id = configuration.default_key_value_store_id().to_owned();
    let storage = Arc::new(MemoryStorageClient::new());
    let crawler = Crawler::builder(HttpKind::builder().build()?)
        .request_handler(|_: HttpContext| async { Ok(()) })
        .configuration(configuration)
        .storage_client(storage.clone())
        .build()
        .await?;
    crawler.run([url(&server, "/page")]).await?;

    let kvs = storage.open_key_value_store(Some(&default_kvs_id)).await?;
    let persisted = kvs
        .get_bytes(SESSION_POOL_PERSIST_KEY)
        .await?
        .expect("stop must persist the session pool to the configured default KVS");
    let state: serde_json::Value = serde_json::from_slice(&persisted.value)?;
    assert_eq!(state["sessions"].as_array().map(Vec::len), Some(1));
    assert_eq!(state["sessions"][0]["usage_count"], 1);
    Ok(())
}

#[tokio::test]
async fn shared_session_pool_is_used_but_not_persisted() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(path("/page"))
        .respond_with(ResponseTemplate::new(200).insert_header("Set-Cookie", "shared=1; Path=/"))
        .mount(&server)
        .await;
    let configuration = Configuration::builder().build()?;
    let default_kvs_id = configuration.default_key_value_store_id().to_owned();
    let storage = Arc::new(MemoryStorageClient::new());
    let pool = Arc::new(SessionPool::new(
        SessionPoolOptions::default().with_max_pool_size(1),
    ));
    let crawler = Crawler::builder(
        HttpKind::builder()
            .shared_session_pool(pool.clone())
            .build()?,
    )
    .request_handler(|_: HttpContext| async { Ok(()) })
    .configuration(configuration)
    .storage_client(storage.clone())
    .build()
    .await?;

    crawler.run([url(&server, "/page")]).await?;

    let session = pool.get_session(None).await;
    assert!(session.cookie_jar().cookie_count() > 0);
    let kvs = storage.open_key_value_store(Some(&default_kvs_id)).await?;
    assert!(kvs.get_bytes(SESSION_POOL_PERSIST_KEY).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn sessions_can_be_disabled() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(path("/page"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let saw_none = Arc::new(Mutex::new(false));
    let crawler = Crawler::builder(HttpKind::builder().disable_sessions().build()?)
        .request_handler({
            let saw_none = Arc::clone(&saw_none);
            move |ctx: HttpContext| {
                let saw_none = Arc::clone(&saw_none);
                async move {
                    *saw_none.lock().expect("session mutex poisoned") = ctx.session.is_none();
                    Ok(())
                }
            }
        })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;
    let stats = crawler.run([url(&server, "/page")]).await?;
    assert_eq!(stats.requests_finished, 1);
    assert!(*saw_none.lock().expect("session mutex poisoned"));
    Ok(())
}

#[tokio::test]
async fn coalescing_is_an_opt_in_smoke_test() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    Mock::given(path("/page"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let crawler = Crawler::builder(HttpKind::builder().coalesce_in_flight(true).build()?)
        .request_handler(|_: HttpContext| async { Ok(()) })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;
    let stats = crawler
        .run([Request::get(url(&server, "/page")).build()?])
        .await?;
    assert_eq!(stats.requests_finished, 1);
    Ok(())
}
