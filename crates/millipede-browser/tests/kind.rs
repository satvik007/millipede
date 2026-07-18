#![allow(missing_docs)]

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use millipede_browser::{
    BrowserContext, BrowserError, BrowserHooks, BrowserKind, BrowserPage, BrowserPoolOptions,
    BrowserProvider, BrowserResponse, GotoOptions, LaunchContext, ScreenshotOptions,
};
use millipede_core::{
    autoscale::AutoscaledPoolOptions,
    cookies::Cookie,
    crawler::Crawler,
    errors::CrawlError,
    handler::FailedRequestContext,
    request::Request,
    session::{SESSION_POOL_PERSIST_KEY, Session, SessionConfig, SessionPool, SessionPoolOptions},
    snapshot::ErrorSnapshotter,
    storage::{KeyValueStoreExt, StorageClient},
};
use millipede_storage_memory::MemoryStorageClient;

#[derive(Clone, Default)]
struct FakeBehavior {
    status: Option<u16>,
    anchors: Vec<String>,
    surface_cookies: Vec<Cookie>,
    hang: bool,
}

#[derive(Default)]
struct FakeStats {
    browsers_launched: usize,
    pages_created: usize,
    pages_closed: usize,
    browsers_closed: usize,
    page_browser_ids: Vec<u64>,
    closed_browser_ids: Vec<u64>,
    open_pages: i64,
    gotos: Vec<String>,
    set_cookie_calls: Vec<(u64, Vec<Cookie>)>,
    set_header_calls: Vec<(u64, http::HeaderMap)>,
}

#[derive(Clone)]
struct FakeProvider {
    stats: Arc<Mutex<FakeStats>>,
    behaviors: Arc<HashMap<String, FakeBehavior>>,
}

impl FakeProvider {
    fn new(behaviors: HashMap<String, FakeBehavior>) -> Self {
        Self {
            stats: Arc::new(Mutex::new(FakeStats::default())),
            behaviors: Arc::new(behaviors),
        }
    }
}

struct FakeBrowser {
    id: u64,
}

#[derive(Clone)]
struct FakePage {
    serial: u64,
    stats: Arc<Mutex<FakeStats>>,
    behaviors: Arc<HashMap<String, FakeBehavior>>,
    behavior: Arc<Mutex<FakeBehavior>>,
}

#[async_trait::async_trait]
impl BrowserPage for FakePage {
    async fn goto(
        &self,
        url: &url::Url,
        _opts: GotoOptions,
    ) -> Result<Option<BrowserResponse>, BrowserError> {
        let behavior = self
            .behaviors
            .get(url.as_str())
            .cloned()
            .unwrap_or_default();
        self.stats
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .gotos
            .push(url.to_string());
        *self
            .behavior
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = behavior.clone();
        if behavior.hang {
            futures_util::future::pending::<()>().await;
        }
        let mut response = BrowserResponse::default();
        response.status = behavior.status.map(|status| {
            http::StatusCode::from_u16(status).expect("fake status code must be valid")
        });
        response.url = Some(url.clone());
        Ok(Some(response))
    }

    async fn content(&self) -> Result<String, BrowserError> {
        Ok("<html></html>".to_owned())
    }

    async fn evaluate_js(&self, _script: &str) -> Result<serde_json::Value, BrowserError> {
        Ok(serde_json::Value::Null)
    }

    async fn evaluate_anchors(
        &self,
        _selector: Option<&str>,
    ) -> Result<Vec<url::Url>, BrowserError> {
        self.behavior
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .anchors
            .iter()
            .map(|anchor| {
                url::Url::parse(anchor).map_err(|error| BrowserError::Evaluation(error.into()))
            })
            .collect()
    }

    async fn cookies(&self) -> Result<Vec<Cookie>, BrowserError> {
        Ok(self
            .behavior
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .surface_cookies
            .clone())
    }

    async fn set_cookies(&self, cookies: &[Cookie]) -> Result<(), BrowserError> {
        self.stats
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .set_cookie_calls
            .push((self.serial, cookies.to_vec()));
        Ok(())
    }

    async fn set_extra_headers(&self, headers: &http::HeaderMap) -> Result<(), BrowserError> {
        self.stats
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .set_header_calls
            .push((self.serial, headers.clone()));
        Ok(())
    }

    async fn wait_for_selector(
        &self,
        _selector: &str,
        _timeout: Duration,
    ) -> Result<(), BrowserError> {
        Ok(())
    }

    async fn click(&self, _selector: &str) -> Result<(), BrowserError> {
        Ok(())
    }

    async fn screenshot(&self, _opts: ScreenshotOptions) -> Result<bytes::Bytes, BrowserError> {
        Ok(bytes::Bytes::new())
    }
}

#[async_trait::async_trait]
impl BrowserProvider for FakeProvider {
    type Browser = FakeBrowser;
    type Page = FakePage;
    type LaunchOptions = ();

    async fn launch(
        &self,
        _opts: Self::LaunchOptions,
        _ctx: &LaunchContext,
    ) -> Result<Self::Browser, BrowserError> {
        let id = {
            let mut stats = self.stats.lock().unwrap_or_else(|error| error.into_inner());
            stats.browsers_launched += 1;
            stats.browsers_launched as u64
        };
        Ok(FakeBrowser { id })
    }

    async fn new_page(&self, browser: &Self::Browser) -> Result<Self::Page, BrowserError> {
        let serial = {
            let mut stats = self.stats.lock().unwrap_or_else(|error| error.into_inner());
            stats.pages_created += 1;
            stats.open_pages += 1;
            stats.page_browser_ids.push(browser.id);
            stats.pages_created as u64
        };
        Ok(FakePage {
            serial,
            stats: Arc::clone(&self.stats),
            behaviors: Arc::clone(&self.behaviors),
            behavior: Arc::new(Mutex::new(FakeBehavior::default())),
        })
    }

    async fn close_page(&self, _page: Self::Page) -> Result<(), BrowserError> {
        let mut stats = self.stats.lock().unwrap_or_else(|error| error.into_inner());
        stats.pages_closed += 1;
        stats.open_pages -= 1;
        Ok(())
    }

    async fn close_browser(&self, browser: Self::Browser) -> Result<(), BrowserError> {
        let mut stats = self.stats.lock().unwrap_or_else(|error| error.into_inner());
        stats.browsers_closed += 1;
        stats.closed_browser_ids.push(browser.id);
        Ok(())
    }
}

fn storage() -> Arc<dyn StorageClient> {
    Arc::new(MemoryStorageClient::new())
}

fn kind(provider: FakeProvider) -> BrowserKind<FakeProvider> {
    BrowserKind::builder(provider).build().unwrap()
}

async fn wait_for_closed_pages(provider: &FakeProvider, expected: usize) -> bool {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if provider
                .stats
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .pages_closed
                == expected
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

#[tokio::test]
async fn browser_crawler_crawls_and_enqueues_dom_links() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let root = "https://example.com/";
        let provider = FakeProvider::new(HashMap::from([(
            root.to_owned(),
            FakeBehavior {
                anchors: vec![
                    "https://example.com/a".to_owned(),
                    "https://example.com/b".to_owned(),
                ],
                ..FakeBehavior::default()
            },
        )]));
        let crawler = Crawler::builder(kind(provider.clone()))
            .storage_client(storage())
            .request_handler(|ctx: BrowserContext| async move {
                let _ = ctx.enqueue.all().await?;
                Ok(())
            })
            .build()
            .await
            .unwrap();

        let stats = crawler.run([root]).await.unwrap();
        assert_eq!(stats.requests_finished, 3);
        let fake = provider
            .stats
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        assert_eq!(fake.gotos.len(), 3);
        assert_eq!(fake.pages_closed, 3);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn engine_retires_browser_after_configured_page_count() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new(HashMap::new());
        let browser_kind = BrowserKind::builder(provider.clone())
            .max_open_pages_per_browser(1)
            .retire_browser_after_page_count(2)
            .build()
            .unwrap();
        let crawler = Crawler::builder(browser_kind)
            .storage_client(storage())
            .max_concurrency(1)
            .request_handler(|_ctx: BrowserContext| async { Ok(()) })
            .build()
            .await
            .unwrap();

        let stats = crawler
            .run([
                "https://example.com/1",
                "https://example.com/2",
                "https://example.com/3",
                "https://example.com/4",
                "https://example.com/5",
            ])
            .await
            .unwrap();

        assert_eq!(stats.requests_finished, 5);
        let fake = provider
            .stats
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        assert_eq!(fake.browsers_launched, 3);
        assert_eq!(fake.page_browser_ids, vec![1, 1, 2, 2, 3]);
        assert_eq!(fake.closed_browser_ids, vec![1, 2, 3]);
        assert_eq!(fake.pages_closed, 5);
        assert_eq!(fake.open_pages, 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cookie_persistence_across_page_recycles() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let first = "https://example.com/1";
        let second = "https://example.com/2";
        let provider = FakeProvider::new(HashMap::from([(
            first.to_owned(),
            FakeBehavior {
                surface_cookies: vec![Cookie::new("k", "v", "example.com")],
                ..FakeBehavior::default()
            },
        )]));
        let browser_kind = BrowserKind::builder(provider.clone())
            .max_open_pages_per_browser(1)
            .session_pool(SessionPoolOptions::default().with_max_pool_size(1))
            .build()
            .unwrap();
        let crawler = Crawler::builder(browser_kind)
            .storage_client(storage())
            .max_concurrency(1)
            .request_handler(|_ctx: BrowserContext| async { Ok(()) })
            .build()
            .await
            .unwrap();

        let stats = crawler.run([first, second]).await.unwrap();
        assert_eq!(stats.requests_finished, 2);
        let fake = provider
            .stats
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        assert!(fake.set_cookie_calls.iter().any(|(serial, cookies)| {
            *serial == 2
                && cookies
                    .iter()
                    .any(|cookie| cookie.name == "k" && cookie.value == "v")
        }));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn fingerprint_hook_installs_headers() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new(HashMap::new());
        let hooks = BrowserHooks::defaults().with_fingerprint(Arc::new(
            millipede_fingerprint::BrowserFingerprintGenerator::new(),
        ));
        let browser_kind = BrowserKind::builder(provider.clone())
            .pool_options(BrowserPoolOptions::default().with_hooks(hooks))
            .build()
            .unwrap();
        let crawler = Crawler::builder(browser_kind)
            .storage_client(storage())
            .request_handler(|_ctx: BrowserContext| async { Ok(()) })
            .build()
            .await
            .unwrap();

        let _ = crawler
            .run(["https://example.com/fingerprint"])
            .await
            .unwrap();

        let fake = provider
            .stats
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        assert!(!fake.set_header_calls.is_empty());
        assert!(
            fake.set_header_calls
                .iter()
                .any(|(_, headers)| headers.contains_key(http::header::USER_AGENT))
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn pre_navigation_hook_runs() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new(HashMap::new());
        let calls = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let observed = Arc::clone(&calls);
        let browser_kind = BrowserKind::builder(provider)
            .pre_navigation_hook(move |_ctx| {
                let observed = Arc::clone(&observed);
                Box::pin(async move {
                    observed
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .push("pre");
                    Ok(())
                })
            })
            .build()
            .unwrap();
        let crawler = Crawler::builder(browser_kind)
            .storage_client(storage())
            .request_handler(|_ctx: BrowserContext| async { Ok(()) })
            .build()
            .await
            .unwrap();

        let _ = crawler.run(["https://example.com/pre-hook"]).await.unwrap();

        assert_eq!(
            *calls.lock().unwrap_or_else(|error| error.into_inner()),
            vec!["pre"]
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn navigation_hooks_run_in_order_and_errors_short_circuit() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let provider = FakeProvider::new(HashMap::new());
        let browser_kind = BrowserKind::builder(provider)
            .pre_navigation_hook({
                let calls = Arc::clone(&calls);
                move |_| {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move {
                        calls
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .push("pre-1");
                        Ok(())
                    })
                }
            })
            .pre_navigation_hook({
                let calls = Arc::clone(&calls);
                move |_| {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move {
                        calls
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .push("pre-2");
                        Ok(())
                    })
                }
            })
            .post_navigation_hook({
                let calls = Arc::clone(&calls);
                move |_| {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move {
                        calls
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .push("post-1");
                        Ok(())
                    })
                }
            })
            .post_navigation_hook({
                let calls = Arc::clone(&calls);
                move |_| {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move {
                        calls
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .push("post-2");
                        Err(CrawlError::non_retryable(anyhow::anyhow!(
                            "stop navigation hooks"
                        )))
                    })
                }
            })
            .post_navigation_hook({
                let calls = Arc::clone(&calls);
                move |_| {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move {
                        calls
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .push("post-3");
                        Ok(())
                    })
                }
            })
            .build()
            .unwrap();
        let crawler = Crawler::builder(browser_kind)
            .storage_client(storage())
            .max_request_retries(0)
            .request_handler({
                let calls = Arc::clone(&calls);
                move |_ctx: BrowserContext| {
                    let calls = Arc::clone(&calls);
                    async move {
                        calls
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .push("handler");
                        Ok(())
                    }
                }
            })
            .build()
            .await
            .unwrap();

        let stats = crawler
            .run(["https://example.com/ordered-hooks"])
            .await
            .unwrap();

        assert_eq!(stats.requests_failed, 1);
        assert_eq!(
            *calls.lock().unwrap_or_else(|error| error.into_inner()),
            vec!["pre-1", "pre-2", "post-1", "post-2"]
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn post_navigation_hook_error_closes_page_and_fails() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new(HashMap::new());
        let failed = Arc::new(Mutex::new(false));
        let observed = Arc::clone(&failed);
        let browser_kind = BrowserKind::builder(provider.clone())
            .post_navigation_hook(|_ctx| {
                Box::pin(async { Err(CrawlError::non_retryable(anyhow::anyhow!("blocked"))) })
            })
            .build()
            .unwrap();
        let crawler = Crawler::builder(browser_kind)
            .storage_client(storage())
            .request_handler(|_ctx: BrowserContext| async { Ok(()) })
            .failed_request_handler(move |_ctx: FailedRequestContext| {
                let observed = Arc::clone(&observed);
                async move {
                    *observed.lock().unwrap_or_else(|error| error.into_inner()) = true;
                    Ok(())
                }
            })
            .build()
            .await
            .unwrap();

        let stats = crawler
            .run(["https://example.com/post-hook"])
            .await
            .unwrap();

        assert_eq!(stats.requests_failed, 1);
        assert!(*failed.lock().unwrap_or_else(|error| error.into_inner()));
        assert!(
            provider
                .stats
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .pages_closed
                > 0
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn snapshot_errors_persists_html_and_png() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let storage = storage();
        let request = Request::get("https://example.com/snapshot")
            .build()
            .unwrap();
        let base_key = ErrorSnapshotter::base_key(&request);
        let browser_kind = BrowserKind::builder(FakeProvider::new(HashMap::new()))
            .snapshot_errors_on_failure(true)
            .build()
            .unwrap();
        let crawler = Crawler::builder(browser_kind)
            .storage_client(Arc::clone(&storage))
            .max_request_retries(0)
            .request_handler(|_ctx: BrowserContext| async {
                Err(CrawlError::non_retryable(anyhow::anyhow!("handler failed")))
            })
            .build()
            .await
            .unwrap();

        let stats = crawler.run([request]).await.unwrap();
        assert_eq!(stats.requests_failed, 1);

        let kvs = storage.open_key_value_store(Some("default")).await.unwrap();
        let snapshotter = ErrorSnapshotter::new(kvs);
        let html = snapshotter
            .load(&format!("{base_key}.html"))
            .await
            .unwrap()
            .expect("HTML snapshot should exist");
        let png = snapshotter
            .load(&format!("{base_key}.png"))
            .await
            .unwrap()
            .expect("PNG snapshot should exist");
        assert_eq!(html.bytes, bytes::Bytes::from_static(b"<html></html>"));
        assert_eq!(html.content_type, "text/html");
        assert_eq!(png.bytes, bytes::Bytes::new());
        assert_eq!(png.content_type, "image/png");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn session_status_code_rotates_session() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let blocked = "https://example.com/blocked";
        let provider = FakeProvider::new(HashMap::from([(
            blocked.to_owned(),
            FakeBehavior {
                status: Some(403),
                ..FakeBehavior::default()
            },
        )]));
        let saw_session_error = Arc::new(Mutex::new(false));
        let page_closed_before_stop = Arc::new(Mutex::new(false));
        let observed = Arc::clone(&saw_session_error);
        let observed_page_closed = Arc::clone(&page_closed_before_stop);
        let observed_provider = provider.clone();
        let crawler = Crawler::builder(kind(provider.clone()))
            .storage_client(storage())
            .max_session_rotations(0)
            .request_handler(|_ctx: BrowserContext| async { Ok(()) })
            .failed_request_handler(move |ctx: FailedRequestContext| {
                let observed = Arc::clone(&observed);
                let observed_page_closed = Arc::clone(&observed_page_closed);
                let provider = observed_provider.clone();
                async move {
                    let page_closed = wait_for_closed_pages(&provider, 1).await;
                    *observed.lock().unwrap_or_else(|error| error.into_inner()) =
                        ctx.error.rotates_session()
                            && ctx.error.http_status() == Some(http::StatusCode::FORBIDDEN);
                    *observed_page_closed
                        .lock()
                        .unwrap_or_else(|error| error.into_inner()) = page_closed;
                    Ok(())
                }
            })
            .build()
            .await
            .unwrap();

        let stats = crawler.run([blocked]).await.unwrap();
        assert_eq!(stats.requests_failed, 1);
        assert!(
            *saw_session_error
                .lock()
                .unwrap_or_else(|error| error.into_inner())
        );
        assert!(
            *page_closed_before_stop
                .lock()
                .unwrap_or_else(|error| error.into_inner())
        );
        let fake = provider
            .stats
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        assert_eq!(fake.pages_closed, fake.pages_created);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn handler_error_closes_page_and_rotates_on_session_error() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new(HashMap::new());
        let captured_session = Arc::new(tokio::sync::Mutex::new(None::<Arc<Session>>));
        let page_closed_before_stop = Arc::new(Mutex::new(false));
        let captured = Arc::clone(&captured_session);
        let observed_page_closed = Arc::clone(&page_closed_before_stop);
        let observed_provider = provider.clone();
        let session_options = SessionPoolOptions::default()
            .with_session_config(SessionConfig::default().with_max_error_score_scaled(1_000));
        let browser_kind = BrowserKind::builder(provider.clone())
            .session_pool(session_options)
            .build()
            .unwrap();
        let crawler = Crawler::builder(browser_kind)
            .storage_client(storage())
            .max_session_rotations(0)
            .request_handler(move |ctx: BrowserContext| {
                let captured = Arc::clone(&captured);
                async move {
                    *captured.lock().await = ctx.session.clone();
                    Err(CrawlError::session(anyhow::anyhow!("rotate")))
                }
            })
            .failed_request_handler(move |_ctx: FailedRequestContext| {
                let observed_page_closed = Arc::clone(&observed_page_closed);
                let provider = observed_provider.clone();
                async move {
                    let page_closed = wait_for_closed_pages(&provider, 1).await;
                    *observed_page_closed
                        .lock()
                        .unwrap_or_else(|error| error.into_inner()) = page_closed;
                    Ok(())
                }
            })
            .build()
            .await
            .unwrap();

        let stats = crawler
            .run(["https://example.com/handler-error"])
            .await
            .unwrap();
        assert_eq!(stats.requests_failed, 1);
        let session = captured_session.lock().await.clone().unwrap();
        assert!(!session.is_usable().await);
        assert!(
            *page_closed_before_stop
                .lock()
                .unwrap_or_else(|error| error.into_inner())
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn task_timeout_cancellation_recovers_page_via_guard() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let hanging = "https://example.com/hang";
        let provider = FakeProvider::new(HashMap::from([(
            hanging.to_owned(),
            FakeBehavior {
                hang: true,
                ..FakeBehavior::default()
            },
        )]));
        let mut options = AutoscaledPoolOptions::default();
        options.fixed_concurrency = Some(1);
        options.task_timeout = Some(Duration::from_millis(200));
        let page_closed_before_stop = Arc::new(Mutex::new(false));
        let observed_page_closed = Arc::clone(&page_closed_before_stop);
        let observed_provider = provider.clone();
        let crawler = Crawler::builder(kind(provider.clone()))
            .storage_client(storage())
            .autoscaled_pool_options(options)
            .max_request_retries(0)
            .request_handler(|_ctx: BrowserContext| async { Ok(()) })
            .failed_request_handler(move |_ctx: FailedRequestContext| {
                let observed_page_closed = Arc::clone(&observed_page_closed);
                let provider = observed_provider.clone();
                async move {
                    let page_closed = wait_for_closed_pages(&provider, 1).await;
                    *observed_page_closed
                        .lock()
                        .unwrap_or_else(|error| error.into_inner()) = page_closed;
                    Ok(())
                }
            })
            .build()
            .await
            .unwrap();

        let stats = crawler.run([hanging]).await.unwrap();
        assert_eq!(stats.requests_failed, 1);
        assert!(
            *page_closed_before_stop
                .lock()
                .unwrap_or_else(|error| error.into_inner())
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stop_shuts_down_pool() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let provider = FakeProvider::new(HashMap::new());
        let crawler = Crawler::builder(kind(provider.clone()))
            .storage_client(storage())
            .request_handler(|_ctx: BrowserContext| async { Ok(()) })
            .build()
            .await
            .unwrap();

        let _ = crawler.run(["https://example.com/"]).await.unwrap();
        let fake = provider
            .stats
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        assert!(fake.browsers_closed >= 1);
        assert_eq!(fake.open_pages, 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn owned_sessions_persist_but_shared_do_not() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let owned_storage = storage();
        let owned = Crawler::builder(kind(FakeProvider::new(HashMap::new())))
            .storage_client(Arc::clone(&owned_storage))
            .request_handler(|_ctx: BrowserContext| async { Ok(()) })
            .build()
            .await
            .unwrap();
        let _ = owned.run(["https://example.com/owned"]).await.unwrap();
        let owned_kvs = owned_storage
            .open_key_value_store(Some("default"))
            .await
            .unwrap();
        assert!(
            owned_kvs
                .get::<serde_json::Value>(SESSION_POOL_PERSIST_KEY)
                .await
                .unwrap()
                .is_some()
        );

        let shared_storage = storage();
        let shared_kvs = shared_storage
            .open_key_value_store(Some("default"))
            .await
            .unwrap();
        let shared_pool = Arc::new(SessionPool::new(SessionPoolOptions::default()));
        shared_pool.attach_persistence(Arc::clone(&shared_kvs));
        let shared_kind = BrowserKind::builder(FakeProvider::new(HashMap::new()))
            .shared_session_pool(shared_pool)
            .build()
            .unwrap();
        let shared = Crawler::builder(shared_kind)
            .storage_client(Arc::clone(&shared_storage))
            .request_handler(|_ctx: BrowserContext| async { Ok(()) })
            .build()
            .await
            .unwrap();
        let _ = shared.run(["https://example.com/shared"]).await.unwrap();
        assert!(
            shared_kvs
                .get::<serde_json::Value>(SESSION_POOL_PERSIST_KEY)
                .await
                .unwrap()
                .is_none()
        );
    })
    .await
    .unwrap();
}
