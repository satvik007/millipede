//! Real-Chrome end-to-end smoke tests for `BrowserCrawler`.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use millipede_browser::{
    BrowserContext, BrowserCrawler, BrowserError, BrowserKind, BrowserProvider, LaunchContext,
};
use millipede_browser_chromiumoxide::{
    ChromiumBrowser, ChromiumLaunchOptions, ChromiumPage, ChromiumoxideProvider,
    discovery::find_browser,
};
use millipede_core::{
    request::Request,
    session::SessionPoolOptions,
    storage::{DatasetExt, ListOptions, StorageClient},
};
use millipede_storage_memory::MemoryStorageClient;
use serde_json::json;
use url::Url;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header_regex, method, path},
};

static BROWSER_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Default)]
struct TrackingStats {
    next_browser_id: u64,
    pages_per_browser: HashMap<u64, usize>,
    closed_browsers: Vec<u64>,
}

#[derive(Clone, Default)]
struct TrackingProvider {
    stats: Arc<Mutex<TrackingStats>>,
}

struct TrackingBrowser {
    id: u64,
    inner: ChromiumBrowser,
}

#[async_trait::async_trait]
impl BrowserProvider for TrackingProvider {
    type Browser = TrackingBrowser;
    type Page = ChromiumPage;
    type LaunchOptions = ChromiumLaunchOptions;

    async fn launch(
        &self,
        options: Self::LaunchOptions,
        context: &LaunchContext,
    ) -> Result<Self::Browser, BrowserError> {
        let inner = ChromiumoxideProvider.launch(options, context).await?;
        let id = {
            let mut stats = self.stats.lock().unwrap_or_else(|error| error.into_inner());
            stats.next_browser_id += 1;
            stats.next_browser_id
        };
        Ok(TrackingBrowser { id, inner })
    }

    async fn new_page(&self, browser: &Self::Browser) -> Result<Self::Page, BrowserError> {
        let page = ChromiumoxideProvider.new_page(&browser.inner).await?;
        *self
            .stats
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .pages_per_browser
            .entry(browser.id)
            .or_default() += 1;
        Ok(page)
    }

    async fn close_page(&self, page: Self::Page) -> Result<(), BrowserError> {
        ChromiumoxideProvider.close_page(page).await
    }

    async fn close_browser(&self, browser: Self::Browser) -> Result<(), BrowserError> {
        ChromiumoxideProvider.close_browser(browser.inner).await?;
        self.stats
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .closed_browsers
            .push(browser.id);
        Ok(())
    }
}

fn require_browser() -> Option<PathBuf> {
    if let Ok(configured) = std::env::var("MILLIPEDE_CHROME") {
        let path = PathBuf::from(configured);
        assert!(
            path.is_file(),
            "MILLIPEDE_CHROME does not name an existing file: {}",
            path.display()
        );
        return Some(path);
    }
    if let Some(browser) = find_browser() {
        return Some(browser);
    }
    eprintln!("SKIP: no Chromium/Chrome binary found; set MILLIPEDE_CHROME");
    None
}

fn html(body: impl Into<Vec<u8>>) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_raw(body, "text/html")
}

async fn local_site() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(
            html(
                r#"<!doctype html><html><head><title>e2e-home</title></head><body>
                <a href="/products/1">product one</a>
                <a href="/products/2">product two</a>
                <a href="/about">about</a>
                </body></html>"#,
            )
            .insert_header("Set-Cookie", "e2e=1; Path=/"),
        )
        .mount(&server)
        .await;

    for (route, title) in [
        ("/products/1", "e2e-product-1"),
        ("/products/2", "e2e-product-2"),
        ("/about", "e2e-about"),
    ] {
        Mock::given(method("GET"))
            .and(path(route))
            .respond_with(html(format!(
                r#"<!doctype html><html><head><title>{title}</title></head><body><a href="/">home</a></body></html>"#
            )))
            .mount(&server)
            .await;
    }
    server
}

async fn cookie_site() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(
            html(r#"<!doctype html><html><head><title>cookie-home</title></head></html>"#)
                .insert_header("Set-Cookie", "e2e=1; Path=/"),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/about"))
        .respond_with(ResponseTemplate::new(500).set_body_raw(
            r#"<!doctype html><html><head><title>cookie-missing</title></head></html>"#,
            "text/html",
        ))
        .with_priority(2)
        .mount(&server)
        .await;
    server
}

async fn mount_cookie_expectation(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/about"))
        .and(header_regex("Cookie", r"(?:^|;\s*)e2e=1(?:;|$)"))
        .respond_with(html(
            r#"<!doctype html><html><head><title>cookie-about</title></head></html>"#,
        ))
        .with_priority(1)
        .expect(1)
        .mount(server)
        .await;
}

fn launch_options(executable: PathBuf) -> ChromiumLaunchOptions {
    ChromiumLaunchOptions::default().with_executable(executable)
}

fn site_url(server: &MockServer, route: &str) -> Url {
    Url::parse(&format!("{}{route}", server.uri())).expect("wiremock URL must parse")
}

#[cfg(unix)]
async fn wait_for_browser_processes_to_exit() -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let is_running = std::process::Command::new("pgrep")
            .args(["-f", "millipede-cdp-profile"])
            .status()
            .context("failed to run pgrep")?
            .success();
        if !is_running {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "Chromium process with a millipede-cdp-profile marker remains after crawler shutdown"
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn browser_crawler_crawls_local_site_end_to_end() -> Result<()> {
    let _test_guard = BROWSER_TEST_LOCK.lock().await;
    let server = local_site().await;
    let Some(executable) = require_browser() else {
        return Ok(());
    };
    let storage = Arc::new(MemoryStorageClient::new());
    let kind = BrowserKind::builder(ChromiumoxideProvider)
        .launch_options(launch_options(executable))
        .navigation_timeout(Duration::from_secs(30))
        .build()?;
    let crawler = BrowserCrawler::builder(kind)
        .max_concurrency(2)
        .request_handler(|ctx: BrowserContext| async move {
            let title = ctx.page.evaluate_js("document.title").await?;
            ctx.storage
                .dataset()
                .push(&json!({ "url": ctx.request.url, "title": title }))
                .await?;
            let _ = ctx.enqueue.same_hostname().await?;
            Ok(())
        })
        .storage_client(storage.clone())
        .build()
        .await?;

    let outcome: Result<()> = async {
        let mut run = Box::pin(crawler.run([site_url(&server, "/")]));
        let stats = match tokio::time::timeout(Duration::from_secs(120), &mut run).await {
            Ok(result) => result?,
            Err(error) => {
                crawler.abort();
                let _ = run.await;
                return Err(error).context("browser crawler E2E test exceeded 120 seconds");
            }
        };
        assert_eq!(stats.requests_finished, 4);
        let dataset = storage.open_dataset(Some("default")).await?;
        let items = dataset.list_raw(ListOptions::default()).await?;
        assert_eq!(items.items.len(), 4);

        #[cfg(unix)]
        wait_for_browser_processes_to_exit().await?;
        Ok(())
    }
    .await;
    outcome
}

#[tokio::test]
async fn cookies_persist_across_pages_with_real_browser() -> Result<()> {
    let _test_guard = BROWSER_TEST_LOCK.lock().await;
    let server = cookie_site().await;
    let Some(executable) = require_browser() else {
        return Ok(());
    };
    mount_cookie_expectation(&server).await;
    let kind = BrowserKind::builder(ChromiumoxideProvider)
        .launch_options(launch_options(executable))
        .session_pool(SessionPoolOptions::default().with_max_pool_size(1))
        .retire_browser_after_page_count(1)
        .navigation_timeout(Duration::from_secs(30))
        .build()?;
    let crawler = BrowserCrawler::builder(kind)
        .max_concurrency(1)
        .request_handler(|_: BrowserContext| async { Ok(()) })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;
    let starts = [
        Request::get(site_url(&server, "/")).build()?,
        Request::get(site_url(&server, "/about")).build()?,
    ];

    let outcome: Result<()> = async {
        let mut run = Box::pin(crawler.run(starts));
        let stats = match tokio::time::timeout(Duration::from_secs(120), &mut run).await {
            Ok(result) => result?,
            Err(error) => {
                crawler.abort();
                let _ = run.await;
                return Err(error).context("browser cookie E2E test exceeded 120 seconds");
            }
        };
        assert_eq!(stats.requests_finished, 2);
        assert_eq!(stats.requests_failed, 0);
        server.verify().await;
        Ok(())
    }
    .await;
    outcome
}

#[tokio::test]
async fn pool_limits_hold_with_real_browser() -> Result<()> {
    let _test_guard = BROWSER_TEST_LOCK.lock().await;
    let server = local_site().await;
    let Some(executable) = require_browser() else {
        return Ok(());
    };
    let provider = TrackingProvider::default();
    let tracking_stats = Arc::clone(&provider.stats);
    let kind = BrowserKind::builder(provider)
        .launch_options(launch_options(executable))
        .max_open_pages_per_browser(1)
        .retire_browser_after_page_count(2)
        .navigation_timeout(Duration::from_secs(30))
        .build()?;
    let crawler = BrowserCrawler::builder(kind)
        .max_concurrency(2)
        .request_handler(|ctx: BrowserContext| async move {
            let _ = ctx.enqueue.same_hostname().await?;
            Ok(())
        })
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .build()
        .await?;

    let outcome: Result<()> = async {
        let mut run = Box::pin(crawler.run([site_url(&server, "/")]));
        let stats = match tokio::time::timeout(Duration::from_secs(120), &mut run).await {
            Ok(result) => result?,
            Err(error) => {
                crawler.abort();
                let _ = run.await;
                return Err(error).context("browser pool-limits E2E test exceeded 120 seconds");
            }
        };
        assert_eq!(stats.requests_finished, 4);
        assert_eq!(stats.requests_failed, 0);
        let tracking = tracking_stats
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // >= rather than ==: a transient navigation failure that succeeds on retry
        // opens an extra page while requests_finished/failed stay 4/0.
        assert!(tracking.pages_per_browser.values().sum::<usize>() >= 4);
        assert!(
            tracking.pages_per_browser.len() >= 2,
            "retirement after two pages should require at least two browser instances: {:?}",
            tracking.pages_per_browser
        );
        assert!(
            tracking
                .pages_per_browser
                .values()
                .all(|page_count| *page_count <= 2),
            "a browser exceeded retire_browser_after_page_count: {:?}",
            tracking.pages_per_browser
        );
        assert_eq!(
            tracking.closed_browsers.len() as u64,
            tracking.next_browser_id,
            "crawler shutdown did not close every launched browser"
        );
        Ok(())
    }
    .await;
    outcome
}
