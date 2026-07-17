#![allow(missing_docs)]

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use millipede_browser::{
    BrowserError, BrowserPage, BrowserProvider, BrowserResponse, DefaultPromotionDetector,
    GotoOptions, LaunchContext, ScreenshotOptions, SmartContext, SmartKind,
};
use millipede_core::{
    cookies::Cookie, crawler::Crawler, session::SessionPoolOptions, storage::StorageClient,
};
use millipede_storage_memory::MemoryStorageClient;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const STATIC_HTML: &str = "<html><body><p>This is a long contentful static page with substantially more than forty visible characters.</p></body></html>";
const SHELL_HTML: &str = "<html><body><script src=\"/app.js\"></script></body></html>";

#[derive(Default)]
struct FakeStats {
    launches: usize,
    pages_created: u64,
    gotos: Vec<(u64, String)>,
    set_cookie_calls: Vec<(u64, Vec<Cookie>)>,
}

#[derive(Clone, Default)]
struct FakeProvider {
    stats: Arc<Mutex<FakeStats>>,
}

struct FakeBrowser;

#[derive(Clone)]
struct FakePage {
    serial: u64,
    stats: Arc<Mutex<FakeStats>>,
}

#[async_trait::async_trait]
impl BrowserPage for FakePage {
    async fn goto(
        &self,
        url: &url::Url,
        _opts: GotoOptions,
    ) -> Result<Option<BrowserResponse>, BrowserError> {
        self.stats
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .gotos
            .push((self.serial, url.to_string()));
        let mut response = BrowserResponse::default();
        response.status = Some(http::StatusCode::OK);
        response.url = Some(url.clone());
        Ok(Some(response))
    }

    async fn content(&self) -> Result<String, BrowserError> {
        Ok("<html><body></body></html>".to_owned())
    }

    async fn evaluate_js(&self, _script: &str) -> Result<serde_json::Value, BrowserError> {
        Ok(serde_json::Value::Null)
    }

    async fn evaluate_anchors(
        &self,
        _selector: Option<&str>,
    ) -> Result<Vec<url::Url>, BrowserError> {
        Ok(Vec::new())
    }

    async fn cookies(&self) -> Result<Vec<Cookie>, BrowserError> {
        Ok(Vec::new())
    }

    async fn set_cookies(&self, cookies: &[Cookie]) -> Result<(), BrowserError> {
        self.stats
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .set_cookie_calls
            .push((self.serial, cookies.to_vec()));
        Ok(())
    }

    async fn set_extra_headers(&self, _headers: &http::HeaderMap) -> Result<(), BrowserError> {
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
        self.stats
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .launches += 1;
        Ok(FakeBrowser)
    }

    async fn new_page(&self, _browser: &Self::Browser) -> Result<Self::Page, BrowserError> {
        let serial = {
            let mut stats = self.stats.lock().unwrap_or_else(|error| error.into_inner());
            stats.pages_created += 1;
            stats.pages_created
        };
        Ok(FakePage {
            serial,
            stats: Arc::clone(&self.stats),
        })
    }

    async fn close_page(&self, _page: Self::Page) -> Result<(), BrowserError> {
        Ok(())
    }

    async fn close_browser(&self, _browser: Self::Browser) -> Result<(), BrowserError> {
        Ok(())
    }
}

fn storage() -> Arc<dyn StorageClient> {
    Arc::new(MemoryStorageClient::new())
}

async fn mount_static(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/static"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(STATIC_HTML, "text/html"))
        .mount(server)
        .await;
}

async fn mount_shell(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/shell"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(SHELL_HTML, "text/html"))
        .mount(server)
        .await;
}

async fn mount_challenge(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/challenge"))
        .respond_with(
            ResponseTemplate::new(403)
                .set_body_raw("<html><body>Just a moment...</body></html>", "text/html"),
        )
        .mount(server)
        .await;
}

fn url(server: &MockServer, route: &str) -> String {
    format!("{}{route}", server.uri())
}

#[tokio::test]
async fn static_page_stays_http() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = MockServer::start().await;
        mount_static(&server).await;
        let provider = FakeProvider::default();
        let paths = Arc::new(Mutex::new(HashMap::new()));
        let handler_paths = Arc::clone(&paths);
        let crawler = Crawler::builder(SmartKind::builder(provider.clone()).build().unwrap())
            .storage_client(storage())
            .max_concurrency(1)
            .max_request_retries(0)
            .request_handler(move |ctx: SmartContext| {
                let handler_paths = Arc::clone(&handler_paths);
                async move {
                    handler_paths
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .insert(ctx.request().url.to_string(), ctx.is_browser());
                    Ok(())
                }
            })
            .build()
            .await
            .unwrap();
        let target = url(&server, "/static");
        let stats = crawler.run([target.clone()]).await.unwrap();
        assert_eq!(stats.requests_finished, 1);
        assert_eq!(paths.lock().unwrap().get(&target), Some(&false));
        assert_eq!(provider.stats.lock().unwrap().launches, 0);
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn js_shell_promotes_to_browser() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = MockServer::start().await;
        mount_shell(&server).await;
        let provider = FakeProvider::default();
        let paths = Arc::new(Mutex::new(HashMap::new()));
        let handler_paths = Arc::clone(&paths);
        let crawler = Crawler::builder(SmartKind::builder(provider.clone()).build().unwrap())
            .storage_client(storage())
            .max_concurrency(1)
            .max_request_retries(0)
            .request_handler(move |ctx: SmartContext| {
                let handler_paths = Arc::clone(&handler_paths);
                async move {
                    handler_paths
                        .lock()
                        .unwrap()
                        .insert(ctx.request().url.to_string(), ctx.is_browser());
                    Ok(())
                }
            })
            .build()
            .await
            .unwrap();
        let target = url(&server, "/shell");
        let stats = crawler.run([target.clone()]).await.unwrap();
        assert_eq!(stats.requests_finished, 1);
        assert_eq!(paths.lock().unwrap().get(&target), Some(&true));
        assert_eq!(provider.stats.lock().unwrap().gotos, vec![(1, target)]);
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn challenge_status_promotes_via_error_path() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = MockServer::start().await;
        mount_challenge(&server).await;
        let provider = FakeProvider::default();
        let crawler = Crawler::builder(SmartKind::builder(provider.clone()).build().unwrap())
            .storage_client(storage())
            .max_concurrency(1)
            .max_request_retries(0)
            .request_handler(|ctx: SmartContext| async move {
                assert!(ctx.is_browser());
                Ok(())
            })
            .build()
            .await
            .unwrap();
        let stats = crawler.run([url(&server, "/challenge")]).await.unwrap();
        assert_eq!(stats.requests_finished, 1);
        let hits = server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .filter(|request| request.url.path() == "/challenge")
            .count();
        assert_eq!(hits, 1);
        assert_eq!(provider.stats.lock().unwrap().gotos.len(), 1);
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn bare_403_does_not_promote_when_status_not_configured() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = MockServer::start().await;
        mount_challenge(&server).await;
        let provider = FakeProvider::default();
        let kind = SmartKind::builder(provider.clone())
            .promote_status_codes([])
            .build()
            .unwrap();
        let crawler = Crawler::builder(kind)
            .storage_client(storage())
            .max_concurrency(1)
            .max_request_retries(0)
            .max_session_rotations(0)
            .request_handler(|_ctx: SmartContext| async move { Ok(()) })
            .build()
            .await
            .unwrap();
        let stats = crawler.run([url(&server, "/challenge")]).await.unwrap();
        assert_eq!(stats.requests_failed, 1);
        assert!(provider.stats.lock().unwrap().gotos.is_empty());
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn sticky_promotion_skips_http_on_second_request() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = MockServer::start().await;
        mount_shell(&server).await;
        mount_static(&server).await;
        let provider = FakeProvider::default();
        let crawler = Crawler::builder(SmartKind::builder(provider.clone()).build().unwrap())
            .storage_client(storage())
            .max_concurrency(1)
            .max_request_retries(0)
            .request_handler(|ctx: SmartContext| async move {
                assert!(ctx.is_browser());
                Ok(())
            })
            .build()
            .await
            .unwrap();
        crawler
            .run([url(&server, "/shell"), url(&server, "/shell?p=2")])
            .await
            .unwrap();
        let shell_hits = server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .filter(|request| request.url.path() == "/shell")
            .count();
        assert_eq!(shell_hits, 1);
        assert_eq!(provider.stats.lock().unwrap().gotos.len(), 2);

        let provider = FakeProvider::default();
        let variants = Arc::new(Mutex::new(Vec::new()));
        let handler_variants = Arc::clone(&variants);
        let kind = SmartKind::builder(provider)
            .sticky_promotion(false)
            .build()
            .unwrap();
        let crawler = Crawler::builder(kind)
            .storage_client(storage())
            .max_concurrency(1)
            .max_request_retries(0)
            .request_handler(move |ctx: SmartContext| {
                let handler_variants = Arc::clone(&handler_variants);
                async move {
                    handler_variants.lock().unwrap().push(ctx.is_browser());
                    Ok(())
                }
            })
            .build()
            .await
            .unwrap();
        crawler
            .run([url(&server, "/shell?nonsticky=1"), url(&server, "/static")])
            .await
            .unwrap();
        assert_eq!(*variants.lock().unwrap(), vec![true, false]);
        let static_hits = server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .filter(|request| request.url.path() == "/static")
            .count();
        assert_eq!(static_hits, 1);
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn custom_detector_required_selector() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = MockServer::start().await;
        mount_static(&server).await;
        let provider = FakeProvider::default();
        let kind = SmartKind::builder(provider.clone())
            .detector(DefaultPromotionDetector::new().with_required_selector("#app-root"))
            .build()
            .unwrap();
        let crawler = Crawler::builder(kind)
            .storage_client(storage())
            .max_concurrency(1)
            .max_request_retries(0)
            .request_handler(|ctx: SmartContext| async move {
                assert!(ctx.is_browser());
                Ok(())
            })
            .build()
            .await
            .unwrap();
        crawler.run([url(&server, "/static")]).await.unwrap();
        assert_eq!(provider.stats.lock().unwrap().gotos.len(), 1);
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn shared_cookie_state_across_paths() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/static"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Set-Cookie", "s=1; Path=/")
                    .set_body_raw(STATIC_HTML, "text/html"),
            )
            .mount(&server)
            .await;
        mount_shell(&server).await;
        let provider = FakeProvider::default();
        let kind = SmartKind::builder(provider.clone())
            .session_pool(SessionPoolOptions::default().with_max_pool_size(1))
            .build()
            .unwrap();
        let crawler = Crawler::builder(kind)
            .storage_client(storage())
            .max_concurrency(1)
            .max_request_retries(0)
            .request_handler(|_ctx: SmartContext| async move { Ok(()) })
            .build()
            .await
            .unwrap();
        crawler
            .run([url(&server, "/static"), url(&server, "/shell")])
            .await
            .unwrap();
        let stats = provider.stats.lock().unwrap();
        assert!(stats.set_cookie_calls.iter().any(|(_, cookies)| {
            cookies
                .iter()
                .any(|cookie| cookie.name == "s" && cookie.value == "1")
        }));
    })
    .await
    .expect("test timed out");
}
