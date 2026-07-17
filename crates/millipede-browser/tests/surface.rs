//! Public surface and default hook contract tests.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use millipede_browser::{
    BrowserError, BrowserHooks, BrowserPage, GotoOptions, LaunchContext, PageOpts,
    ScreenshotOptions, WaitUntil,
};
use millipede_core::{
    cookies::Cookie,
    errors::CrawlError,
    session::{Session, SessionConfig},
};

#[derive(Debug, Default)]
struct FakeSurfaceState {
    cookies: Vec<Cookie>,
    set_cookie_calls: Vec<Vec<Cookie>>,
    extra_header_calls: Vec<http::HeaderMap>,
}

#[derive(Debug, Clone, Default)]
struct FakeSurfacePage {
    state: Arc<Mutex<FakeSurfaceState>>,
}

impl FakeSurfacePage {
    fn with_cookies(cookies: Vec<Cookie>) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeSurfaceState {
                cookies,
                ..FakeSurfaceState::default()
            })),
        }
    }
}

#[async_trait::async_trait]
impl BrowserPage for FakeSurfacePage {
    async fn goto(
        &self,
        url: &url::Url,
        _opts: GotoOptions,
    ) -> Result<Option<millipede_browser::BrowserResponse>, BrowserError> {
        let mut response = millipede_browser::BrowserResponse::default();
        response.status = Some(http::StatusCode::OK);
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
        Ok(vec![url::Url::parse("https://example.com/linked").unwrap()])
    }

    async fn cookies(&self) -> Result<Vec<Cookie>, BrowserError> {
        Ok(self.state.lock().unwrap().cookies.clone())
    }

    async fn set_cookies(&self, cookies: &[Cookie]) -> Result<(), BrowserError> {
        self.state
            .lock()
            .unwrap()
            .set_cookie_calls
            .push(cookies.to_vec());
        Ok(())
    }

    async fn set_extra_headers(&self, headers: &http::HeaderMap) -> Result<(), BrowserError> {
        self.state
            .lock()
            .unwrap()
            .extra_header_calls
            .push(headers.clone());
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

    async fn screenshot(&self, _opts: ScreenshotOptions) -> Result<Bytes, BrowserError> {
        Ok(Bytes::from_static(b"image"))
    }
}

fn takes_erased_page(_: Arc<dyn BrowserPage>) {}

#[test]
fn browser_page_is_object_safe() {
    takes_erased_page(Arc::new(FakeSurfacePage::default()));
}

fn source_error() -> anyhow::Error {
    anyhow::anyhow!("provider failed")
}

#[test]
fn browser_errors_have_expected_crawl_classifications() {
    let url = url::Url::parse("https://example.com/").unwrap();
    let timeout = Duration::from_secs(3);

    let retryable = [
        BrowserError::Navigation {
            url: url.clone(),
            source: source_error(),
        },
        BrowserError::NavigationTimeout {
            url: url.clone(),
            timeout,
        },
        BrowserError::WaitTimeout {
            what: "selector".to_owned(),
            timeout,
        },
        BrowserError::PageCreate(source_error()),
        BrowserError::PageClosed,
        BrowserError::Protocol(source_error()),
    ];
    for error in retryable {
        assert!(matches!(error.classify(), CrawlError::Retry(_)));
    }

    let critical = [
        BrowserError::BrowserNotFound {
            hint: "install Chromium".to_owned(),
        },
        BrowserError::Launch(source_error()),
    ];
    for error in critical {
        assert!(matches!(CrawlError::from(error), CrawlError::Critical(_)));
    }

    let non_retryable = [
        BrowserError::Shutdown,
        BrowserError::Evaluation(source_error()),
        BrowserError::CookieConversion(source_error()),
    ];
    for error in non_retryable {
        assert!(matches!(error.classify(), CrawlError::NonRetryable(_)));
    }
}

#[test]
fn option_defaults_match_browser_contract() {
    let goto = GotoOptions::default();
    assert_eq!(goto.timeout, Duration::from_secs(30));
    assert_eq!(goto.wait_until, WaitUntil::Load);

    let page = PageOpts::default();
    assert!(page.session.is_none());
    assert!(page.extra_headers.is_empty());
}

#[tokio::test]
async fn session_cookie_hooks_synchronize_both_directions() {
    let session = Arc::new(Session::new(SessionConfig::default()));
    let session_cookie = Cookie::new("session", "from-jar", "example.com");
    assert_eq!(
        session
            .cookie_jar()
            .import_cookies(std::slice::from_ref(&session_cookie)),
        1
    );

    let page_cookie = Cookie::new("browser", "from-page", "example.com");
    let page = FakeSurfacePage::with_cookies(vec![page_cookie.clone()]);
    let opts = PageOpts::new().session(Arc::clone(&session));
    let hooks = BrowserHooks::default().with_session_cookie_sync();

    for hook in &hooks.post_page_create {
        hook(&page, &opts).await.unwrap();
    }
    {
        let state = page.state.lock().unwrap();
        assert_eq!(state.set_cookie_calls, vec![vec![session_cookie.clone()]]);
    }

    for hook in &hooks.pre_page_close {
        hook(&page, &opts).await.unwrap();
    }
    let exported = session.cookie_jar().export_cookies();
    assert!(exported.iter().any(|cookie| cookie == &session_cookie));
    assert!(exported.iter().any(|cookie| cookie == &page_cookie));
}

#[test]
fn launch_argument_hooks_append_in_registration_order() {
    let hooks = BrowserHooks::default()
        .with_launch_args(vec!["--first".to_owned(), "one".to_owned()])
        .with_launch_args(vec!["--second".to_owned(), "two".to_owned()]);
    let mut context = LaunchContext::new().extra_args(vec!["existing".to_owned()]);

    for hook in &hooks.pre_launch {
        hook(&mut context);
    }

    assert_eq!(
        context.extra_args,
        ["existing", "--first", "one", "--second", "two"]
    );
}
