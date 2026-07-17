//! Real-browser integration coverage for the Chromiumoxide provider.

use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result, ensure};
use futures_util::FutureExt;
use http::{HeaderMap, HeaderValue, StatusCode, header::HeaderName};
use millipede_browser::{BrowserError, BrowserPage, BrowserProvider, GotoOptions, LaunchContext};
use millipede_browser_chromiumoxide::{
    ChromiumBrowser, ChromiumLaunchOptions, ChromiumPage, ChromiumoxideProvider, find_browser,
};
use millipede_core::cookies::Cookie;
use serde_json::Value;
use url::Url;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

static BROWSER_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

async fn launch_page(
    executable: PathBuf,
) -> Result<(ChromiumoxideProvider, ChromiumBrowser, ChromiumPage)> {
    launch_page_with_options(ChromiumLaunchOptions::default().with_executable(executable)).await
}

async fn launch_page_with_options(
    options: ChromiumLaunchOptions,
) -> Result<(ChromiumoxideProvider, ChromiumBrowser, ChromiumPage)> {
    let provider = ChromiumoxideProvider;
    let browser = provider.launch(options, &LaunchContext::default()).await?;
    match provider.new_page(&browser).await {
        Ok(page) => Ok((provider, browser, page)),
        Err(error) => {
            let _ = provider.close_browser(browser).await;
            Err(error.into())
        }
    }
}

async fn cleanup(
    provider: &ChromiumoxideProvider,
    page: ChromiumPage,
    browser: ChromiumBrowser,
) -> Result<()> {
    let page_result = provider.close_page(page).await;
    let browser_result = provider.close_browser(browser).await;
    page_result?;
    browser_result?;
    Ok(())
}

type AssertionOutcome = std::thread::Result<Result<()>>;
type TimedTestOutcome =
    std::thread::Result<std::result::Result<Result<()>, tokio::time::error::Elapsed>>;

fn propagate_after_cleanup(outcome: AssertionOutcome, cleanup_result: Result<()>) -> Result<()> {
    match outcome {
        Err(panic) => {
            drop(cleanup_result);
            std::panic::resume_unwind(panic);
        }
        Ok(Err(error)) => Err(error),
        Ok(Ok(())) => cleanup_result,
    }
}

fn propagate_timed_test(outcome: TimedTestOutcome) -> Result<()> {
    match outcome {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(Err(error)) => Err(error).context("browser integration test exceeded 90 seconds"),
        Ok(Ok(result)) => result,
    }
}

#[tokio::test]
async fn navigates_and_reads_dom() -> Result<()> {
    let outcome = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(90),
        async {
            let _test_guard = BROWSER_TEST_LOCK.lock().await;
            let server = MockServer::start().await;
            let Some(executable) = require_browser() else {
                return Ok(());
            };
            Mock::given(method("GET"))
                .and(path("/"))
                .respond_with(ResponseTemplate::new(200).set_body_raw(
                    format!(
                        r#"<!doctype html><title>Millipede</title><main>provider-marker</main><a href="/one">one</a><a href="{}/two">two</a><a href="https://example.test/three">three</a>"#,
                        server.uri()
                    ),
                    "text/html",
                ))
                .mount(&server)
                .await;

            let (provider, browser, page) = launch_page(executable).await?;
            let assertion_outcome = std::panic::AssertUnwindSafe(async {
                let url = Url::parse(&format!("{}/", server.uri()))?;
                let response = page.goto(&url, GotoOptions::default()).await?;
                let content = page.content().await?;
                if let Some(response) = response {
                    ensure!(response.status == Some(StatusCode::OK));
                } else {
                    ensure!(content.contains("provider-marker"));
                }
                ensure!(content.contains("provider-marker"));
                let anchors = page.evaluate_anchors(None).await?;
                ensure!(
                    anchors.len() == 3,
                    "expected three anchors, got {anchors:?}"
                );
                ensure!(anchors[0] == Url::parse(&format!("{}/one", server.uri()))?);
                ensure!(anchors[1] == Url::parse(&format!("{}/two", server.uri()))?);
                ensure!(anchors[2] == Url::parse("https://example.test/three")?);
                ensure!(page.evaluate_js("1+2").await? == Value::from(3));
                Ok(())
            })
            .catch_unwind()
            .await;
            let cleanup_result = cleanup(&provider, page, browser).await;
            propagate_after_cleanup(assertion_outcome, cleanup_result)
        },
    ))
    .catch_unwind()
    .await;
    propagate_timed_test(outcome)
}

#[tokio::test]
async fn cookie_roundtrip_through_real_browser() -> Result<()> {
    let outcome =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(90), async {
            let _test_guard = BROWSER_TEST_LOCK.lock().await;
            let server = MockServer::start().await;
            let Some(executable) = require_browser() else {
                return Ok(());
            };
            Mock::given(method("GET"))
                .and(path("/cookies"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header("Set-Cookie", "server_cookie=from-response; Path=/")
                        .set_body_string("cookies-ready"),
                )
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/cookie-origin"))
                .respond_with(ResponseTemplate::new(200).set_body_string("origin-ready"))
                .mount(&server)
                .await;

            let (provider, browser, page) = launch_page(executable).await?;
            let assertion_outcome = std::panic::AssertUnwindSafe(async {
                let url = Url::parse(&format!("{}/cookies", server.uri()))?;
                let origin_url = Url::parse(&format!("{}/cookie-origin", server.uri()))?;
                let host = url.host_str().context("wiremock URL has no host")?;
                page.goto(&origin_url, GotoOptions::default()).await?;
                let mut cookie = Cookie::new("client_cookie", "from-cdp", host);
                cookie.host_only = true;
                page.set_cookies(&[cookie]).await?;
                page.goto(&url, GotoOptions::default()).await?;
                let cookies = page.cookies().await?;
                ensure!(cookies.iter().any(|cookie| {
                    cookie.name == "client_cookie" && cookie.value == "from-cdp" && cookie.host_only
                }));
                ensure!(cookies.iter().any(|cookie| {
                    cookie.name == "server_cookie" && cookie.value == "from-response"
                }));
                Ok(())
            })
            .catch_unwind()
            .await;
            let cleanup_result = cleanup(&provider, page, browser).await;
            propagate_after_cleanup(assertion_outcome, cleanup_result)
        }))
        .catch_unwind()
        .await;
    propagate_timed_test(outcome)
}

#[tokio::test]
async fn wait_click_and_headers() -> Result<()> {
    let outcome = std::panic::AssertUnwindSafe(tokio::time::timeout(
        Duration::from_secs(90),
        async {
            let _test_guard = BROWSER_TEST_LOCK.lock().await;
            let server = MockServer::start().await;
            let Some(executable) = require_browser() else {
                return Ok(());
            };
            Mock::given(method("GET"))
                .and(path("/interactive"))
                .and(header("x-millipede-test", "present"))
                .respond_with(ResponseTemplate::new(200).set_body_raw(
                    r#"<!doctype html><button id="go" onclick="const done=document.createElement('div');done.id='done';document.body.appendChild(done)">go</button>"#,
                    "text/html",
                ))
                .mount(&server)
                .await;

            let (provider, browser, page) = launch_page(executable).await?;
            let assertion_outcome = std::panic::AssertUnwindSafe(async {
                let mut headers = HeaderMap::new();
                headers.insert(
                    HeaderName::from_static("x-millipede-test"),
                    HeaderValue::from_static("present"),
                );
                page.set_extra_headers(&headers).await?;
                let url = Url::parse(&format!("{}/interactive", server.uri()))?;
                page.goto(&url, GotoOptions::default()).await?;
                page.click("#go").await?;
                page.wait_for_selector("#done", Duration::from_secs(5))
                    .await?;
                let missing = page
                    .wait_for_selector("#missing", Duration::from_millis(300))
                    .await;
                ensure!(matches!(missing, Err(BrowserError::WaitTimeout { .. })));
                Ok(())
            })
            .catch_unwind()
            .await;
            let cleanup_result = cleanup(&provider, page, browser).await;
            propagate_after_cleanup(assertion_outcome, cleanup_result)
        },
    ))
    .catch_unwind()
    .await;
    propagate_timed_test(outcome)
}

#[tokio::test]
async fn close_browser_reaps_child() -> Result<()> {
    let outcome =
        std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(90), async {
            let _test_guard = BROWSER_TEST_LOCK.lock().await;
            let _server = MockServer::start().await;
            let Some(executable) = require_browser() else {
                return Ok(());
            };
            let (provider, browser, page) = launch_page(executable).await?;
            let cleanup_result = cleanup(&provider, page, browser).await;
            cleanup_result?;

            #[cfg(unix)]
            ensure!(
                !std::process::Command::new("pgrep")
                    .args(["-f", "millipede-cdp-profile"])
                    .status()
                    .context("failed to run pgrep")?
                    .success(),
                "Chromium process with a millipede-cdp-profile marker remains after close_browser",
            );
            Ok(())
        }))
        .catch_unwind()
        .await;
    propagate_timed_test(outcome)
}
