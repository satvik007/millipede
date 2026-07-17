//! Chromiumoxide `Send` and browser-process lifecycle spike.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use chromiumoxide::browser::{Browser, BrowserConfig};
use futures_util::StreamExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn find_browser_for_test() -> Option<PathBuf> {
    if let Ok(value) = std::env::var("MILLIPEDE_CHROME") {
        let path = PathBuf::from(value);
        assert!(
            path.exists(),
            "MILLIPEDE_CHROME points to a browser binary that does not exist: {}",
            path.display()
        );
        return Some(path);
    }

    if let Ok(value) = std::env::var("CHROME") {
        let path = PathBuf::from(value);
        if path.exists() {
            return Some(path);
        }
    }

    let well_known_paths = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/snap/bin/chromium",
    ];
    for candidate in well_known_paths {
        let path = PathBuf::from(candidate);
        if path.exists() {
            return Some(path);
        }
    }

    eprintln!("SKIP: no Chromium/Chrome binary found; set MILLIPEDE_CHROME");
    None
}

fn assert_send<T: Send>() {}

fn assert_send_value<T: Send>(_: &T) {}

fn assert_sync<T: Sync>() {}

#[allow(clippy::manual_async_fn)]
fn holds_page_across_await(
    page: chromiumoxide::Page,
) -> impl std::future::Future<Output = ()> + Send {
    async move {
        let _ = page.url().await;
    }
}

#[test]
fn spike_types_are_send_and_sync() {
    assert_send::<chromiumoxide::Page>();
    assert_sync::<chromiumoxide::Page>();
    assert_send::<chromiumoxide::Browser>();
    assert_sync::<chromiumoxide::Browser>();
}

#[tokio::test]
async fn spike_launch_navigate_close() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(90), async {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/html")
                    .set_body_string(
                        "<html><head><title>spike</title></head><body><a href=\"/next\">next</a></body></html>",
                    ),
            )
            .mount(&server)
            .await;

        let Some(chrome_executable) = find_browser_for_test() else {
            return Ok::<(), anyhow::Error>(());
        };
        let user_data_dir = tempfile::Builder::new()
            .prefix("millipede-cdp-profile")
            .tempdir()?;
        let config = BrowserConfig::builder()
            .chrome_executable(chrome_executable)
            .user_data_dir(user_data_dir.path())
            .build()
            .map_err(anyhow::Error::msg)?;

        let (mut browser, mut handler) = Browser::launch(config).await?;
        let mut h = tokio::spawn(async move {
            while let Some(event) = StreamExt::next(&mut handler).await {
                if event.is_err() {
                    break;
                }
            }
        });

        let page = browser.new_page(server.uri()).await?;
        assert!(page.content().await?.contains("next"));
        let _ = page.get_cookies().await?;
        let evaluation = page.evaluate("1+1").await?;
        assert_eq!(evaluation.value(), Some(&serde_json::Value::from(2)));
        page.close().await?;

        let send_page = browser.new_page(server.uri()).await?;
        let holds_page_future = holds_page_across_await(send_page);
        assert_send_value(&holds_page_future);
        holds_page_future.await;

        browser.close().await?;
        let _ = browser.wait().await?;

        match tokio::time::timeout(Duration::from_secs(5), &mut h).await {
            Ok(result) => result?,
            Err(_) => {
                h.abort();
                let _ = h.await;
            }
        }

        Ok(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn spike_drop_kills_child() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), async {
        let Some(chrome_executable) = find_browser_for_test() else {
            return Ok::<(), anyhow::Error>(());
        };
        let user_data_dir = tempfile::Builder::new()
            .prefix("millipede-cdp-profile")
            .tempdir()?;
        let user_data_dir_pattern = user_data_dir.path().to_string_lossy().into_owned();
        let config = BrowserConfig::builder()
            .chrome_executable(chrome_executable)
            .user_data_dir(user_data_dir.path())
            .build()
            .map_err(anyhow::Error::msg)?;

        let (browser, mut handler) = Browser::launch(config).await?;
        let h = tokio::spawn(async move {
            while let Some(event) = StreamExt::next(&mut handler).await {
                if event.is_err() {
                    break;
                }
            }
        });

        drop(browser);
        h.abort();
        let _ = h.await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        #[cfg(unix)]
        {
            let status = std::process::Command::new("pgrep")
                .arg("-f")
                .arg(&user_data_dir_pattern)
                .status()?;
            assert!(!status.success(), "Chromium child survived Browser drop");
        }

        Ok(())
    })
    .await??;
    Ok(())
}
