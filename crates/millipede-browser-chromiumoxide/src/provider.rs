//! Chromiumoxide browser provider lifecycle.

use std::time::Duration;

use async_trait::async_trait;
use chromiumoxide::{Browser, BrowserConfig};
use futures_util::StreamExt;
use millipede_browser::{BrowserError, BrowserProvider, LaunchContext};

use crate::{ChromiumLaunchOptions, ChromiumPage};

fn page_was_already_closed(error: &chromiumoxide::error::CdpError) -> bool {
    if matches!(
        error,
        chromiumoxide::error::CdpError::ChannelSendError(_)
            | chromiumoxide::error::CdpError::NoResponse
            | chromiumoxide::error::CdpError::NotFound
    ) {
        return true;
    }
    let message = error.to_string().to_ascii_lowercase();
    message.contains("no target")
        || message.contains("target closed")
        || message.contains("target not found")
}

/// A launched Chromium process and its CDP event driver.
///
/// Millipede runs browsers headlessly in its tests and CI. Explicit close-and-wait is the normal
/// shutdown path; chromiumoxide's verified kill-on-drop behavior remains a last resort.
pub struct ChromiumBrowser {
    browser: Browser,
    handler_task: tokio::task::JoinHandle<()>,
    _profile_dir: Option<tempfile::TempDir>,
}

/// Chromium CDP provider backed by chromiumoxide.
#[derive(Debug, Default, Clone)]
pub struct ChromiumoxideProvider;

#[async_trait]
impl BrowserProvider for ChromiumoxideProvider {
    type Browser = ChromiumBrowser;
    type Page = ChromiumPage;
    type LaunchOptions = ChromiumLaunchOptions;

    async fn launch(
        &self,
        opts: Self::LaunchOptions,
        ctx: &LaunchContext,
    ) -> Result<Self::Browser, BrowserError> {
        let executable = opts
            .executable_path()
            .map(ToOwned::to_owned)
            .or_else(crate::discovery::find_browser)
            .ok_or_else(|| BrowserError::BrowserNotFound {
                hint: "set MILLIPEDE_CHROME or install Google Chrome/Chromium".to_owned(),
            })?;

        let (profile_dir, user_data_dir) = if let Some(path) = opts.profile_path() {
            (None, path.to_owned())
        } else {
            let directory = tempfile::Builder::new()
                .prefix("millipede-cdp-profile")
                .tempdir()
                .map_err(|error| BrowserError::Launch(anyhow::Error::new(error)))?;
            let path = directory.path().to_owned();
            (Some(directory), path)
        };

        let mut builder = BrowserConfig::builder()
            .chrome_executable(executable)
            .user_data_dir(user_data_dir)
            .launch_timeout(opts.browser_launch_timeout())
            .request_timeout(opts.cdp_request_timeout());
        if !opts.is_headless() {
            builder = builder.with_head();
        }
        if let Some((width, height)) = opts.viewport() {
            builder = builder.window_size(width, height);
        }
        if let Some(proxy) = &ctx.proxy {
            builder = builder.arg(format!("--proxy-server={}", proxy.url));
        }
        for argument in opts.additional_args().iter().chain(&ctx.extra_args) {
            builder = builder.arg(argument.clone());
        }

        let config = builder
            .build()
            .map_err(|error| BrowserError::Launch(anyhow::anyhow!(error)))?;
        let (browser, mut handler) = Browser::launch(config)
            .await
            .map_err(|error| BrowserError::Launch(anyhow::Error::new(error)))?;
        let handler_task = tokio::spawn(async move {
            while let Some(event) = handler.next().await {
                if event.is_err() {
                    tracing::debug!(?event, "cdp handler event error");
                }
            }
        });

        Ok(ChromiumBrowser {
            browser,
            handler_task,
            _profile_dir: profile_dir,
        })
    }

    async fn new_page(&self, browser: &Self::Browser) -> Result<Self::Page, BrowserError> {
        let page = browser
            .browser
            .new_page("about:blank")
            .await
            .map_err(|error| BrowserError::PageCreate(anyhow::Error::new(error)))?;
        Ok(ChromiumPage::new(page))
    }

    async fn close_page(&self, page: Self::Page) -> Result<(), BrowserError> {
        match page.into_inner().close().await {
            Ok(()) => Ok(()),
            Err(error) if page_was_already_closed(&error) => Ok(()),
            Err(error) => Err(BrowserError::Protocol(anyhow::Error::new(error))),
        }
    }

    async fn close_browser(&self, mut browser: Self::Browser) -> Result<(), BrowserError> {
        let close_error = browser
            .browser
            .close()
            .await
            .err()
            .map(|error| BrowserError::Protocol(anyhow::Error::new(error)));

        if let Err(error) = browser.browser.wait().await {
            tracing::debug!(?error, "failed to wait for Chromium child process");
        }

        let mut handler_task = browser.handler_task;
        if tokio::time::timeout(Duration::from_secs(5), &mut handler_task)
            .await
            .is_err()
        {
            handler_task.abort();
            let _ = handler_task.await;
        }

        if let Some(error) = close_error {
            Err(error)
        } else {
            Ok(())
        }
    }
}
