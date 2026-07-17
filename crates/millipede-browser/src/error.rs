//! Browser errors and their crawl retry semantics.

use millipede_core::errors::CrawlError;

/// An error produced while launching or operating a browser.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BrowserError {
    /// No supported browser executable could be discovered.
    #[error("no usable browser binary found: {hint}")]
    BrowserNotFound {
        /// Actionable browser discovery guidance.
        hint: String,
    },
    /// Starting the browser process failed.
    #[error("browser launch failed: {0}")]
    Launch(#[source] anyhow::Error),
    /// Creating a page failed.
    #[error("page creation failed: {0}")]
    PageCreate(#[source] anyhow::Error),
    /// Navigation failed before its timeout.
    #[error("navigation to {url} failed: {source}")]
    Navigation {
        /// Navigation target.
        url: url::Url,
        /// Provider error that caused navigation to fail.
        #[source]
        source: anyhow::Error,
    },
    /// Navigation exceeded its configured timeout.
    #[error("navigation to {url} timed out after {timeout:?}")]
    NavigationTimeout {
        /// Navigation target.
        url: url::Url,
        /// Configured navigation timeout.
        timeout: std::time::Duration,
    },
    /// Waiting for a browser condition exceeded its timeout.
    #[error("waiting for {what} timed out after {timeout:?}")]
    WaitTimeout {
        /// Description of the awaited condition.
        what: String,
        /// Configured wait timeout.
        timeout: std::time::Duration,
    },
    /// JavaScript evaluation failed.
    #[error("script evaluation failed: {0}")]
    Evaluation(#[source] anyhow::Error),
    /// Conversion between provider and Millipede cookies failed.
    #[error("cookie conversion failed: {0}")]
    CookieConversion(#[source] anyhow::Error),
    /// An operation targeted a page that was already closed.
    #[error("page is already closed")]
    PageClosed,
    /// An operation targeted a browser pool after shutdown.
    #[error("browser pool is shut down")]
    Shutdown,
    /// A Chrome DevTools Protocol operation failed.
    #[error("CDP protocol error: {0}")]
    Protocol(#[source] anyhow::Error),
}

impl BrowserError {
    /// Classifies this browser failure for the crawler retry engine.
    #[allow(unreachable_patterns)]
    pub fn classify(self) -> CrawlError {
        match self {
            Self::Navigation { .. }
            | Self::NavigationTimeout { .. }
            | Self::WaitTimeout { .. }
            | Self::PageCreate(_)
            | Self::PageClosed
            | Self::Protocol(_) => CrawlError::retry(self),
            Self::BrowserNotFound { .. } | Self::Launch(_) => CrawlError::critical(self),
            Self::Shutdown | Self::Evaluation(_) | Self::CookieConversion(_) => {
                CrawlError::non_retryable(self)
            }
            _ => CrawlError::non_retryable(self),
        }
    }
}

impl From<BrowserError> for CrawlError {
    fn from(error: BrowserError) -> Self {
        error.classify()
    }
}
