//! Provider-erased browser page operations and configuration.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use millipede_core::{cookies::Cookie, session::Session};

use crate::BrowserError;

#[allow(dead_code)]
static NEXT_PAGE_ID: AtomicU64 = AtomicU64::new(1);

/// Stable process-local identifier for a pooled browser page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId(u64);

impl PageId {
    #[allow(dead_code)]
    pub(crate) fn next() -> Self {
        Self(NEXT_PAGE_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Browser lifecycle event awaited after navigation.
///
/// Providers map these events best-effort. A provider with weaker protocol capabilities may use
/// the nearest available event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WaitUntil {
    /// Wait until the initial HTML is parsed without waiting for subresources.
    DomContentLoaded,
    /// Wait until the document and dependent resources report loaded.
    Load,
}

/// Options controlling a page navigation.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GotoOptions {
    /// Maximum duration allowed for navigation.
    pub timeout: Duration,
    /// Lifecycle event awaited after navigation.
    pub wait_until: WaitUntil,
}

impl GotoOptions {
    /// Sets the navigation timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the lifecycle event awaited after navigation.
    pub fn wait_until(mut self, wait_until: WaitUntil) -> Self {
        self.wait_until = wait_until;
        self
    }
}

impl Default for GotoOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            wait_until: WaitUntil::Load,
        }
    }
}

/// Navigation response metadata when the provider can expose it.
///
/// Providers are allowed to be lossy and may return no response from
/// [`BrowserPage::goto`]. Individual fields may also be unavailable.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct BrowserResponse {
    /// Final navigation status code, when available.
    pub status: Option<http::StatusCode>,
    /// Final navigation response headers, when available.
    pub headers: http::HeaderMap,
    /// Final response URL, including redirects, when available.
    pub url: Option<url::Url>,
}

/// Options controlling screenshot capture.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ScreenshotOptions {
    /// Capture the complete scrollable page instead of only the viewport.
    pub full_page: bool,
}

/// Per-page creation context consumed by browser hooks.
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct PageOpts {
    /// Session whose cookies should be synchronized with the page.
    pub session: Option<Arc<Session>>,
    /// Headers to install on the page before navigation.
    pub extra_headers: http::HeaderMap,
}

impl PageOpts {
    /// Creates an empty page context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the session associated with the page.
    pub fn session(mut self, session: Arc<Session>) -> Self {
        self.session = Some(session);
        self
    }

    /// Replaces the page's extra request headers.
    pub fn extra_headers(mut self, extra_headers: http::HeaderMap) -> Self {
        self.extra_headers = extra_headers;
        self
    }
}

impl fmt::Debug for PageOpts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PageOpts")
            .field(
                "session_id",
                &self.session.as_ref().map(|session| session.id()),
            )
            .field("extra_headers", &self.extra_headers)
            .finish()
    }
}

/// Object-safe browser page surface implemented by concrete providers.
///
/// This is the provider-erased page interface from INTERFACE §12.2. Providers may adapt weaker
/// protocols lossily; in particular, [`Self::goto`] may return `None` when response metadata is
/// unavailable.
#[async_trait::async_trait]
pub trait BrowserPage: Send + Sync + 'static {
    /// Navigates to `url` and returns response metadata when the provider exposes it.
    async fn goto(
        &self,
        url: &url::Url,
        opts: GotoOptions,
    ) -> Result<Option<BrowserResponse>, BrowserError>;

    /// Returns the current serialized document HTML.
    async fn content(&self) -> Result<String, BrowserError>;

    /// Evaluates JavaScript in the page and returns its JSON-compatible value.
    async fn evaluate_js(&self, script: &str) -> Result<serde_json::Value, BrowserError>;

    /// Evaluates anchor destinations and returns DOM-resolved absolute URLs.
    ///
    /// `None` selects `a[href]`. Implementations must read the DOM `a.href` value so relative
    /// destinations are resolved against the document URL.
    async fn evaluate_anchors(&self, selector: Option<&str>)
    -> Result<Vec<url::Url>, BrowserError>;

    /// Returns the page's cookies as Millipede's structured cookie records.
    async fn cookies(&self) -> Result<Vec<Cookie>, BrowserError>;

    /// Replaces or merges the supplied structured cookies into the page.
    async fn set_cookies(&self, cookies: &[Cookie]) -> Result<(), BrowserError>;

    /// Installs additional request headers for subsequent page requests.
    async fn set_extra_headers(&self, headers: &http::HeaderMap) -> Result<(), BrowserError>;

    /// Waits until an element matching `selector` exists or `timeout` elapses.
    async fn wait_for_selector(
        &self,
        selector: &str,
        timeout: Duration,
    ) -> Result<(), BrowserError>;

    /// Clicks an element matching `selector`.
    async fn click(&self, selector: &str) -> Result<(), BrowserError>;

    /// Captures a screenshot and returns its encoded bytes.
    async fn screenshot(&self, opts: ScreenshotOptions) -> Result<bytes::Bytes, BrowserError>;
}
