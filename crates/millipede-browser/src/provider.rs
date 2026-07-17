//! Integration surface for concrete browser providers.

use std::fmt;

use millipede_core::proxy::ProxyInfo;

use crate::{BrowserError, BrowserPage};

/// Process-level context applied when launching a browser.
///
/// # Provider contract
///
/// Providers **must apply both fields at launch**: [`Self::proxy`] is the process-level browser
/// proxy because browser proxies are per-process rather than per-page, and [`Self::extra_args`]
/// must be appended to the browser command line. Together with the default lifecycle hooks, this
/// implements ROADMAP Phase 6's `pre_launch` proxy and launch-argument behavior.
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct LaunchContext {
    /// Resolved process-level browser proxy.
    pub proxy: Option<ProxyInfo>,
    /// Additional arguments appended to the browser command line.
    pub extra_args: Vec<String>,
}

impl LaunchContext {
    /// Creates an empty launch context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the process-level browser proxy.
    pub fn proxy(mut self, proxy: ProxyInfo) -> Self {
        self.proxy = Some(proxy);
        self
    }

    /// Replaces the additional browser command-line arguments.
    pub fn extra_args(mut self, extra_args: Vec<String>) -> Self {
        self.extra_args = extra_args;
        self
    }
}

impl fmt::Debug for LaunchContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LaunchContext")
            .field(
                "proxy_endpoint",
                &self
                    .proxy
                    .as_ref()
                    .map(|proxy| (&proxy.hostname, proxy.port)),
            )
            .field("extra_args", &self.extra_args)
            .finish()
    }
}

/// Concrete browser backend used by a browser pool.
///
/// This deliberately follows ADR-0006 rather than duplicating the page methods sketched in
/// INTERFACE §12.1. Page-level operations (`goto`, cookies, evaluation, and related methods) live
/// solely on [`BrowserPage`]. [`Self::Page`] is `BrowserPage + Clone`: the pool retains a concrete
/// clone for close bookkeeping while handing users an `Arc<dyn BrowserPage>`.
#[async_trait::async_trait]
pub trait BrowserProvider: Send + Sync + 'static {
    /// Provider-native launched browser handle.
    type Browser: Send + Sync + 'static;
    /// Provider-native page adapter used for erasure and close bookkeeping.
    type Page: BrowserPage + Clone;
    /// Provider-specific browser launch options.
    type LaunchOptions: Default + Clone + Send + Sync + 'static;

    /// Launches a browser and applies every field in `ctx`.
    async fn launch(
        &self,
        opts: Self::LaunchOptions,
        ctx: &LaunchContext,
    ) -> Result<Self::Browser, BrowserError>;

    /// Creates a new page in `browser`.
    async fn new_page(&self, browser: &Self::Browser) -> Result<Self::Page, BrowserError>;

    /// Closes a page owned by the provider.
    async fn close_page(&self, page: Self::Page) -> Result<(), BrowserError>;

    /// Closes the browser and reaps its child process.
    ///
    /// Implementations must perform close-and-wait shutdown so browser child processes cannot
    /// become zombies. Drop-based termination is only a fallback.
    async fn close_browser(&self, browser: Self::Browser) -> Result<(), BrowserError>;
}
