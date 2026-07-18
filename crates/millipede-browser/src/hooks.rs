//! Provider-erased browser lifecycle hooks.

use std::{fmt, sync::Arc};

use futures_util::future::BoxFuture;

use crate::{BrowserError, BrowserPage, LaunchContext, PageId, PageOpts};

/// Synchronous hook run before a browser launches.
pub type PreLaunchHook = Arc<dyn Fn(&mut LaunchContext) + Send + Sync>;

/// Synchronous hook that prepares per-page creation context.
pub type PagePrepHook = Arc<dyn Fn(&mut PageOpts) + Send + Sync>;

/// Asynchronous hook operating on a provider-erased page and its creation context.
pub type PageHook = Arc<
    dyn for<'a> Fn(&'a dyn BrowserPage, &'a PageOpts) -> BoxFuture<'a, Result<(), BrowserError>>
        + Send
        + Sync,
>;

/// Synchronous notification run after a page has closed.
pub type PageClosedHook = Arc<dyn Fn(PageId) + Send + Sync>;

/// Provider-erased browser lifecycle hooks.
///
/// This deliberately simplifies the generic `BrowserHooks<P>` sketch in INTERFACE §12.1.
/// Provider generics stay out of hook plumbing because `dyn BrowserPage` is the hook surface,
/// mirroring `PageHandle` erasure. `post_launch` and browser-parameterized `pre_page_create` slots
/// are deferred. Phase 7 fingerprint installation uses [`Self::post_page_create`], as directed by
/// INTERFACE §12 and ADR-0006.
#[derive(Clone, Default)]
pub struct BrowserHooks {
    /// Hooks that mutate launch context before provider launch.
    pub pre_launch: Vec<PreLaunchHook>,
    /// Hooks that mutate page context before provider page creation.
    pub pre_page_create: Vec<PagePrepHook>,
    /// Hooks run after the provider creates a page.
    pub post_page_create: Vec<PageHook>,
    /// Hooks run before the provider closes a page.
    pub pre_page_close: Vec<PageHook>,
    /// Hooks notified after a page closes.
    pub post_page_close: Vec<PageClosedHook>,
}

impl BrowserHooks {
    /// Creates the standard browser hooks, including bidirectional session cookie synchronization.
    pub fn defaults() -> Self {
        Self::default().with_session_cookie_sync()
    }

    /// Appends a pre-launch hook.
    pub fn push_pre_launch(
        mut self,
        hook: impl Fn(&mut LaunchContext) + Send + Sync + 'static,
    ) -> Self {
        self.pre_launch.push(Arc::new(hook));
        self
    }

    /// Appends a page-context preparation hook.
    pub fn push_pre_page_create(
        mut self,
        hook: impl Fn(&mut PageOpts) + Send + Sync + 'static,
    ) -> Self {
        self.pre_page_create.push(Arc::new(hook));
        self
    }

    /// Appends a post-page-creation hook.
    pub fn push_post_page_create<F>(mut self, hook: F) -> Self
    where
        F: for<'a> Fn(&'a dyn BrowserPage, &'a PageOpts) -> BoxFuture<'a, Result<(), BrowserError>>
            + Send
            + Sync
            + 'static,
    {
        self.post_page_create.push(Arc::new(hook));
        self
    }

    /// Appends a pre-page-close hook.
    pub fn push_pre_page_close<F>(mut self, hook: F) -> Self
    where
        F: for<'a> Fn(&'a dyn BrowserPage, &'a PageOpts) -> BoxFuture<'a, Result<(), BrowserError>>
            + Send
            + Sync
            + 'static,
    {
        self.pre_page_close.push(Arc::new(hook));
        self
    }

    /// Appends a post-page-close hook.
    pub fn push_post_page_close(mut self, hook: impl Fn(PageId) + Send + Sync + 'static) -> Self {
        self.post_page_close.push(Arc::new(hook));
        self
    }

    /// Adds a launch hook that appends command-line arguments in registration order.
    pub fn with_launch_args(self, args: Vec<String>) -> Self {
        self.push_pre_launch(move |ctx| ctx.extra_args.extend(args.iter().cloned()))
    }

    /// Adds page hooks that synchronize session cookies and page headers.
    ///
    /// Hook failures are returned unchanged to the caller.
    pub fn with_session_cookie_sync(self) -> Self {
        self.push_post_page_create(|page, opts| {
            Box::pin(async move {
                if let Some(session) = &opts.session {
                    let cookies = session.cookie_jar().export_cookies();
                    if !cookies.is_empty() {
                        page.set_cookies(&cookies).await?;
                    }
                }
                if !opts.extra_headers.is_empty() {
                    page.set_extra_headers(&opts.extra_headers).await?;
                }
                Ok(())
            })
        })
        .push_pre_page_close(|page, opts| {
            Box::pin(async move {
                if let Some(session) = &opts.session {
                    let cookies = page.cookies().await?;
                    session.cookie_jar().import_cookies(&cookies);
                    tracing::debug!(cookie_count = cookies.len(), "synchronized browser cookies");
                }
                Ok(())
            })
        })
    }

    /// Adds v0.1 browser fingerprint header/context consistency.
    ///
    /// This does not patch navigator, canvas, or WebGL properties. See
    /// `docs/guide/fingerprinting.md` for the documented limits.
    pub fn with_fingerprint(
        self,
        generator: Arc<millipede_fingerprint::BrowserFingerprintGenerator>,
    ) -> Self {
        self.push_post_page_create(move |page, opts| {
            let generator = Arc::clone(&generator);
            Box::pin(async move {
                let seed = opts
                    .session
                    .as_ref()
                    .map(|session| session.id().as_str().to_owned())
                    .unwrap_or_else(|| "anonymous".to_owned());
                let profile = generator.generate(&seed);
                let mut headers = http::HeaderMap::new();
                if let Ok(user_agent) = http::HeaderValue::from_str(&profile.user_agent) {
                    headers.insert(http::header::USER_AGENT, user_agent);
                }
                for (name, value) in profile.headers {
                    if let Ok(name) = http::HeaderName::from_bytes(name.as_bytes()) {
                        if let Ok(value) = http::HeaderValue::from_str(&value) {
                            headers.insert(name, value);
                        }
                    }
                }
                page.set_extra_headers(&headers).await?;
                Ok(())
            })
        })
    }
}

impl fmt::Debug for BrowserHooks {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrowserHooks")
            .field("pre_launch_count", &self.pre_launch.len())
            .field("pre_page_create_count", &self.pre_page_create.len())
            .field("post_page_create_count", &self.post_page_create.len())
            .field("pre_page_close_count", &self.pre_page_close.len())
            .field("post_page_close_count", &self.post_page_close.len())
            .finish()
    }
}
