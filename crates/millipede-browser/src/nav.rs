//! Browser pre- and post-navigation hook contexts.

use std::sync::Arc;

use futures_util::future::BoxFuture;
use millipede_core::{errors::CrawlError, proxy::ProxyInfo, request::Request, session::Session};

use crate::{BrowserResponse, PageHandle};

/// Context supplied to browser pre-navigation hooks.
///
/// Unlike the INTERFACE §18 sketch, `request` is immutable because execution owns an
/// [`Arc<Request>`]. The proposed `log` field is omitted because no `Log` type exists; use
/// `tracing` macros instead.
#[non_exhaustive]
pub struct BrowserPreHookCtx<'a> {
    /// Request about to be navigated.
    pub request: &'a Request,
    /// Live page that will perform the navigation.
    pub page: &'a PageHandle,
    /// Session selected for this attempt, when sessions are enabled.
    pub session: Option<&'a Session>,
    /// Proxy assigned to the browser containing the page.
    pub proxy: Option<&'a ProxyInfo>,
}

/// Context supplied to browser post-navigation hooks.
///
/// The proposed INTERFACE §18 `log` field is omitted because no `Log` type exists; use `tracing`
/// macros instead.
#[non_exhaustive]
pub struct BrowserPostHookCtx<'a> {
    /// Request that was navigated.
    pub request: &'a Request,
    /// Live page after navigation and status classification.
    pub page: &'a PageHandle,
    /// Navigation response metadata, when exposed by the provider.
    pub response: Option<&'a BrowserResponse>,
    /// Session selected for this attempt, when sessions are enabled.
    pub session: Option<&'a Session>,
    /// Proxy assigned to the browser containing the page.
    pub proxy: Option<&'a ProxyInfo>,
}

/// Asynchronous browser hook run after page creation and before navigation.
pub type BrowserPreNavigationHook = Arc<
    dyn for<'a> Fn(BrowserPreHookCtx<'a>) -> BoxFuture<'a, Result<(), CrawlError>> + Send + Sync,
>;

/// Asynchronous browser hook run after navigation and status classification.
pub type BrowserPostNavigationHook = Arc<
    dyn for<'a> Fn(BrowserPostHookCtx<'a>) -> BoxFuture<'a, Result<(), CrawlError>> + Send + Sync,
>;
