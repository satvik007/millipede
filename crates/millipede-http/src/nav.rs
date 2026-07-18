//! Concrete HTTP navigation hook contexts.
//!
//! These contexts follow INTERFACE §18 with two documented deviations. First, `request` is a
//! read-only `&Request` rather than `&mut Request` because [`crate::HttpKind`] execution holds the
//! request in an `Arc<Request>` (`env.request`) that cannot be mutated in place; the worked
//! example only mutates `http_request`, so its documented usage is preserved. Second, the
//! interface's `log` field is omitted because no `Log` type exists in the workspace yet. The
//! context structs are `#[non_exhaustive]`, allowing that field to be added later without a
//! breaking change.

use futures_util::future::BoxFuture;
use millipede_core::{
    errors::CrawlError,
    http_client::{HttpRequest, HttpResponse},
    proxy::ProxyInfo,
    request::Request,
    session::Session,
};
use std::sync::Arc;

/// State exposed immediately before an HTTP request is sent.
#[non_exhaustive]
pub struct HttpPreHookCtx<'a> {
    /// Crawl request that produced the outgoing HTTP request.
    pub request: &'a Request,
    /// Mutable outgoing HTTP request.
    pub http_request: &'a mut HttpRequest,
    /// Session selected for this attempt, if enabled.
    pub session: Option<&'a Session>,
    /// Proxy selected for this attempt, if any.
    pub proxy: Option<&'a ProxyInfo>,
}

/// State exposed after an HTTP response is received.
#[non_exhaustive]
pub struct HttpPostHookCtx<'a> {
    /// Crawl request that produced the response.
    pub request: &'a Request,
    /// Received HTTP response.
    pub response: &'a HttpResponse,
    /// Session selected for this attempt, if enabled.
    pub session: Option<&'a Session>,
    /// Proxy selected for this attempt, if any.
    pub proxy: Option<&'a ProxyInfo>,
}

/// Asynchronous hook run immediately before an HTTP request is sent.
pub type HttpPreNavigationHook =
    Arc<dyn for<'a> Fn(HttpPreHookCtx<'a>) -> BoxFuture<'a, Result<(), CrawlError>> + Send + Sync>;

/// Asynchronous hook run after an HTTP response is received and anti-bot detection completes.
pub type HttpPostNavigationHook =
    Arc<dyn for<'a> Fn(HttpPostHookCtx<'a>) -> BoxFuture<'a, Result<(), CrawlError>> + Send + Sync>;
