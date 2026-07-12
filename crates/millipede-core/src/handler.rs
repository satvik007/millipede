//! Request handler and middleware contracts.

use std::future::Future;
use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::errors::CrawlError;
use crate::request::Request;

/// Processes an owned request context.
pub trait RequestHandler<C>: Send + Sync + 'static {
    /// Processes `ctx` and returns its eventual outcome.
    fn handle(&self, ctx: C) -> BoxFuture<'static, Result<(), CrawlError>>;
}

impl<C, F, Fut> RequestHandler<C> for F
where
    F: Fn(C) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), CrawlError>> + Send + 'static,
{
    fn handle(&self, ctx: C) -> BoxFuture<'static, Result<(), CrawlError>> {
        Box::pin((self)(ctx))
    }
}

/// Transforms a request context before its matched handler runs.
pub trait Middleware<C>: Send + Sync + 'static {
    /// Runs before the matched handler; receives the context by value and returns it (possibly
    /// mutated). An error short-circuits the request.
    fn run(&self, ctx: C) -> BoxFuture<'static, Result<C, CrawlError>>;
}

impl<C, F, Fut> Middleware<C> for F
where
    F: Fn(C) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<C, CrawlError>> + Send + 'static,
{
    fn run(&self, ctx: C) -> BoxFuture<'static, Result<C, CrawlError>> {
        Box::pin((self)(ctx))
    }
}

/// Owned payload handed to the failure handler when a request permanently fails.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FailedRequestContext {
    /// The request that permanently failed.
    pub request: Arc<Request>,
    /// The final error produced while processing the request.
    pub error: Arc<CrawlError>,
    /// The number of retry attempts made before the permanent failure.
    pub retry_count: u32,
}

impl FailedRequestContext {
    /// Creates a failure-handler context.
    pub fn new(request: Arc<Request>, error: Arc<CrawlError>, retry_count: u32) -> Self {
        Self {
            request,
            error,
            retry_count,
        }
    }
}

/// Handles a request after it has permanently failed.
pub trait FailedRequestHandler: Send + Sync + 'static {
    /// Processes a permanently failed request.
    fn handle(&self, ctx: FailedRequestContext) -> BoxFuture<'static, Result<(), CrawlError>>;
}

impl<F, Fut> FailedRequestHandler for F
where
    F: Fn(FailedRequestContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), CrawlError>> + Send + 'static,
{
    fn handle(&self, ctx: FailedRequestContext) -> BoxFuture<'static, Result<(), CrawlError>> {
        Box::pin((self)(ctx))
    }
}
