//! Label- and method-based request routing.

use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::errors::CrawlError;
use crate::handler::{Middleware, RequestHandler};
use crate::request::{Method, Request};

/// Provides request metadata used to select a route.
pub trait HasRequest {
    /// Returns the request associated with this context.
    fn request(&self) -> &Request;
}

/// Restricts a route to selected HTTP methods.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum MethodFilter {
    /// Matches every HTTP method.
    Any,
    /// Matches only the listed HTTP methods.
    Only(Vec<Method>),
}

impl MethodFilter {
    /// Returns whether this filter accepts `method`.
    pub fn matches(&self, method: &Method) -> bool {
        match self {
            Self::Any => true,
            Self::Only(methods) => methods.contains(method),
        }
    }

    fn shadows(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Any, _) => true,
            (Self::Only(_), Self::Any) => false,
            (Self::Only(earlier), Self::Only(later)) => {
                later.iter().all(|method| earlier.contains(method))
            }
        }
    }
}

/// Routes request contexts by label and HTTP method.
pub struct Router<C> {
    routes: Vec<Route<C>>,
    default: Option<Arc<dyn RequestHandler<C>>>,
    middleware: Vec<Arc<dyn Middleware<C>>>,
}

/// A registered route. A `None` label is a wildcard that matches every request label.
struct Route<C> {
    label: Option<String>,
    methods: MethodFilter,
    handler: Arc<dyn RequestHandler<C>>,
}

impl<C: HasRequest + Send + 'static> Router<C> {
    /// Creates an empty router.
    // Router intentionally has no `Default` implementation because the inherent `default`
    // builder method would shadow `Default::default()` and produce a confusing arity error.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
            default: None,
            middleware: Vec::new(),
        }
    }

    /// Adds a route that matches `label` with any HTTP method.
    pub fn route<H: RequestHandler<C>>(self, label: impl Into<String>, handler: H) -> Self {
        self.push_route(Some(label.into()), MethodFilter::Any, handler)
    }

    /// Adds a route that matches `label` with one HTTP method.
    pub fn route_method<H: RequestHandler<C>>(
        self,
        label: impl Into<String>,
        method: Method,
        handler: H,
    ) -> Self {
        self.route_methods(label, [method], handler)
    }

    /// Adds a route that matches `label` with any of the supplied HTTP methods.
    pub fn route_methods<H, I>(self, label: impl Into<String>, methods: I, handler: H) -> Self
    where
        H: RequestHandler<C>,
        I: IntoIterator<Item = Method>,
    {
        self.push_route(
            Some(label.into()),
            MethodFilter::Only(methods.into_iter().collect()),
            handler,
        )
    }

    /// Sets the fallback handler used when no registered route matches.
    pub fn default<H: RequestHandler<C>>(mut self, handler: H) -> Self {
        self.default = Some(Arc::new(handler));
        self
    }

    /// Appends middleware that runs in registration order before a matched handler.
    pub fn middleware<M: Middleware<C>>(mut self, middleware: M) -> Self {
        self.middleware.push(Arc::new(middleware));
        self
    }

    fn push_route<H: RequestHandler<C>>(
        mut self,
        label: Option<String>,
        methods: MethodFilter,
        handler: H,
    ) -> Self {
        if self.routes.iter().any(|route| {
            (route.label.is_none() || route.label == label) && route.methods.shadows(&methods)
        }) {
            tracing::warn!(label = ?label, "route is unreachable because an earlier route shadows it");
        }
        self.routes.push(Route {
            label,
            methods,
            handler: Arc::new(handler),
        });
        self
    }
}

impl<C: HasRequest + Send + 'static> RequestHandler<C> for Router<C> {
    fn handle(&self, ctx: C) -> BoxFuture<'static, Result<(), CrawlError>> {
        let request = ctx.request();
        let handler = self
            .routes
            .iter()
            .find(|route| {
                (route.label.is_none() || route.label.as_deref() == request.label.as_deref())
                    && route.methods.matches(&request.method)
            })
            .map(|route| Arc::clone(&route.handler))
            .or_else(|| self.default.as_ref().map(Arc::clone));

        let Some(handler) = handler else {
            let error = CrawlError::MissingRoute {
                label: request.label.clone(),
                method: request.method.clone(),
            };
            return Box::pin(async move { Err(error) });
        };
        let middleware = self.middleware.clone();

        Box::pin(async move {
            let mut ctx = ctx;
            for middleware in middleware {
                ctx = middleware.run(ctx).await?;
            }
            handler.handle(ctx).await
        })
    }
}
