use super::{CrawlerHandle, CrawlerKind, RequestEnv, RequestOutcome};
use crate::{errors::CrawlError, request::Request};
use futures_util::future::BoxFuture;
use std::sync::Arc;

/// The no-fetch crawler kind whose execution hands requests directly to handlers.
#[derive(Debug, Clone, Copy, Default)]
pub struct BasicKind;

/// The handler context for [`BasicKind`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BasicContext {
    /// The request being handled.
    pub request: Arc<Request>,
    /// A weak back-reference to the running crawler.
    pub crawler: CrawlerHandle,
}

impl crate::router::HasRequest for BasicContext {
    fn request(&self) -> &Request {
        &self.request
    }
}

impl CrawlerKind for BasicKind {
    type Context = BasicContext;

    fn execute<'a>(
        &'a self,
        env: RequestEnv<'a>,
    ) -> BoxFuture<'a, Result<BasicContext, CrawlError>> {
        Box::pin(async move {
            Ok(BasicContext {
                request: env.request,
                crawler: env.crawler,
            })
        })
    }

    fn cleanup(
        &self,
        _outcome: RequestOutcome<BasicContext>,
    ) -> BoxFuture<'_, Result<(), CrawlError>> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Configuration,
        crawler::{CrawlerEnv, RequestPrep},
        router::HasRequest,
    };

    #[tokio::test]
    async fn basic_kind_execute_is_identity() {
        let config = Arc::new(Configuration::default());
        let shared = crate::crawler::tests::shared();
        let crawler_env = CrawlerEnv {
            shared: shared.clone(),
            config,
            storage: None,
            kvs: None,
        };
        let request = Arc::new(Request::get("https://example.com/").build().unwrap());
        let kind = BasicKind;

        assert!(kind.start(&crawler_env).await.is_ok());
        let mut prep = RequestPrep {
            request: (*request).clone(),
        };
        assert!(kind.before_request(&mut prep).await.is_ok());
        let mut context = kind
            .execute(RequestEnv {
                request: request.clone(),
                crawler: crawler_env.handle(),
                events: crawler_env.events(),
                overrides: Default::default(),
            })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&context.request, &request));
        assert!(std::ptr::eq(context.request(), request.as_ref()));
        assert!(kind.after_success(&mut context).await.is_ok());
        assert!(kind.stop(&crawler_env).await.is_ok());
    }
}
