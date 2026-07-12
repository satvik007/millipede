//! Start-request conversions accepted by crawlers.

use crate::{errors::CrawlError, request::Request};

/// A single value convertible into one start [`Request`].
pub trait IntoStartRequest {
    /// Converts this value into a request.
    fn into_start_request(self) -> Result<Request, CrawlError>;
}

impl IntoStartRequest for Request {
    fn into_start_request(self) -> Result<Request, CrawlError> {
        Ok(self)
    }
}

impl IntoStartRequest for url::Url {
    fn into_start_request(self) -> Result<Request, CrawlError> {
        Ok(Request::get(self).build()?)
    }
}

impl IntoStartRequest for &str {
    fn into_start_request(self) -> Result<Request, CrawlError> {
        Ok(Request::get(self).build()?)
    }
}

impl IntoStartRequest for String {
    fn into_start_request(self) -> Result<Request, CrawlError> {
        Ok(Request::get(self).build()?)
    }
}

/// A collection of start requests accepted by [`super::Crawler::run`].
pub trait IntoStartRequests {
    /// Converts this value into an owned list of requests.
    fn into_start_requests(self) -> Result<Vec<Request>, CrawlError>;
}

macro_rules! impl_single_start_requests {
    ($($ty:ty),+ $(,)?) => {$(
        impl IntoStartRequests for $ty {
            fn into_start_requests(self) -> Result<Vec<Request>, CrawlError> {
                Ok(vec![self.into_start_request()?])
            }
        }
    )+};
}

impl_single_start_requests!(Request, url::Url, &str, String);

impl<T: IntoStartRequest> IntoStartRequests for Vec<T> {
    fn into_start_requests(self) -> Result<Vec<Request>, CrawlError> {
        self.into_iter()
            .map(IntoStartRequest::into_start_request)
            .collect()
    }
}

impl<T: IntoStartRequest, const N: usize> IntoStartRequests for [T; N] {
    fn into_start_requests(self) -> Result<Vec<Request>, CrawlError> {
        self.into_iter()
            .map(IntoStartRequest::into_start_request)
            .collect()
    }
}

impl<T: IntoStartRequest + Clone> IntoStartRequests for &[T] {
    fn into_start_requests(self) -> Result<Vec<Request>, CrawlError> {
        self.iter()
            .cloned()
            .map(IntoStartRequest::into_start_request)
            .collect()
    }
}
