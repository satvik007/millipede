//! Backend-independent HTTP request and response abstractions.

use std::{borrow::Cow, fmt, sync::Arc, time::Duration};

use bytes::Bytes;
use futures_util::stream::BoxStream;
use http::{HeaderMap, StatusCode};
use url::Url;

use crate::{
    cookies::CookieJar,
    request::{Method, Request, RequestBody},
};

/// A typed HTTP status carried inside a [`crate::errors::CrawlError`].
///
/// # Examples
///
/// ```
/// use http::StatusCode;
/// use millipede_core::http_client::HttpStatusError;
///
/// let error = HttpStatusError::new(StatusCode::TOO_MANY_REQUESTS);
/// assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("HTTP status {status}")]
pub struct HttpStatusError {
    /// The response status.
    pub status: StatusCode,
}

impl HttpStatusError {
    /// Creates a status carrier.
    pub fn new(status: StatusCode) -> Self {
        Self { status }
    }
}

/// An error produced while preparing or executing an HTTP request.
///
/// # Examples
///
/// ```
/// use millipede_core::http_client::HttpClientError;
///
/// let error = HttpClientError::timeout(anyhow::anyhow!("deadline elapsed"));
/// assert!(error.is_timeout());
/// assert!(!error.is_connect());
/// ```
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum HttpClientError {
    /// The HTTP client could not be built.
    #[error("failed to build HTTP client: {0}")]
    Build(#[source] anyhow::Error),
    /// The request was invalid.
    #[error("invalid HTTP request: {0}")]
    InvalidRequest(#[source] anyhow::Error),
    /// The remote endpoint could not be connected to.
    #[error("HTTP connection failed: {0}")]
    Connect(#[source] anyhow::Error),
    /// The request timed out.
    #[error("HTTP request timed out: {0}")]
    Timeout(#[source] anyhow::Error),
    /// Redirect processing failed.
    #[error("HTTP redirect failed: {0}")]
    Redirect(#[source] anyhow::Error),
    /// The response could not be decoded.
    #[error("HTTP response decode failed: {0}")]
    Decode(#[source] anyhow::Error),
    /// An input/output operation failed.
    #[error("HTTP I/O failed: {0}")]
    Io(#[source] anyhow::Error),
    /// Another HTTP client error occurred.
    #[error("HTTP client error: {0}")]
    Other(#[source] anyhow::Error),
}

impl HttpClientError {
    /// Returns whether this error represents a connection failure.
    pub fn is_connect(&self) -> bool {
        matches!(self, Self::Connect(_))
    }

    /// Returns whether this error represents a timeout.
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Timeout(_))
    }

    /// Creates a client-build error.
    pub fn build(error: impl Into<anyhow::Error>) -> Self {
        Self::Build(error.into())
    }

    /// Creates an invalid-request error.
    pub fn invalid_request(error: impl Into<anyhow::Error>) -> Self {
        Self::InvalidRequest(error.into())
    }

    /// Creates a connection error.
    pub fn connect(error: impl Into<anyhow::Error>) -> Self {
        Self::Connect(error.into())
    }

    /// Creates a timeout error.
    pub fn timeout(error: impl Into<anyhow::Error>) -> Self {
        Self::Timeout(error.into())
    }

    /// Creates a redirect error.
    pub fn redirect(error: impl Into<anyhow::Error>) -> Self {
        Self::Redirect(error.into())
    }

    /// Creates a response-decode error.
    pub fn decode(error: impl Into<anyhow::Error>) -> Self {
        Self::Decode(error.into())
    }

    /// Creates an input/output error.
    pub fn io(error: impl Into<anyhow::Error>) -> Self {
        Self::Io(error.into())
    }

    /// Creates an otherwise unclassified HTTP client error.
    pub fn other(error: impl Into<anyhow::Error>) -> Self {
        Self::Other(error.into())
    }
}

/// A backend-independent HTTP request.
///
/// `use_header_generator` and `session_token` are deliberately omitted until
/// `millipede-fingerprint` lands in Phase 7. This type is `#[non_exhaustive]`,
/// so those fields can be added later without breaking downstream code.
///
/// # Examples
///
/// ```
/// use millipede_core::{http_client::HttpRequest, request::Method};
/// use url::Url;
///
/// let request = HttpRequest::new(Url::parse("https://example.com/")?)
///     .method(Method::HEAD)
///     .max_redirects(3);
/// assert_eq!(request.method, Method::HEAD);
/// # Ok::<(), url::ParseError>(())
/// ```
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct HttpRequest {
    /// URL to request.
    pub url: Url,
    /// HTTP method.
    pub method: Method,
    /// HTTP request headers.
    pub headers: HeaderMap,
    /// Optional request body.
    pub body: Option<RequestBody>,
    /// Optional cookie jar used for this request and its response.
    pub cookie_jar: Option<Arc<CookieJar>>,
    /// Optional proxy URL.
    pub proxy: Option<Url>,
    /// Optional request timeout.
    pub timeout: Option<Duration>,
    /// Maximum number of redirects to follow.
    pub max_redirects: u32,
}

impl HttpRequest {
    /// Creates a GET request with no headers, body, cookie jar, proxy, or timeout.
    pub fn new(url: Url) -> Self {
        Self {
            url,
            method: Method::GET,
            headers: HeaderMap::new(),
            body: None,
            cookie_jar: None,
            proxy: None,
            timeout: None,
            max_redirects: 10,
        }
    }

    /// Copies the HTTP-facing fields from a crawl request.
    pub fn from_request(request: &Request) -> Self {
        Self::new(request.url.clone())
            .method(request.method.clone())
            .headers(request.headers.clone())
            .body_option(request.body.clone())
    }

    /// Sets the HTTP method.
    pub fn method(mut self, method: Method) -> Self {
        self.method = method;
        self
    }

    /// Replaces the HTTP headers.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    /// Sets the request body.
    pub fn body(mut self, body: RequestBody) -> Self {
        self.body = Some(body);
        self
    }

    fn body_option(mut self, body: Option<RequestBody>) -> Self {
        self.body = body;
        self
    }

    /// Sets the cookie jar.
    pub fn cookie_jar(mut self, cookie_jar: Arc<CookieJar>) -> Self {
        self.cookie_jar = Some(cookie_jar);
        self
    }

    /// Sets the proxy URL.
    pub fn proxy(mut self, proxy: Url) -> Self {
        self.proxy = Some(proxy);
        self
    }

    /// Sets the request timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the maximum number of redirects to follow.
    pub fn max_redirects(mut self, max_redirects: u32) -> Self {
        self.max_redirects = max_redirects;
        self
    }
}

/// A fully buffered HTTP response.
///
/// # Examples
///
/// ```
/// use bytes::Bytes;
/// use http::{HeaderMap, StatusCode};
/// use millipede_core::http_client::HttpResponse;
/// use url::Url;
///
/// let response = HttpResponse::new(
///     Url::parse("https://example.com/")?,
///     StatusCode::OK,
///     HeaderMap::new(),
///     Bytes::from_static(b"hello"),
/// );
/// assert_eq!(response.text(), "hello");
/// # Ok::<(), url::ParseError>(())
/// ```
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct HttpResponse {
    /// Final URL after redirects.
    pub url: Url,
    /// HTTP response status.
    pub status: StatusCode,
    /// HTTP response headers.
    pub headers: HeaderMap,
    /// Fully buffered response body.
    pub body: Bytes,
    /// Intermediate redirect URLs in order, excluding the final URL.
    pub redirect_chain: Vec<Url>,
}

impl HttpResponse {
    /// Creates a response with an empty redirect chain.
    pub fn new(url: Url, status: StatusCode, headers: HeaderMap, body: Bytes) -> Self {
        Self {
            url,
            status,
            headers,
            body,
            redirect_chain: Vec::new(),
        }
    }

    /// Sets the intermediate redirect URLs.
    pub fn with_redirect_chain(mut self, chain: Vec<Url>) -> Self {
        self.redirect_chain = chain;
        self
    }

    /// Returns the body decoded as UTF-8, replacing invalid sequences lossily.
    pub fn text(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body)
    }

    /// Deserializes the response body as JSON.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.body)
    }
}

/// An HTTP response whose body arrives as a byte stream.
///
/// # Examples
///
/// ```
/// use futures_util::stream;
/// use http::{HeaderMap, StatusCode};
/// use millipede_core::http_client::StreamingResponse;
/// use url::Url;
///
/// let response = StreamingResponse::new(
///     Url::parse("https://example.com/")?,
///     StatusCode::OK,
///     HeaderMap::new(),
///     Box::pin(stream::empty()),
/// );
/// assert_eq!(response.status, StatusCode::OK);
/// # Ok::<(), url::ParseError>(())
/// ```
#[non_exhaustive]
pub struct StreamingResponse {
    /// Final URL after redirects.
    pub url: Url,
    /// HTTP response status.
    pub status: StatusCode,
    /// HTTP response headers.
    pub headers: HeaderMap,
    /// Stream of response body chunks.
    pub body: BoxStream<'static, Result<Bytes, HttpClientError>>,
}

impl StreamingResponse {
    /// Creates a streaming response.
    pub fn new(
        url: Url,
        status: StatusCode,
        headers: HeaderMap,
        body: BoxStream<'static, Result<Bytes, HttpClientError>>,
    ) -> Self {
        Self {
            url,
            status,
            headers,
            body,
        }
    }
}

impl fmt::Debug for StreamingResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamingResponse")
            .field("url", &self.url)
            .field("status", &self.status)
            .field("headers", &self.headers)
            .finish_non_exhaustive()
    }
}

/// An object-safe asynchronous HTTP client backend.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use millipede_core::http_client::HttpClient;
///
/// fn accepts_client(_client: Arc<dyn HttpClient>) {}
/// ```
#[async_trait::async_trait]
pub trait HttpClient: Send + Sync + 'static {
    /// Sends a request and buffers the complete response body.
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpClientError>;

    /// Sends a request and returns a streaming response body.
    async fn stream(&self, request: HttpRequest) -> Result<StreamingResponse, HttpClientError>;
}
