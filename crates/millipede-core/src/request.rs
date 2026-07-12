//! Request values and their builder API.

use serde::{Deserialize, Serialize};
use std::fmt;
use url::Url;

pub use http::{HeaderMap, Method};

/// A crawl request and its processing state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Request {
    /// Stable identifier derived from `unique_key`.
    pub id: RequestId,
    /// URL to navigate to.
    pub url: Url,
    /// Final URL after redirects, when navigation has occurred.
    pub loaded_url: Option<Url>,
    /// Queue deduplication key.
    pub unique_key: String,
    /// HTTP method.
    #[serde(with = "method_serde")]
    pub method: Method,
    /// HTTP headers, including repeated values.
    #[serde(with = "header_map_serde")]
    pub headers: HeaderMap,
    /// Optional request body.
    pub body: Option<RequestBody>,
    /// User-defined structured metadata.
    pub user_data: UserData,
    /// Optional routing label.
    pub label: Option<String>,
    /// Number of retry attempts made.
    pub retry_count: u32,
    /// Number of session rotations made.
    pub session_rotation_count: u32,
    /// Per-request retry limit override.
    pub max_retries: Option<u32>,
    /// Whether retries are disabled.
    pub no_retry: bool,
    /// Errors recorded during processing.
    pub error_messages: Vec<String>,
    /// Time at which processing completed.
    #[serde(with = "time::serde::rfc3339::option")]
    pub handled_at: Option<time::OffsetDateTime>,
    /// Current lifecycle state.
    pub state: RequestState,
    /// Depth at which this request was discovered.
    pub crawl_depth: u32,
    /// Whether navigation should be skipped.
    pub skip_navigation: bool,
    // `enqueue_strategy` is intentionally omitted until ROADMAP Phase 5.
}

/// The processing lifecycle state of a request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestState {
    /// Waiting to be processed.
    #[default]
    Unprocessed,
    /// Running pre-navigation hooks.
    BeforeNav,
    /// Running post-navigation hooks.
    AfterNav,
    /// Running the request handler.
    RequestHandler,
    /// Successfully completed.
    Done,
    /// Running the error handler.
    ErrorHandler,
    /// Permanently failed.
    Error,
    /// Deliberately skipped.
    Skipped,
}

/// A deterministic request identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId(String);

impl RequestId {
    /// Derives an identifier deterministically from a request unique key.
    pub fn from_unique_key(unique_key: &str) -> Self {
        let h1 = fnv1a64(unique_key.as_bytes());
        let h2 = fnv1a64_seeded(unique_key.as_bytes(), h1);
        Self(format!("{h1:016x}{h2:016x}"))
    }

    /// Returns the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A supported request-body representation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestBody {
    /// Uninterpreted bytes.
    Bytes(Vec<u8>),
    /// Ordered form key-value pairs.
    Form(Vec<(String, String)>),
    /// A JSON value.
    Json(serde_json::Value),
}

impl RequestBody {
    /// Returns the canonical bytes used for unique-key hashing.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        match self {
            Self::Bytes(bytes) => bytes.clone(),
            Self::Form(pairs) => pairs
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join("&")
                .into_bytes(),
            Self::Json(value) => serde_json::to_vec(value).expect(
                // Serialization can only fail for non-string map keys, which Value forbids.
                "a serde_json::Value is always serializable",
            ),
        }
    }
}

/// User-defined JSON metadata attached to a request.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UserData(pub serde_json::Map<String, serde_json::Value>);

impl UserData {
    /// Deserializes the value at `key`, returning `None` when it is absent.
    pub fn get_typed<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Option<Result<T, serde_json::Error>> {
        self.0.get(key).cloned().map(serde_json::from_value)
    }

    /// Serializes and stores a typed value at `key`.
    pub fn set_typed<T: Serialize>(
        &mut self,
        key: &str,
        value: &T,
    ) -> Result<(), serde_json::Error> {
        self.0.insert(key.to_owned(), serde_json::to_value(value)?);
        Ok(())
    }

    /// Returns whether no user data is stored.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the raw JSON value stored at `key`.
    pub fn get(&self, key: &str) -> Option<&serde_json::Value> {
        self.0.get(key)
    }
}

impl Request {
    /// Creates a builder configured for a GET request.
    pub fn get(url: impl IntoUrl) -> RequestBuilder {
        Self::builder().url(url).method(Method::GET)
    }

    /// Creates a builder configured for a POST request.
    pub fn post(url: impl IntoUrl) -> RequestBuilder {
        Self::builder().url(url).method(Method::POST)
    }

    /// Creates an empty request builder.
    pub fn builder() -> RequestBuilder {
        RequestBuilder::default()
    }

    /// Computes a deterministic queue deduplication key.
    pub fn compute_unique_key(url: &Url, method: &Method, body: Option<&RequestBody>) -> String {
        let mut normalized = url.clone();
        normalized.set_fragment(None);
        if *method == Method::GET && body.is_none() {
            normalized.into()
        } else {
            let body_bytes = body.map(RequestBody::canonical_bytes).unwrap_or_default();
            format!(
                "{}({:016x}):{}",
                method.as_str(),
                fnv1a64(&body_bytes),
                normalized
            )
        }
    }
}

/// Conversion into a parsed URL for request builders.
pub trait IntoUrl {
    /// Converts this value into a URL.
    fn into_url(self) -> Result<Url, url::ParseError>;
}

impl IntoUrl for Url {
    fn into_url(self) -> Result<Url, url::ParseError> {
        Ok(self)
    }
}

impl IntoUrl for &str {
    fn into_url(self) -> Result<Url, url::ParseError> {
        Url::parse(self)
    }
}

impl IntoUrl for String {
    fn into_url(self) -> Result<Url, url::ParseError> {
        Url::parse(&self)
    }
}

impl IntoUrl for &String {
    fn into_url(self) -> Result<Url, url::ParseError> {
        Url::parse(self)
    }
}

#[derive(Debug)]
struct PendingHeader {
    name: String,
    value: String,
}

/// Builds a [`Request`] while deferring parsing and serialization errors.
#[derive(Debug, Default)]
pub struct RequestBuilder {
    url: Option<Result<Url, url::ParseError>>,
    method: Option<Method>,
    headers: HeaderMap,
    pending_headers: Vec<PendingHeader>,
    body: Option<RequestBody>,
    serialization_error: Option<serde_json::Error>,
    user_data: UserData,
    label: Option<String>,
    max_retries: Option<u32>,
    no_retry: bool,
    skip_navigation: bool,
    unique_key: Option<String>,
    pub(crate) forefront: bool,
    crawl_depth: u32,
}

impl RequestBuilder {
    /// Builds and adds this request to a queue.
    pub async fn enqueue(
        self,
        queue: &dyn crate::storage::RequestQueue,
    ) -> Result<crate::storage::QueueOpInfo, crate::errors::CrawlError> {
        let forefront = self.is_forefront();
        let request = self.build()?;
        Ok(queue
            .add(
                request,
                crate::storage::AddOptions {
                    forefront,
                    ..Default::default()
                },
            )
            .await?)
    }

    /// Sets the request URL.
    pub fn url(mut self, url: impl IntoUrl) -> Self {
        self.url = Some(url.into_url());
        self
    }

    /// Sets the HTTP method.
    pub fn method(mut self, method: Method) -> Self {
        self.method = Some(method);
        self
    }

    /// Appends a header, deferring validation until [`Self::build`].
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.pending_headers.push(PendingHeader {
            name: name.to_owned(),
            value: value.to_owned(),
        });
        self
    }

    /// Replaces all currently configured headers.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self.pending_headers.clear();
        self
    }

    /// Sets the request body.
    pub fn body(mut self, body: RequestBody) -> Self {
        self.body = Some(body);
        self
    }

    /// Serializes and sets a JSON request body.
    pub fn json<T: Serialize>(mut self, value: &T) -> Self {
        match serde_json::to_value(value) {
            Ok(value) => self.body = Some(RequestBody::Json(value)),
            Err(error) => self.serialization_error = Some(error),
        }
        self
    }

    /// Sets an ordered form request body.
    pub fn form(mut self, pairs: impl IntoIterator<Item = (String, String)>) -> Self {
        self.body = Some(RequestBody::Form(pairs.into_iter().collect()));
        self
    }

    /// Sets the routing label.
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Replaces the request's user data.
    pub fn user_data(mut self, user_data: UserData) -> Self {
        self.user_data = user_data;
        self
    }

    /// Serializes and inserts an entry into user data.
    pub fn user_data_entry<T: Serialize>(mut self, key: impl Into<String>, value: &T) -> Self {
        match serde_json::to_value(value) {
            Ok(value) => {
                self.user_data.0.insert(key.into(), value);
            }
            Err(error) => self.serialization_error = Some(error),
        }
        self
    }

    /// Sets the per-request retry limit.
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = Some(max_retries);
        self
    }

    /// Enables or disables retries.
    pub fn no_retry(mut self, no_retry: bool) -> Self {
        self.no_retry = no_retry;
        self
    }

    /// Controls whether navigation is skipped.
    pub fn skip_navigation(mut self, skip_navigation: bool) -> Self {
        self.skip_navigation = skip_navigation;
        self
    }

    /// Overrides the computed queue deduplication key.
    pub fn unique_key(mut self, unique_key: impl Into<String>) -> Self {
        self.unique_key = Some(unique_key.into());
        self
    }

    /// Controls whether a future queue enqueue places this request at the front.
    pub fn forefront(mut self, forefront: bool) -> Self {
        self.forefront = forefront;
        self
    }

    /// Returns whether future queue enqueue convenience should use the forefront.
    pub fn is_forefront(&self) -> bool {
        self.forefront
    }

    /// Sets the discovery depth.
    pub fn crawl_depth(mut self, crawl_depth: u32) -> Self {
        self.crawl_depth = crawl_depth;
        self
    }

    /// Validates this builder and creates a request.
    pub fn build(mut self) -> Result<Request, RequestBuildError> {
        if let Some(error) = self.serialization_error {
            return Err(error.into());
        }
        let url = self.url.ok_or(RequestBuildError::MissingUrl)??;
        for pending in self.pending_headers {
            let name =
                http::header::HeaderName::from_bytes(pending.name.as_bytes()).map_err(|error| {
                    RequestBuildError::InvalidHeader {
                        name: pending.name.clone(),
                        message: error.to_string(),
                    }
                })?;
            let value =
                http::HeaderValue::from_bytes(pending.value.as_bytes()).map_err(|error| {
                    RequestBuildError::InvalidHeader {
                        name: pending.name.clone(),
                        message: error.to_string(),
                    }
                })?;
            self.headers.append(name, value);
        }
        let method = self.method.unwrap_or(Method::GET);
        let unique_key = self
            .unique_key
            .unwrap_or_else(|| Request::compute_unique_key(&url, &method, self.body.as_ref()));
        let id = RequestId::from_unique_key(&unique_key);
        Ok(Request {
            id,
            url,
            loaded_url: None,
            unique_key,
            method,
            headers: self.headers,
            body: self.body,
            user_data: self.user_data,
            label: self.label,
            retry_count: 0,
            session_rotation_count: 0,
            max_retries: self.max_retries,
            no_retry: self.no_retry,
            error_messages: Vec::new(),
            handled_at: None,
            state: RequestState::Unprocessed,
            crawl_depth: self.crawl_depth,
            skip_navigation: self.skip_navigation,
        })
    }
}

/// Errors encountered while building a request.
#[derive(Debug, thiserror::Error)]
pub enum RequestBuildError {
    /// The builder has no URL.
    #[error("request URL is missing")]
    MissingUrl,
    /// The supplied URL could not be parsed.
    #[error("invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
    /// A header name or value is invalid.
    #[error("invalid header {name}: {message}")]
    InvalidHeader {
        /// Invalid header name as supplied by the caller.
        name: String,
        /// Parser error describing the invalid header.
        message: String,
    },
    /// JSON serialization failed.
    #[error("user data serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    fnv1a64_seeded(bytes, 0xcbf29ce484222325)
}

fn fnv1a64_seeded(bytes: &[u8], seed: u64) -> u64 {
    bytes.iter().fold(seed, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

mod method_serde {
    use http::Method;
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    pub fn serialize<S: Serializer>(method: &Method, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(method.as_str())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Method, D::Error> {
        let value = String::deserialize(deserializer)?;
        Method::from_bytes(value.as_bytes()).map_err(D::Error::custom)
    }
}

mod header_map_serde {
    use http::{HeaderMap, HeaderName, HeaderValue};
    use serde::{
        Deserialize, Deserializer, Serialize, Serializer, de::Error as _, ser::Error as _,
    };

    pub fn serialize<S: Serializer>(headers: &HeaderMap, serializer: S) -> Result<S::Ok, S::Error> {
        headers
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.as_str().to_owned(),
                    value.to_str().map_err(S::Error::custom)?.to_owned(),
                ))
            })
            .collect::<Result<Vec<(String, String)>, S::Error>>()?
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<HeaderMap, D::Error> {
        let pairs = Vec::<(String, String)>::deserialize(deserializer)?;
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(D::Error::custom)?;
            let value = HeaderValue::from_bytes(value.as_bytes()).map_err(D::Error::custom)?;
            headers.append(name, value);
        }
        Ok(headers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_happy_path() {
        let request = Request::get("https://example.com/a?x=1")
            .label("l")
            .build()
            .unwrap();
        assert_eq!(request.label.as_deref(), Some("l"));
        assert_eq!(request.method, Method::GET);
    }

    #[test]
    fn unique_key_override_is_respected() {
        let request = Request::get("https://example.com")
            .unique_key("mine")
            .build()
            .unwrap();
        assert_eq!(request.unique_key, "mine");
    }

    #[test]
    fn get_key_is_fragmentless_url() {
        let request = Request::get("https://example.com/a#frag").build().unwrap();
        assert_eq!(request.unique_key, "https://example.com/a");
    }

    #[test]
    fn fragment_does_not_affect_key() {
        let first = Request::get("https://example.com/a#frag").build().unwrap();
        let second = Request::get("https://example.com/a").build().unwrap();
        assert_eq!(first.unique_key, second.unique_key);
    }

    #[test]
    fn post_body_affects_key_deterministically() {
        let build = |bytes| {
            Request::post("https://example.com/a")
                .body(RequestBody::Bytes(bytes))
                .build()
                .unwrap()
        };
        assert_eq!(build(vec![1]).unique_key, build(vec![1]).unique_key);
        assert_ne!(build(vec![1]).unique_key, build(vec![2]).unique_key);
    }

    #[test]
    fn request_id_is_deterministic() {
        assert_eq!(
            RequestId::from_unique_key("key"),
            RequestId::from_unique_key("key")
        );
    }

    #[test]
    fn user_data_typed_roundtrip() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Example {
            number: u32,
        }

        let mut data = UserData::default();
        data.set_typed("example", &Example { number: 7 }).unwrap();
        assert_eq!(
            data.get_typed::<Example>("example").unwrap().unwrap(),
            Example { number: 7 }
        );
    }

    #[test]
    fn invalid_header_is_reported_at_build() {
        let result = Request::get("https://example.com")
            .header("bad header", "value")
            .build();
        assert!(matches!(
            result,
            Err(RequestBuildError::InvalidHeader { .. })
        ));
    }
}
