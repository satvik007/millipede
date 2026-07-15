use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::anyhow;
use async_trait::async_trait;
use futures_util::TryStreamExt;
use http::header::{COOKIE, LOCATION, USER_AGENT};
use millipede_core::{
    http_client::{HttpClient, HttpClientError, HttpRequest, HttpResponse, StreamingResponse},
    request::{Method, RequestBody},
};
use url::Url;

/// Configuration for [`ReqwestClient`].
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use millipede_http::ReqwestClientOptions;
///
/// let options = ReqwestClientOptions::default()
///     .with_connect_timeout(Duration::from_secs(5))
///     .with_default_timeout(Duration::from_secs(20))
///     .with_max_cached_clients(4)
///     .with_default_user_agent(None);
/// let client = millipede_http::ReqwestClient::with_options(options)?;
/// # let _ = client;
/// # Ok::<(), millipede_core::http_client::HttpClientError>(())
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ReqwestClientOptions {
    /// Maximum time allowed while establishing a connection.
    pub connect_timeout: Duration,
    /// Request timeout used when a request does not provide one.
    pub default_timeout: Duration,
    /// Maximum number of proxy-specific clients retained in the simple cache.
    pub max_cached_clients: usize,
    /// User-Agent inserted when a request does not already contain one.
    pub default_user_agent: Option<String>,
}

impl Default for ReqwestClientOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            default_timeout: Duration::from_secs(30),
            max_cached_clients: 8,
            default_user_agent: Some(
                "millipede/0.1 (+https://github.com/satvik007/millipede)".to_owned(),
            ),
        }
    }
}

impl ReqwestClientOptions {
    /// Sets the connection timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sets the default request timeout.
    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = timeout;
        self
    }

    /// Sets the maximum number of cached clients.
    pub fn with_max_cached_clients(mut self, maximum: usize) -> Self {
        self.max_cached_clients = maximum;
        self
    }

    /// Sets or disables the default User-Agent.
    pub fn with_default_user_agent(mut self, user_agent: Option<String>) -> Self {
        self.default_user_agent = user_agent;
        self
    }
}

/// A reqwest-backed [`HttpClient`] with manual redirect and cookie handling.
///
/// # Examples
///
/// ```
/// use millipede_http::ReqwestClient;
///
/// let client = ReqwestClient::new()?;
/// # Ok::<(), millipede_core::http_client::HttpClientError>(())
/// ```
pub struct ReqwestClient {
    options: ReqwestClientOptions,
    clients: Mutex<HashMap<Option<Url>, Arc<reqwest::Client>>>,
}

impl ReqwestClient {
    /// Creates a client with default options.
    pub fn new() -> Result<Self, HttpClientError> {
        Self::with_options(ReqwestClientOptions::default())
    }

    /// Creates a client with the supplied options.
    pub fn with_options(options: ReqwestClientOptions) -> Result<Self, HttpClientError> {
        let client = Arc::new(Self::build_client(&options, None)?);
        let mut clients = HashMap::new();
        clients.insert(None, client);
        Ok(Self {
            options,
            clients: Mutex::new(clients),
        })
    }

    fn build_client(
        options: &ReqwestClientOptions,
        proxy: Option<&Url>,
    ) -> Result<reqwest::Client, HttpClientError> {
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(options.connect_timeout)
            .timeout(options.default_timeout);

        if let Some(proxy_url) = proxy {
            let mut reqwest_proxy = reqwest::Proxy::all(proxy_url.as_str())
                .map_err(|error| HttpClientError::build(anyhow::Error::new(error)))?;
            if !proxy_url.username().is_empty() {
                let username = percent_decode(proxy_url.username());
                let password = percent_decode(proxy_url.password().unwrap_or_default());
                reqwest_proxy = reqwest_proxy.basic_auth(&username, &password);
            }
            builder = builder.proxy(reqwest_proxy);
        } else {
            builder = builder.no_proxy();
        }

        builder
            .build()
            .map_err(|error| HttpClientError::build(anyhow::Error::new(error)))
    }

    fn client_for(&self, proxy: Option<&Url>) -> Result<Arc<reqwest::Client>, HttpClientError> {
        let key = proxy.cloned();
        let mut clients = self
            .clients
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(client) = clients.get(&key) {
            return Ok(Arc::clone(client));
        }

        let client = Arc::new(Self::build_client(&self.options, proxy)?);
        // This intentionally simple policy bounds proxy-specific client growth.
        if clients.len() >= self.options.max_cached_clients {
            clients.clear();
        }
        clients.insert(key, Arc::clone(&client));
        Ok(client)
    }

    async fn execute_following_redirects(
        &self,
        request: &HttpRequest,
    ) -> Result<(reqwest::Response, Vec<Url>), HttpClientError> {
        let mut current_url = request.url.clone();
        let mut current_method = request.method.clone();
        let mut current_body = request.body.clone();
        let mut chain = Vec::new();

        loop {
            let client = self.client_for(request.proxy.as_ref())?;
            let mut headers = request.headers.clone();
            if !headers.contains_key(USER_AGENT) {
                if let Some(user_agent) = &self.options.default_user_agent {
                    let value = user_agent.parse().map_err(|error| {
                        HttpClientError::invalid_request(anyhow!(
                            "invalid default User-Agent: {error}"
                        ))
                    })?;
                    headers.insert(USER_AGENT, value);
                }
            }
            if let Some(jar) = &request.cookie_jar {
                if let Some(jar_cookie) = jar.cookie_header_for(&current_url) {
                    if let Some(existing) = headers.get(COOKIE) {
                        let mut combined = Vec::with_capacity(
                            existing.as_bytes().len() + 2 + jar_cookie.as_bytes().len(),
                        );
                        combined.extend_from_slice(existing.as_bytes());
                        combined.extend_from_slice(b"; ");
                        combined.extend_from_slice(jar_cookie.as_bytes());
                        let value = http::HeaderValue::from_bytes(&combined).map_err(|error| {
                            HttpClientError::invalid_request(anyhow::Error::new(error))
                        })?;
                        headers.insert(COOKIE, value);
                    } else {
                        headers.insert(COOKIE, jar_cookie);
                    }
                }
            }

            let mut builder = client
                .request(current_method.clone(), current_url.clone())
                .headers(headers);
            if let Some(body) = &current_body {
                builder = match body {
                    RequestBody::Bytes(bytes) => builder.body(bytes.clone()),
                    RequestBody::Form(pairs) => builder.form(pairs),
                    RequestBody::Json(value) => builder.json(value),
                };
            }
            if let Some(timeout) = request.timeout {
                builder = builder.timeout(timeout);
            }

            let response = builder.send().await.map_err(map_reqwest_error)?;
            let status = response.status();
            if let Some(jar) = &request.cookie_jar {
                jar.store_response_cookies(&current_url, response.headers());
            }

            if status.is_redirection() {
                if let Some(location) = response.headers().get(LOCATION) {
                    if chain.len() as u32 >= request.max_redirects {
                        return Err(HttpClientError::redirect(anyhow!(
                            "exceeded {} redirects",
                            request.max_redirects
                        )));
                    }
                    let location = location.to_str().map_err(|error| {
                        HttpClientError::redirect(anyhow!(
                            "invalid redirect Location header: {error}"
                        ))
                    })?;
                    let next_url = current_url.join(location).map_err(|error| {
                        HttpClientError::redirect(anyhow!("invalid redirect target: {error}"))
                    })?;
                    chain.push(current_url);

                    // Match browser-compatible behavior for legacy POST redirects.
                    if status == http::StatusCode::SEE_OTHER
                        || ((status == http::StatusCode::MOVED_PERMANENTLY
                            || status == http::StatusCode::FOUND)
                            && current_method != Method::GET
                            && current_method != Method::HEAD)
                    {
                        current_method = Method::GET;
                        current_body = None;
                    }
                    current_url = next_url;
                    continue;
                }
            }

            return Ok((response, chain));
        }
    }
}

#[async_trait]
impl HttpClient for ReqwestClient {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpClientError> {
        let (response, chain) = self.execute_following_redirects(&request).await?;
        let url = response.url().clone();
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .bytes()
            .await
            .map_err(|error| HttpClientError::decode(anyhow::Error::new(error)))?;
        Ok(HttpResponse::new(url, status, headers, body).with_redirect_chain(chain))
    }

    async fn stream(&self, request: HttpRequest) -> Result<StreamingResponse, HttpClientError> {
        let (response, _chain) = self.execute_following_redirects(&request).await?;
        let url = response.url().clone();
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .bytes_stream()
            .map_err(|error| HttpClientError::io(anyhow::Error::new(error)));
        Ok(StreamingResponse::new(url, status, headers, Box::pin(body)))
    }
}

impl fmt::Debug for ReqwestClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cached_clients = self
            .clients
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .len();
        formatter
            .debug_struct("ReqwestClient")
            .field("options", &self.options)
            .field("cached_clients", &cached_clients)
            .finish()
    }
}

fn map_reqwest_error(error: reqwest::Error) -> HttpClientError {
    if error.is_timeout() {
        HttpClientError::timeout(anyhow::Error::new(error))
    } else if error.is_connect() {
        HttpClientError::connect(anyhow::Error::new(error))
    } else if error.is_builder() || error.is_request() {
        HttpClientError::invalid_request(anyhow::Error::new(error))
    } else if error.is_decode() || error.is_body() {
        HttpClientError::decode(anyhow::Error::new(error))
    } else {
        HttpClientError::other(anyhow::Error::new(error))
    }
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let (Some(high), Some(low)) = (hex(bytes[index + 1]), hex(bytes[index + 2])) {
                decoded.push(high * 16 + low);
                index += 3;
                continue;
            }
        }
        decoded.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
