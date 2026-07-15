use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

use anyhow::anyhow;
use async_trait::async_trait;
use millipede_core::{
    http_client::{HttpClient, HttpClientError, HttpRequest, HttpResponse, StreamingResponse},
    request::{Method, Request},
};
use tokio::sync::OnceCell;

#[derive(Hash, Eq, PartialEq, Clone)]
struct CoalesceKey {
    unique_key: String,
    proxy: Option<String>,
    jar: Option<usize>,
}

type SharedResponse = Arc<OnceCell<Result<HttpResponse, String>>>;
type InFlightRequests = HashMap<CoalesceKey, SharedResponse>;

struct InFlightGuard<'a> {
    in_flight: &'a Mutex<InFlightRequests>,
    key: CoalesceKey,
    cell: SharedResponse,
    active: bool,
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        // `OnceCell` cancels an initializer when its caller is dropped and lets
        // another waiter take over. Keep the entry discoverable during that
        // handoff; only the caller that observes a settled cell may remove it.
        if !self.active || self.cell.get().is_none() {
            return;
        }
        let mut in_flight = self
            .in_flight
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if in_flight
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.cell))
        {
            in_flight.remove(&self.key);
        }
    }
}

/// An HTTP client decorator that joins safe, identical requests already in flight.
///
/// Only bodyless `GET` and `HEAD` requests are coalesced. Cookie-jar identity and
/// proxy selection are part of the key, so distinct sessions and routes never share
/// a fetch. Joined callers of a failed request receive an `Other` error because
/// [`HttpClientError`] is not cloneable; this intentionally loses connect/timeout
/// variant fidelity.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use millipede_http::{CoalescingClient, ReqwestClient};
///
/// let inner = Arc::new(ReqwestClient::new()?);
/// let client = CoalescingClient::new(inner);
/// # Ok::<(), millipede_core::http_client::HttpClientError>(())
/// ```
pub struct CoalescingClient {
    inner: Arc<dyn HttpClient>,
    in_flight: Mutex<InFlightRequests>,
}

impl CoalescingClient {
    /// Wraps an HTTP client with in-flight request coalescing.
    pub fn new(inner: Arc<dyn HttpClient>) -> Self {
        Self {
            inner,
            in_flight: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl HttpClient for CoalescingClient {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpClientError> {
        if (request.method != Method::GET && request.method != Method::HEAD)
            || request.body.is_some()
        {
            return self.inner.send(request).await;
        }

        let key = CoalesceKey {
            unique_key: Request::compute_unique_key(&request.url, &request.method, None),
            proxy: request.proxy.as_ref().map(ToString::to_string),
            jar: request
                .cookie_jar
                .as_ref()
                .map(|jar| Arc::as_ptr(jar) as usize),
        };
        let cell = {
            let mut in_flight = self
                .in_flight
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(cell) = in_flight.get(&key) {
                Arc::clone(cell)
            } else {
                let cell = Arc::new(OnceCell::new());
                in_flight.insert(key.clone(), Arc::clone(&cell));
                cell
            }
        };

        let mut cleanup = InFlightGuard {
            in_flight: &self.in_flight,
            key,
            cell: Arc::clone(&cell),
            active: false,
        };
        let mut leader_result = None;
        let shared_result = cell
            .get_or_init(|| async {
                cleanup.active = true;
                let result = self.inner.send(request).await;
                let follower_result = result
                    .as_ref()
                    .map(Clone::clone)
                    .map_err(ToString::to_string);
                leader_result = Some(result);
                follower_result
            })
            .await;

        leader_result.unwrap_or_else(|| {
            shared_result.clone().map_err(|message| {
                HttpClientError::other(anyhow!("coalesced request failed: {message}"))
            })
        })
    }

    async fn stream(&self, request: HttpRequest) -> Result<StreamingResponse, HttpClientError> {
        self.inner.stream(request).await
    }
}

impl fmt::Debug for CoalescingClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let in_flight = self
            .in_flight
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .len();
        formatter
            .debug_struct("CoalescingClient")
            .field("inner", &"dyn HttpClient")
            .field("in_flight", &in_flight)
            .finish()
    }
}
