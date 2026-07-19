//! Instrumented axum server serving a pre-rendered [`SiteSpec`].
//!
//! One handler code path for ALL engines: the server keys purely on the
//! request path and headers and cannot distinguish millipede, spider, or the
//! baseline client (review A-5). Instrumentation is cheap (atomics; the
//! per-path table is pre-built, so the hot path is a lock-free lookup).

use std::collections::HashMap;
use std::io::Write as _;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, Response, StatusCode, header};
use bytes::Bytes;
use tokio::net::{TcpListener, TcpStream};

use crate::scenario::SiteSpec;

/// One served page: identity body plus (optionally) a pre-compressed variant.
struct PageEntry {
    identity: Bytes,
    /// Present only when the site is gzip-enabled; compressed once at startup.
    gzip: Option<Bytes>,
    hits: AtomicU64,
}

struct RedirectEntry {
    location: String,
    hits: AtomicU64,
}

/// Shared server state + instrumentation counters.
pub struct ServerState {
    pages: HashMap<String, PageEntry>,
    redirects: HashMap<String, RedirectEntry>,
    latency: Option<Duration>,
    robots_path: String,
    robots_body: Bytes,
    robots_hits: AtomicU64,
    off_host_hits: AtomicU64,
    unknown_hits: AtomicU64,
    /// Body bytes actually written on the wire (compressed size when gzip).
    bytes_on_wire: AtomicU64,
    /// Requests whose `Accept-Encoding` permitted gzip.
    accept_encoding_gzip: AtomicU64,
    /// Requests without gzip in `Accept-Encoding` (or no header).
    accept_encoding_identity: AtomicU64,
    /// TCP connections accepted (counted by the listener wrapper).
    connections: AtomicU64,
}

/// Per-trial snapshot of the instrumentation counters.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ServerSnapshot {
    /// Number of distinct page paths hit at least once.
    pub unique_pages_hit: u64,
    /// Sum over pages of `hits - 1` for pages hit more than once.
    pub duplicate_page_hits: u64,
    /// Number of distinct redirect paths hit at least once.
    pub unique_redirects_hit: u64,
    /// Sum over redirect paths of `hits - 1` (each must be hit exactly once).
    pub duplicate_redirect_hits: u64,
    /// Hits on `/<nonce>/robots.txt` (must be zero for a valid trial).
    pub robots_hits: u64,
    /// Requests carrying `Host: localhost...` (off-host leak; must be zero).
    pub off_host_hits: u64,
    /// Requests for paths outside the site map (must be zero).
    pub unknown_hits: u64,
    /// Body bytes written on the wire (compressed size when gzip applies).
    pub bytes_on_wire: u64,
    /// Requests whose Accept-Encoding permitted gzip.
    pub accept_encoding_gzip: u64,
    /// Requests without gzip in Accept-Encoding.
    pub accept_encoding_identity: u64,
    /// TCP connections accepted during the window.
    pub connections: u64,
}

impl ServerState {
    fn new(site: &SiteSpec, nonce: &str) -> anyhow::Result<Self> {
        let mut pages = HashMap::with_capacity(site.pages.len());
        for (path, body) in &site.pages {
            let gzip = if site.gzip {
                let mut enc =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                enc.write_all(body)?;
                Some(Bytes::from(enc.finish()?))
            } else {
                None
            };
            pages.insert(
                path.clone(),
                PageEntry {
                    identity: body.clone(),
                    gzip,
                    hits: AtomicU64::new(0),
                },
            );
        }
        let redirects = site
            .redirects
            .iter()
            .map(|(path, target)| {
                (
                    path.clone(),
                    RedirectEntry {
                        location: target.clone(),
                        hits: AtomicU64::new(0),
                    },
                )
            })
            .collect();
        Ok(Self {
            pages,
            redirects,
            latency: site.latency,
            robots_path: format!("/{nonce}/robots.txt"),
            robots_body: Bytes::from_static(b"User-agent: *\nAllow: /\n"),
            robots_hits: AtomicU64::new(0),
            off_host_hits: AtomicU64::new(0),
            unknown_hits: AtomicU64::new(0),
            bytes_on_wire: AtomicU64::new(0),
            accept_encoding_gzip: AtomicU64::new(0),
            accept_encoding_identity: AtomicU64::new(0),
            connections: AtomicU64::new(0),
        })
    }

    /// Takes a snapshot of all counters and resets them to zero, so each trial
    /// gets an independent validation window.
    pub fn snapshot_and_reset(&self) -> ServerSnapshot {
        let mut unique_pages_hit = 0u64;
        let mut duplicate_page_hits = 0u64;
        for entry in self.pages.values() {
            let hits = entry.hits.swap(0, Ordering::AcqRel);
            if hits > 0 {
                unique_pages_hit += 1;
                duplicate_page_hits += hits - 1;
            }
        }
        let mut unique_redirects_hit = 0u64;
        let mut duplicate_redirect_hits = 0u64;
        for entry in self.redirects.values() {
            let hits = entry.hits.swap(0, Ordering::AcqRel);
            if hits > 0 {
                unique_redirects_hit += 1;
                duplicate_redirect_hits += hits - 1;
            }
        }
        ServerSnapshot {
            unique_pages_hit,
            duplicate_page_hits,
            unique_redirects_hit,
            duplicate_redirect_hits,
            robots_hits: self.robots_hits.swap(0, Ordering::AcqRel),
            off_host_hits: self.off_host_hits.swap(0, Ordering::AcqRel),
            unknown_hits: self.unknown_hits.swap(0, Ordering::AcqRel),
            bytes_on_wire: self.bytes_on_wire.swap(0, Ordering::AcqRel),
            accept_encoding_gzip: self.accept_encoding_gzip.swap(0, Ordering::AcqRel),
            accept_encoding_identity: self.accept_encoding_identity.swap(0, Ordering::AcqRel),
            connections: self.connections.swap(0, Ordering::AcqRel),
        }
    }
}

/// A running server bound to `127.0.0.1:0`.
pub struct ServerHandle {
    /// Bound address (ephemeral port).
    pub addr: SocketAddr,
    /// Shared state for snapshot/reset between trials.
    pub state: Arc<ServerState>,
    task: tokio::task::JoinHandle<()>,
}

impl ServerHandle {
    /// Base URL (`http://127.0.0.1:<port>`), no trailing slash.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Counts TCP accepts so pooling behavior is observable per trial.
struct CountingListener {
    inner: TcpListener,
    state: Arc<ServerState>,
}

impl axum::serve::Listener for CountingListener {
    type Io = TcpStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.inner.accept().await {
                Ok((stream, addr)) => {
                    self.state.connections.fetch_add(1, Ordering::AcqRel);
                    // Loopback benchmark: minimize per-request latency jitter.
                    let _ = stream.set_nodelay(true);
                    return (stream, addr);
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(1)).await,
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

/// Starts the server for a site. Must run inside a tokio runtime.
pub async fn start(site: &SiteSpec, nonce: &str) -> anyhow::Result<ServerHandle> {
    let state = Arc::new(ServerState::new(site, nonce)?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let app = axum::Router::new()
        .fallback(handle)
        .with_state(Arc::clone(&state));
    let counting = CountingListener {
        inner: listener,
        state: Arc::clone(&state),
    };
    let task = tokio::spawn(async move {
        // Runs until the handle is dropped (task abort).
        let _ = axum::serve(counting, app).await;
    });
    Ok(ServerHandle { addr, state, task })
}

/// Single handler code path for every request from every engine.
async fn handle(State(state): State<Arc<ServerState>>, req: Request<Body>) -> Response<Body> {
    // Off-host leak detection: seeds use 127.0.0.1; a request arriving with a
    // `localhost` Host header means an engine followed the off-host trap link.
    let host = req
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if host == "localhost" || host.starts_with("localhost:") {
        state.off_host_hits.fetch_add(1, Ordering::AcqRel);
    }

    let path = req.uri().path().to_owned();

    if let Some(latency) = state.latency {
        tokio::time::sleep(latency).await;
    }

    if path == state.robots_path || path == "/robots.txt" {
        state.robots_hits.fetch_add(1, Ordering::AcqRel);
        let body = state.robots_body.clone();
        state
            .bytes_on_wire
            .fetch_add(body.len() as u64, Ordering::AcqRel);
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .header(header::CONTENT_LENGTH, body.len())
            .header(header::CACHE_CONTROL, "no-store")
            .body(Body::from(body))
            .expect("static robots response");
    }

    if let Some(redirect) = state.redirects.get(&path) {
        redirect.hits.fetch_add(1, Ordering::AcqRel);
        return Response::builder()
            .status(StatusCode::MOVED_PERMANENTLY)
            .header(header::LOCATION, redirect.location.as_str())
            .header(header::CACHE_CONTROL, "no-store")
            .header(header::CONTENT_LENGTH, 0)
            .body(Body::empty())
            .expect("static redirect response");
    }

    let Some(page) = state.pages.get(&path) else {
        state.unknown_hits.fetch_add(1, Ordering::AcqRel);
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(header::CACHE_CONTROL, "no-store")
            .header(header::CONTENT_LENGTH, 0)
            .body(Body::empty())
            .expect("static 404 response");
    };
    page.hits.fetch_add(1, Ordering::AcqRel);

    // Content negotiation (only meaningful when the site is gzip-enabled).
    let accepts_gzip = req
        .headers()
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().contains("gzip"));
    if accepts_gzip {
        state.accept_encoding_gzip.fetch_add(1, Ordering::AcqRel);
    } else {
        state
            .accept_encoding_identity
            .fetch_add(1, Ordering::AcqRel);
    }

    let (body, encoding) = match (&page.gzip, accepts_gzip) {
        (Some(gz), true) => (gz.clone(), Some("gzip")),
        _ => (page.identity.clone(), None),
    };
    state
        .bytes_on_wire
        .fetch_add(body.len() as u64, Ordering::AcqRel);

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::CONTENT_LENGTH, body.len())
        .header(header::CACHE_CONTROL, "no-store");
    if let Some(enc) = encoding {
        builder = builder.header(header::CONTENT_ENCODING, enc);
    }
    builder
        .body(Body::from(body))
        .expect("static page response")
}
