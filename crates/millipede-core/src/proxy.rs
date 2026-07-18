//! Proxy configuration and rotation.
//!
//! Tiered configurations implement a simplified Crawlee-compatible policy: blocking escalates a
//! domain, periodic requests probe the next lower tier, and a `None` slot explicitly means a direct
//! request without a proxy.

use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use url::Url;

use crate::{errors::CrawlError, request::Request, session::SessionId};

/// Selection policy for a static proxy list.
///
/// ```
/// use millipede_core::proxy::RotationStrategy;
/// assert_eq!(RotationStrategy::default(), RotationStrategy::RoundRobin);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum RotationStrategy {
    /// Selects proxies in stable cyclic order.
    #[default]
    RoundRobin,
    /// Selects a proxy using the crate's lightweight process-local generator.
    Random,
}

/// Borrowed inputs available to proxy resolution.
///
/// ```
/// use millipede_core::proxy::ProxyResolveContext;
/// let context = ProxyResolveContext::new().attempt(2);
/// assert_eq!(context.attempt, 2);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct ProxyResolveContext<'a> {
    /// Request being routed, when available.
    pub request: Option<&'a Request>,
    /// Checked-out session identifier, when available.
    pub session_id: Option<&'a SessionId>,
    /// Zero-based request attempt.
    pub attempt: u32,
}

impl<'a> ProxyResolveContext<'a> {
    /// Creates an empty resolution context.
    pub fn new() -> Self {
        Self::default()
    }
    /// Sets the current request.
    pub fn request(mut self, value: &'a Request) -> Self {
        self.request = Some(value);
        self
    }
    /// Sets the current session identifier.
    pub fn session_id(mut self, value: &'a SessionId) -> Self {
        self.session_id = Some(value);
        self
    }
    /// Sets the current attempt.
    pub fn attempt(mut self, value: u32) -> Self {
        self.attempt = value;
        self
    }
}

/// Asynchronous custom proxy URL resolver.
///
/// ```
/// # use millipede_core::{errors::CrawlError, proxy::{ProxyResolveContext, ProxyResolver}};
/// # struct Direct;
/// # #[async_trait::async_trait]
/// # impl ProxyResolver for Direct {
/// #   async fn resolve(&self, _: ProxyResolveContext<'_>) -> Result<Option<url::Url>, CrawlError> { Ok(None) }
/// # }
/// ```
#[async_trait::async_trait]
pub trait ProxyResolver: Send + Sync + 'static {
    /// Resolves a proxy URL or chooses a direct request with `None`.
    async fn resolve(&self, ctx: ProxyResolveContext<'_>) -> Result<Option<Url>, CrawlError>;
}

/// Parsed connection details for a selected proxy.
///
/// ```
/// use millipede_core::proxy::ProxyInfo;
/// let info = ProxyInfo::from_url(url::Url::parse("http://proxy.example:8080")?);
/// assert_eq!(info.port, 8080);
/// # Ok::<(), url::ParseError>(())
/// ```
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ProxyInfo {
    /// Complete proxy URL.
    pub url: Url,
    /// Proxy host name.
    pub hostname: String,
    /// Explicit or scheme-default port.
    pub port: u16,
    /// Optional non-empty username.
    pub username: Option<String>,
    /// Optional password.
    pub password: Option<String>,
    /// Tier that supplied this proxy.
    pub tier: Option<u8>,
    /// Session associated with the resolution request.
    pub session_id: Option<SessionId>,
}

impl ProxyInfo {
    /// Parses connection metadata from a URL.
    pub fn from_url(url: Url) -> Self {
        let hostname = url.host_str().unwrap_or_default().to_owned();
        let port = url.port_or_known_default().unwrap_or(80);
        let username = (!url.username().is_empty()).then(|| url.username().to_owned());
        let password = url.password().map(str::to_owned);
        Self {
            url,
            hostname,
            port,
            username,
            password,
            tier: None,
            session_id: None,
        }
    }
    /// Sets the originating tier.
    pub fn with_tier(mut self, value: u8) -> Self {
        self.tier = Some(value);
        self
    }
    /// Sets the associated session.
    pub fn with_session_id(mut self, value: SessionId) -> Self {
        self.session_id = Some(value);
        self
    }
}

enum ProxyInner {
    Static {
        urls: Vec<Url>,
        rotation: RotationStrategy,
        cursor: AtomicUsize,
    },
    Custom(Arc<dyn ProxyResolver>),
    Tiered(TieredState),
}

struct TieredState {
    tiers: Vec<Vec<Option<Url>>>,
    probe_interval: u32,
    domains: Mutex<HashMap<String, DomainTier>>,
}

#[derive(Default)]
struct DomainTier {
    tier: usize,
    requests: u32,
    probing: Option<usize>,
}

/// Static, custom, or per-domain tiered proxy selection.
///
/// ```
/// use millipede_core::proxy::ProxyConfiguration;
/// let config = ProxyConfiguration::round_robin([url::Url::parse("http://proxy.example")?]);
/// assert!(format!("{config:?}").contains("Static"));
/// # Ok::<(), url::ParseError>(())
/// ```
pub struct ProxyConfiguration {
    inner: ProxyInner,
}

impl ProxyConfiguration {
    /// Creates a round-robin static list.
    pub fn round_robin(urls: impl IntoIterator<Item = Url>) -> Self {
        Self::rotating(urls, RotationStrategy::RoundRobin)
    }
    /// Creates a static list using `rotation`.
    pub fn rotating(urls: impl IntoIterator<Item = Url>, rotation: RotationStrategy) -> Self {
        Self {
            inner: ProxyInner::Static {
                urls: urls.into_iter().collect(),
                rotation,
                cursor: AtomicUsize::new(0),
            },
        }
    }
    /// Creates a configuration backed by a custom resolver.
    pub fn custom<R: ProxyResolver>(resolver: R) -> Self {
        Self {
            inner: ProxyInner::Custom(Arc::new(resolver)),
        }
    }
    /// Creates tiered rotation with a probe every 20 requests.
    pub fn tiered(tiers: Vec<Vec<Option<Url>>>) -> Self {
        Self::tiered_with_probe_interval(tiers, 20)
    }
    /// Creates tiered rotation with the requested probe interval.
    pub fn tiered_with_probe_interval(tiers: Vec<Vec<Option<Url>>>, probe_interval: u32) -> Self {
        Self {
            inner: ProxyInner::Tiered(TieredState {
                tiers,
                probe_interval: probe_interval.max(1),
                domains: Mutex::new(HashMap::new()),
            }),
        }
    }

    fn tiered_url(state: &TieredState, ctx: ProxyResolveContext<'_>) -> (Option<Url>, Option<u8>) {
        if state.tiers.is_empty() {
            return (None, None);
        }
        let key = ctx
            .request
            .and_then(|request| request.url.host_str())
            .unwrap_or_default()
            .to_owned();
        let mut domains = state.domains.lock().unwrap_or_else(|e| e.into_inner());
        let domain = domains.entry(key).or_default();
        domain.tier = domain.tier.min(state.tiers.len() - 1);
        domain.requests = domain.requests.saturating_add(1);
        let serving = if domain.tier > 0 && domain.requests % state.probe_interval == 0 {
            let probe = domain.tier - 1;
            domain.probing = Some(probe);
            probe
        } else {
            domain.tier
        };
        let tier = &state.tiers[serving];
        if tier.is_empty() {
            return (None, Some(serving as u8));
        }
        (
            tier[domain.requests as usize % tier.len()].clone(),
            Some(serving as u8),
        )
    }

    async fn resolve(
        &self,
        ctx: ProxyResolveContext<'_>,
    ) -> Result<(Option<Url>, Option<u8>), CrawlError> {
        match &self.inner {
            ProxyInner::Static {
                urls,
                rotation,
                cursor,
            } => {
                if urls.is_empty() {
                    return Ok((None, None));
                }
                let index = match rotation {
                    RotationStrategy::RoundRobin => cursor.fetch_add(1, Ordering::Relaxed),
                    RotationStrategy::Random => crate::util::rand_u64() as usize,
                } % urls.len();
                Ok((Some(urls[index].clone()), None))
            }
            ProxyInner::Custom(resolver) => resolver.resolve(ctx).await.map(|url| (url, None)),
            ProxyInner::Tiered(state) => Ok(Self::tiered_url(state, ctx)),
        }
    }

    /// Selects the next proxy URL, or `None` for a direct request.
    pub async fn new_url(&self, ctx: ProxyResolveContext<'_>) -> Result<Option<Url>, CrawlError> {
        self.resolve(ctx).await.map(|(url, _)| url)
    }
    /// Selects and parses the next proxy, retaining tier and session metadata.
    pub async fn new_proxy_info(
        &self,
        ctx: ProxyResolveContext<'_>,
    ) -> Result<Option<ProxyInfo>, CrawlError> {
        let session_id = ctx.session_id.cloned();
        let (url, tier) = self.resolve(ctx).await?;
        Ok(url.map(|url| {
            let mut info = ProxyInfo::from_url(url);
            info.tier = tier;
            info.session_id = session_id;
            info
        }))
    }
    /// Reports that `target` was blocked, escalating its domain unless a recovery probe failed.
    pub fn report_blocked(&self, target: &Url) {
        let ProxyInner::Tiered(state) = &self.inner else {
            return;
        };
        if state.tiers.is_empty() {
            return;
        }
        let key = target.host_str().unwrap_or_default().to_owned();
        let mut domains = state.domains.lock().unwrap_or_else(|e| e.into_inner());
        let domain = domains.entry(key).or_default();
        if domain.probing.take().is_none() {
            domain.tier = (domain.tier + 1).min(state.tiers.len() - 1);
            domain.requests = 0;
        }
    }
    /// Reports success for `target`, accepting a pending lower-tier recovery probe.
    pub fn report_success(&self, target: &Url) {
        let ProxyInner::Tiered(state) = &self.inner else {
            return;
        };
        let key = target.host_str().unwrap_or_default().to_owned();
        let mut domains = state.domains.lock().unwrap_or_else(|e| e.into_inner());
        let domain = domains.entry(key).or_default();
        if let Some(probe) = domain.probing.take() {
            domain.tier = probe;
            domain.requests = 0;
        }
    }
}

impl fmt::Debug for ProxyConfiguration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let variant = match self.inner {
            ProxyInner::Static { .. } => "Static",
            ProxyInner::Custom(_) => "Custom",
            ProxyInner::Tiered(_) => "Tiered",
        };
        formatter
            .debug_struct("ProxyConfiguration")
            .field("variant", &variant)
            .finish()
    }
}

/// Synchronous policy selecting a proxy bucket for a request.
///
/// ```
/// # use millipede_core::proxy::{ProxyKind, ProxyRouteContext, ProxyStrategy};
/// # struct DefaultRoute;
/// # impl ProxyStrategy for DefaultRoute { fn route(&self, _: &ProxyRouteContext<'_>) -> ProxyKind { ProxyKind::Default } }
/// ```
pub trait ProxyStrategy: Send + Sync + 'static {
    /// Selects the logical proxy bucket.
    fn route(&self, ctx: &ProxyRouteContext<'_>) -> ProxyKind;
}

/// Borrowed inputs available to a [`ProxyStrategy`].
///
/// ```
/// # use millipede_core::{proxy::ProxyRouteContext, request::Request};
/// # let request = Request::get("https://example.com").build()?;
/// let context = ProxyRouteContext::new(&request, 0).previous_profile_key("old");
/// assert_eq!(context.previous_profile_key, Some("old"));
/// # Ok::<(), millipede_core::request::RequestBuildError>(())
/// ```
pub struct ProxyRouteContext<'a> {
    /// Request being routed.
    pub request: &'a Request,
    /// Zero-based attempt.
    pub attempt: u32,
    /// Profile selected on the previous attempt, when any.
    pub previous_profile_key: Option<&'a str>,
}

impl<'a> ProxyRouteContext<'a> {
    /// Creates a route context.
    pub fn new(request: &'a Request, attempt: u32) -> Self {
        Self {
            request,
            attempt,
            previous_profile_key: None,
        }
    }
    /// Sets the prior profile key.
    pub fn previous_profile_key(mut self, value: &'a str) -> Self {
        self.previous_profile_key = Some(value);
        self
    }
}

/// Logical proxy configuration bucket.
///
/// ```
/// use millipede_core::proxy::ProxyKind;
/// assert_eq!(ProxyKind::default(), ProxyKind::Default);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum ProxyKind {
    /// General-purpose proxy configuration.
    #[default]
    Default,
    /// Proxy configuration specialized for media assets.
    MediaAsset,
    /// User-named proxy configuration.
    Custom(String),
}

/// Named proxy configurations with deterministic fallbacks.
///
/// ```
/// use millipede_core::proxy::{ProxyBuckets, ProxyKind};
/// assert!(ProxyBuckets::new().for_kind(&ProxyKind::Default).is_none());
/// ```
#[derive(Default)]
#[must_use = "proxy buckets do nothing unless installed on a crawler"]
pub struct ProxyBuckets {
    default_bucket: Option<ProxyConfiguration>,
    media: Option<ProxyConfiguration>,
    custom: HashMap<String, ProxyConfiguration>,
}

impl ProxyBuckets {
    /// Creates empty buckets.
    pub fn new() -> Self {
        Self::default()
    }
    /// Sets the default bucket.
    pub fn with_default(mut self, value: ProxyConfiguration) -> Self {
        self.default_bucket = Some(value);
        self
    }
    /// Sets the media bucket.
    pub fn with_media(mut self, value: ProxyConfiguration) -> Self {
        self.media = Some(value);
        self
    }
    /// Inserts or replaces a named custom bucket.
    pub fn with_custom(mut self, name: impl Into<String>, value: ProxyConfiguration) -> Self {
        self.custom.insert(name.into(), value);
        self
    }
    /// Finds a bucket, falling media and unknown custom names back to the default.
    pub fn for_kind(&self, kind: &ProxyKind) -> Option<&ProxyConfiguration> {
        match kind {
            ProxyKind::Default => self.default_bucket.as_ref(),
            ProxyKind::MediaAsset => self.media.as_ref().or(self.default_bucket.as_ref()),
            ProxyKind::Custom(name) => self.custom.get(name).or(self.default_bucket.as_ref()),
        }
    }
}

impl fmt::Debug for ProxyBuckets {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProxyBuckets")
            .field("default_bucket", &self.default_bucket)
            .field("media", &self.media)
            .field("custom", &self.custom)
            .finish()
    }
}
