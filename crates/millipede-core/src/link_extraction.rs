//! Scraper-independent link extraction types, URL matching, and crawl policy.
//!
//! This module contains the pure-logic half of link extraction. DOM-specific
//! implementations can provide [`LinkExtractor`](crate::link_extraction::LinkExtractor), while enqueueing code can apply
//! the strategies, patterns, overrides, and policy defined here.
//!
//! `RobotsPolicy` from `INTERFACE.md` section 7.1 is deliberately deferred to a
//! later phase: Phase 5 locks no robots dependency, and the Phase 5 roadmap scope
//! does not include robots handling. [`CrawlPolicy`](crate::link_extraction::CrawlPolicy) is `#[non_exhaustive]` so a
//! robots policy can be added without breaking downstream construction patterns.

use crate::{
    enqueue::SkipReason,
    errors::CrawlError,
    request::{HeaderMap, Method, UserData},
};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use regex::Regex;
use std::{fmt, sync::Arc};
use url::{Host, Url};

/// Controls how closely a discovered URL must relate to its parent URL.
///
/// The strategy applies to extractor and raw-link candidates. Explicit [`url::Url`] values passed
/// through `EnqueueLinksOptions::urls` are caller-selected inputs and bypass relationship
/// filtering.
///
/// # Examples
///
/// ```
/// use millipede_core::link_extraction::EnqueueStrategy;
///
/// assert_eq!(EnqueueStrategy::default(), EnqueueStrategy::SameHostname);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EnqueueStrategy {
    /// Allows every HTTP or HTTPS URL.
    All,
    /// Allows URLs whose hostname exactly matches the parent hostname.
    #[default]
    SameHostname,
    /// Allows URLs with the same registrable domain as the parent.
    SameDomain,
    /// Allows URLs with the same scheme, hostname, and effective port as the parent.
    SameOrigin,
}

/// Returns whether `candidate` is allowed by `strategy` relative to `parent`.
///
/// Candidate URLs using schemes other than HTTP and HTTPS are always rejected,
/// including under [`EnqueueStrategy::All`].
///
/// # Examples
///
/// ```
/// use millipede_core::link_extraction::{strategy_allows, EnqueueStrategy};
/// use url::Url;
///
/// let parent = Url::parse("http://example.com/catalog").unwrap();
/// let secure = Url::parse("https://example.com/product").unwrap();
/// assert!(strategy_allows(
///     EnqueueStrategy::SameHostname,
///     &parent,
///     &secure,
/// ));
/// assert!(!strategy_allows(
///     EnqueueStrategy::SameOrigin,
///     &parent,
///     &secure,
/// ));
/// ```
pub fn strategy_allows(strategy: EnqueueStrategy, parent: &Url, candidate: &Url) -> bool {
    if !matches!(candidate.scheme(), "http" | "https") {
        return false;
    }

    match strategy {
        EnqueueStrategy::All => true,
        EnqueueStrategy::SameHostname => hosts_equal(parent.host_str(), candidate.host_str()),
        EnqueueStrategy::SameDomain => same_domain(parent, candidate),
        EnqueueStrategy::SameOrigin => {
            parent.scheme() == candidate.scheme()
                && hosts_equal(parent.host_str(), candidate.host_str())
                && parent.port_or_known_default() == candidate.port_or_known_default()
        }
    }
}

fn hosts_equal(left: Option<&str>, right: Option<&str>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.eq_ignore_ascii_case(right),
        _ => false,
    }
}

fn same_domain(parent: &Url, candidate: &Url) -> bool {
    let (Some(parent_host), Some(candidate_host)) = (parent.host(), candidate.host()) else {
        return false;
    };

    match (parent_host, candidate_host) {
        (Host::Domain(parent_host), Host::Domain(candidate_host)) => {
            match (
                psl::domain_str(parent_host),
                psl::domain_str(candidate_host),
            ) {
                (Some(parent_domain), Some(candidate_domain)) => {
                    parent_domain.eq_ignore_ascii_case(candidate_domain)
                }
                _ => parent_host.eq_ignore_ascii_case(candidate_host),
            }
        }
        (parent_host, candidate_host) => parent_host == candidate_host,
    }
}

/// A URL include or exclude pattern.
///
/// String conversions create [`UrlPattern::Glob`] values, while compiled regular
/// expressions can be converted without recompilation.
///
/// # Examples
///
/// ```
/// use millipede_core::link_extraction::UrlPattern;
///
/// let pattern = UrlPattern::from("**/products/*");
/// assert!(matches!(pattern, UrlPattern::Glob(_)));
/// ```
#[derive(Debug, Clone)]
pub enum UrlPattern {
    /// A minimatch-style glob matched against the complete URL string.
    Glob(String),
    /// A regular expression matched against the complete URL string.
    Regex(Regex),
}

impl From<&str> for UrlPattern {
    fn from(pattern: &str) -> Self {
        Self::Glob(pattern.to_owned())
    }
}

impl From<String> for UrlPattern {
    fn from(pattern: String) -> Self {
        Self::Glob(pattern)
    }
}

impl From<Regex> for UrlPattern {
    fn from(pattern: Regex) -> Self {
        Self::Regex(pattern)
    }
}

/// Compiles URL globs with separators treated as ordinary characters.
#[allow(dead_code)] // Consumed by the later Phase 5 enqueue-pipeline commit.
pub(crate) fn compile_globs(patterns: &[String]) -> Result<GlobSet, LinkPatternError> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob = GlobBuilder::new(pattern)
            .literal_separator(false)
            .build()
            .map_err(|source| LinkPatternError::InvalidGlob {
                pattern: pattern.clone(),
                source,
            })?;
        builder.add(glob);
    }

    builder
        .build()
        .map_err(|source| LinkPatternError::InvalidGlob {
            pattern: patterns.last().cloned().unwrap_or_default(),
            source,
        })
}

/// An error produced while compiling link patterns.
#[derive(Debug, thiserror::Error)]
pub enum LinkPatternError {
    /// A glob cannot be parsed or compiled.
    #[error("invalid glob pattern {pattern:?}: {source}")]
    InvalidGlob {
        /// The invalid source pattern.
        pattern: String,
        /// The glob parser or compiler error.
        #[source]
        source: globset::Error,
    },
}

/// A URL pattern with request fields to apply when it matches.
///
/// # Examples
///
/// ```
/// use millipede_core::{
///     link_extraction::UrlMatch,
///     request::{HeaderMap, Method, UserData},
/// };
///
/// let matched = UrlMatch::new("**/products/*")
///     .label("product")
///     .user_data(UserData::default())
///     .method(Method::POST)
///     .headers(HeaderMap::new());
/// assert_eq!(matched.label.as_deref(), Some("product"));
/// ```
#[derive(Debug, Clone)]
pub struct UrlMatch {
    /// The URL pattern to match.
    pub pattern: UrlPattern,
    /// An optional route label applied to matching requests.
    pub label: Option<String>,
    /// Optional user data applied to matching requests.
    pub user_data: Option<UserData>,
    /// An optional HTTP method applied to matching requests.
    pub method: Option<Method>,
    /// Optional HTTP headers applied to matching requests.
    pub headers: Option<HeaderMap>,
}

impl UrlMatch {
    /// Creates a pattern with no request-field overrides.
    pub fn new(pattern: impl Into<UrlPattern>) -> Self {
        Self {
            pattern: pattern.into(),
            label: None,
            user_data: None,
            method: None,
            headers: None,
        }
    }

    /// Sets the route label for matching requests.
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Sets the user data for matching requests.
    pub fn user_data(mut self, user_data: UserData) -> Self {
        self.user_data = Some(user_data);
        self
    }

    /// Sets the HTTP method for matching requests.
    pub fn method(mut self, method: Method) -> Self {
        self.method = Some(method);
        self
    }

    /// Sets the HTTP headers for matching requests.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = Some(headers);
        self
    }
}

/// An include glob or regular expression with optional per-pattern overrides.
///
/// Use [`UrlMatch`] when a matching link should override request fields.
///
/// # Examples
///
/// ```
/// use millipede_core::link_extraction::{GlobPattern, UrlMatch};
/// use regex::Regex;
///
/// let plain = GlobPattern::from("https://example.com/**");
/// let regex = GlobPattern::from(Regex::new(r"/items/\\d+$").unwrap());
/// let labeled = GlobPattern::from(UrlMatch::new("**/items/*").label("item"));
/// # let _ = (plain, regex, labeled);
/// ```
#[derive(Debug, Clone)]
pub struct GlobPattern {
    matched: UrlMatch,
}

impl From<&str> for GlobPattern {
    fn from(pattern: &str) -> Self {
        Self {
            matched: UrlMatch::new(pattern),
        }
    }
}

impl From<String> for GlobPattern {
    fn from(pattern: String) -> Self {
        Self {
            matched: UrlMatch::new(pattern),
        }
    }
}

/// Converts a compiled regular expression into a pattern with no request-field overrides.
impl From<Regex> for GlobPattern {
    fn from(pattern: Regex) -> Self {
        Self {
            matched: UrlMatch::new(pattern),
        }
    }
}

impl From<UrlMatch> for GlobPattern {
    fn from(matched: UrlMatch) -> Self {
        Self { matched }
    }
}

// These accessors form the handoff to the later enqueue-pipeline commit.
#[allow(dead_code)]
impl GlobPattern {
    /// Returns the wrapped URL pattern.
    pub(crate) fn pattern(&self) -> &UrlPattern {
        &self.matched.pattern
    }

    /// Returns the optional routing-label override.
    pub(crate) fn label(&self) -> Option<&str> {
        self.matched.label.as_deref()
    }

    /// Returns the optional user-data override.
    pub(crate) fn user_data(&self) -> Option<&UserData> {
        self.matched.user_data.as_ref()
    }

    /// Returns the optional HTTP-method override.
    pub(crate) fn method(&self) -> Option<&Method> {
        self.matched.method.as_ref()
    }

    /// Returns the optional HTTP-headers override.
    pub(crate) fn headers(&self) -> Option<&HeaderMap> {
        self.matched.headers.as_ref()
    }
}

/// A raw extracted link and the optional document base used to resolve it.
///
/// Resolution is intentionally deferred until enqueue time, preserving invalid
/// raw values for skip reporting.
///
/// # Examples
///
/// ```
/// use millipede_core::link_extraction::ExtractedLink;
/// use url::Url;
///
/// let link = ExtractedLink {
///     url: "../about".to_owned(),
///     base: Some(Url::parse("https://example.com/docs/").unwrap()),
/// };
/// assert_eq!(link.url, "../about");
/// ```
#[derive(Debug, Clone)]
pub struct ExtractedLink {
    /// The unmodified link text, such as an element's `href` value.
    pub url: String,
    /// A per-document base URL, typically sourced from `<base href>`.
    pub base: Option<Url>,
}

/// Extracts links from a static document or live browser page.
///
/// Passing `None` selects the implementation's default selector, normally
/// `a[href]`. Extraction is asynchronous because a DOM-level browser extractor
/// evaluates JavaScript against a live page over CDP (`millipede-browser`, Phase
/// 6), while static-document extractors such as `millipede-html` simply have no
/// await points. The trait remains object-safe so crawler implementations can
/// erase their extractor.
#[async_trait::async_trait]
pub trait LinkExtractor: Send + Sync {
    /// Extracts raw links selected by `selector` or the implementation default.
    async fn extract(&self, selector: Option<&str>) -> Result<Vec<ExtractedLink>, CrawlError>;
}

/// The outcome of transforming a candidate request before enqueueing.
#[derive(Debug)]
pub enum TransformResult {
    /// Enqueue the candidate, including any mutations made by the transform.
    Enqueue,
    /// Reject the candidate and report the supplied reason.
    Skip {
        /// A user-facing explanation for rejecting the candidate.
        reason: String,
    },
}

/// Receives notifications for URL candidates skipped during enqueueing.
///
/// This intentionally differs from `INTERFACE.md` section 7.1: skips often happen
/// before a [`crate::request::Request`] can be built (for example, a glob-excluded
/// raw URL), so the hook receives the URL string instead of an owned request. It is
/// synchronous to avoid allocating and boxing a future for every skip.
pub trait SkippedHandler: Send + Sync + 'static {
    /// Handles one skipped URL and its reason.
    fn on_skip(&self, url: &str, reason: &SkipReason);
}

impl<F> SkippedHandler for F
where
    F: Fn(&str, &SkipReason) + Send + Sync + 'static,
{
    fn on_skip(&self, url: &str, reason: &SkipReason) {
        self(url, reason);
    }
}

/// Long-lived limits and URL admission policy applied during a crawl.
///
/// The type is non-exhaustive so later phases can add robots handling without
/// breaking callers.
///
/// # Examples
///
/// ```
/// use millipede_core::link_extraction::{CrawlPolicy, EnqueueStrategy};
///
/// let policy = CrawlPolicy::new()
///     .strategy(EnqueueStrategy::SameDomain)
///     .max_crawl_depth(4)
///     .max_requests_per_crawl(10_000);
/// assert_eq!(policy.max_crawl_depth, Some(4));
/// ```
#[non_exhaustive]
#[derive(Default)]
pub struct CrawlPolicy {
    /// The default relationship required between parent and candidate URLs.
    pub strategy: EnqueueStrategy,
    /// The maximum child crawl depth, or `None` for no depth limit.
    pub max_crawl_depth: Option<u32>,
    /// The maximum number of requests accepted in one crawl, or `None` for no limit.
    pub max_requests_per_crawl: Option<u64>,
    /// An optional callback invoked for every skipped URL.
    pub on_skipped: Option<Arc<dyn SkippedHandler>>,
}

impl CrawlPolicy {
    /// Creates the default crawl policy.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the default URL admission strategy.
    pub fn strategy(mut self, strategy: EnqueueStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Sets the maximum crawl depth.
    pub fn max_crawl_depth(mut self, max_crawl_depth: u32) -> Self {
        self.max_crawl_depth = Some(max_crawl_depth);
        self
    }

    /// Sets the maximum number of requests accepted during the crawl.
    pub fn max_requests_per_crawl(mut self, max_requests_per_crawl: u64) -> Self {
        self.max_requests_per_crawl = Some(max_requests_per_crawl);
        self
    }

    /// Sets the skipped-URL callback.
    pub fn on_skipped<H: SkippedHandler>(mut self, handler: H) -> Self {
        self.on_skipped = Some(Arc::new(handler));
        self
    }
}

impl fmt::Debug for CrawlPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CrawlPolicy")
            .field("strategy", &self.strategy)
            .field("max_crawl_depth", &self.max_crawl_depth)
            .field("max_requests_per_crawl", &self.max_requests_per_crawl)
            .field(
                "on_skipped",
                &self.on_skipped.as_ref().map(|_| "<dyn SkippedHandler>"),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiled_globs_match_complete_url_strings() {
        let patterns = vec![
            "**/products/*".to_owned(),
            "https://example.com/**".to_owned(),
        ];
        let compiled = compile_globs(&patterns).expect("valid globs");

        assert!(compiled.is_match("https://shop.test/products/p1"));
        assert!(compiled.is_match("https://example.com/anything/here"));
        assert!(!compiled.is_match("https://shop.test/categories/c1"));
    }

    #[test]
    fn invalid_glob_retains_source_pattern() {
        let error = compile_globs(&["[".to_owned()]).expect_err("glob should be invalid");
        assert!(matches!(
            error,
            LinkPatternError::InvalidGlob { ref pattern, .. } if pattern == "["
        ));
    }

    #[test]
    fn glob_pattern_accessors_expose_overrides() {
        let mut user_data = UserData::default();
        user_data
            .set_typed("kind", &"product")
            .expect("serializable data");
        let mut headers = HeaderMap::new();
        headers.insert("x-test", "yes".parse().expect("valid header value"));
        let pattern = GlobPattern::from(
            UrlMatch::new(Regex::new("products").expect("valid regex"))
                .label("product")
                .user_data(user_data)
                .method(Method::POST)
                .headers(headers),
        );

        assert!(matches!(pattern.pattern(), UrlPattern::Regex(_)));
        assert_eq!(pattern.label(), Some("product"));
        assert_eq!(
            pattern.user_data().and_then(|data| data.get("kind")),
            Some(&serde_json::json!("product"))
        );
        assert_eq!(pattern.method(), Some(&Method::POST));
        assert_eq!(
            pattern.headers().and_then(|map| map.get("x-test")),
            Some(&"yes".parse().expect("valid header value"))
        );
    }
}
