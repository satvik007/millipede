//! Link extraction, filtering, transformation, and enqueueing from handler contexts.

use crate::{
    crawler::{CrawlerHandle, EnqueueAdmissionReservation},
    errors::CrawlError,
    link_extraction::{
        CrawlPolicy, EnqueueStrategy, ExtractedLink, GlobPattern, LinkExtractor, TransformResult,
        UrlPattern, compile_globs, strategy_allows,
    },
    request::{Request, RequestId, UserData},
    storage::{AddOptions, ProcessedRequest},
};
use futures_util::future::BoxFuture;
use globset::GlobSet;
use regex::Regex;
use std::{collections::HashSet, fmt, sync::Arc};
use url::Url;

type Transform = dyn for<'r> Fn(&'r mut Request) -> BoxFuture<'r, TransformResult> + Send + Sync;

/// Enqueues child URLs through a running crawler.
///
/// # Examples
///
/// ```no_run
/// use millipede_core::prelude::*;
/// use url::Url;
///
/// # async fn enqueue_children(ctx: BasicContext) -> Result<(), Box<dyn std::error::Error>> {
/// let enqueue = EnqueueLinker::new(ctx.crawler.clone(), &ctx.request);
/// enqueue
///     .urls([Url::parse("https://example.com/child")?])
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct EnqueueLinker {
    crawler: CrawlerHandle,
    parent_url: Url,
    parent_depth: u32,
    extractor: Option<Arc<dyn LinkExtractor>>,
}

impl EnqueueLinker {
    /// Creates a linker from the current crawler handle and parent request.
    pub fn new(crawler: CrawlerHandle, parent: &Request) -> Self {
        Self {
            crawler,
            parent_url: parent.url.clone(),
            parent_depth: parent.crawl_depth,
            extractor: None,
        }
    }

    /// Creates a linker with an HTML-aware link extractor.
    pub fn with_extractor(
        crawler: CrawlerHandle,
        parent: &Request,
        extractor: Arc<dyn LinkExtractor>,
    ) -> Self {
        Self {
            crawler,
            parent_url: parent.url.clone(),
            parent_depth: parent.crawl_depth,
            extractor: Some(extractor),
        }
    }

    /// Starts configuring an enqueue operation.
    pub fn options(&self) -> EnqueueLinksOptions<'_> {
        EnqueueLinksOptions::new(self)
    }

    /// Enqueues explicit absolute URLs using default options.
    ///
    /// Explicit URLs bypass [`EnqueueStrategy`] relationship filtering. Strategies constrain DOM
    /// discovery; callers that already selected concrete URLs retain the URLs-only behavior that
    /// predates extractor support.
    pub async fn urls(
        &self,
        urls: impl IntoIterator<Item = Url>,
    ) -> Result<EnqueueResult, CrawlError> {
        self.options().urls(urls).send().await
    }

    /// Extracts links with the default selector and allows every HTTP(S) URL.
    pub async fn all(&self) -> Result<EnqueueResult, CrawlError> {
        self.options().strategy(EnqueueStrategy::All).send().await
    }

    /// Extracts links with the default selector and keeps the parent's origin.
    pub async fn same_origin(&self) -> Result<EnqueueResult, CrawlError> {
        self.options()
            .strategy(EnqueueStrategy::SameOrigin)
            .send()
            .await
    }

    /// Extracts links with the default selector and keeps the parent's hostname.
    pub async fn same_hostname(&self) -> Result<EnqueueResult, CrawlError> {
        self.options()
            .strategy(EnqueueStrategy::SameHostname)
            .send()
            .await
    }

    /// Extracts links with the default selector and keeps the parent's registrable domain.
    pub async fn same_domain(&self) -> Result<EnqueueResult, CrawlError> {
        self.options()
            .strategy(EnqueueStrategy::SameDomain)
            .send()
            .await
    }
}

impl fmt::Debug for EnqueueLinker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnqueueLinker")
            .field("crawler", &self.crawler)
            .field("parent_url", &self.parent_url)
            .field("parent_depth", &self.parent_depth)
            .field(
                "extractor",
                &self.extractor.as_ref().map(|_| "<dyn LinkExtractor>"),
            )
            .finish()
    }
}

/// Fluent options for one URLs-only enqueue operation.
///
/// # Examples
///
/// ```no_run
/// use millipede_core::prelude::*;
/// use url::Url;
///
/// # async fn enqueue_children(ctx: BasicContext) -> Result<(), Box<dyn std::error::Error>> {
/// EnqueueLinker::new(ctx.crawler.clone(), &ctx.request)
///     .options()
///     .raw_urls(["child", "/about"])
///     .base_url(Url::parse("https://example.com/docs/")?)
///     .label("detail")
///     .limit(10)
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct EnqueueLinksOptions<'a> {
    linker: &'a EnqueueLinker,
    candidates: Vec<UrlCandidate>,
    base_url: Option<Url>,
    label: Option<String>,
    user_data: Option<UserData>,
    selector: Option<String>,
    strategy: Option<EnqueueStrategy>,
    globs: Vec<GlobPattern>,
    regex: Vec<Regex>,
    exclude: Vec<UrlPattern>,
    transform: Option<Arc<Transform>>,
    limit: Option<usize>,
    forefront: bool,
}

enum UrlCandidate {
    Absolute(Url),
    Raw { url: String, base: Option<Url> },
}

impl<'a> EnqueueLinksOptions<'a> {
    fn new(linker: &'a EnqueueLinker) -> Self {
        Self {
            linker,
            candidates: Vec::new(),
            base_url: None,
            label: None,
            user_data: None,
            selector: None,
            strategy: None,
            globs: Vec::new(),
            regex: Vec::new(),
            exclude: Vec::new(),
            transform: None,
            limit: None,
            forefront: false,
        }
    }

    /// Extends the explicit absolute URL candidates.
    ///
    /// These candidates bypass enqueue-strategy relationship filtering, including the default
    /// [`EnqueueStrategy::SameHostname`]. Other filters and crawl-policy limits still apply.
    pub fn urls(mut self, urls: impl IntoIterator<Item = Url>) -> Self {
        self.candidates
            .extend(urls.into_iter().map(UrlCandidate::Absolute));
        self
    }
    /// Extends raw absolute or relative URL candidates.
    pub fn raw_urls<S: Into<String>>(mut self, urls: impl IntoIterator<Item = S>) -> Self {
        self.candidates
            .extend(urls.into_iter().map(|url| UrlCandidate::Raw {
                url: url.into(),
                base: None,
            }));
        self
    }
    /// Overrides the base used to resolve raw relative URLs.
    pub fn base_url(mut self, base_url: Url) -> Self {
        self.base_url = Some(base_url);
        self
    }
    /// Applies a label to every child. Children do not inherit the parent label; without this
    /// option they remain unlabeled and use the router's default route.
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }
    /// Applies user data to every child.
    pub fn user_data(mut self, user_data: UserData) -> Self {
        self.user_data = Some(user_data);
        self
    }
    /// Selects links from the associated HTML context with a CSS selector.
    pub fn selector(mut self, selector: impl Into<String>) -> Self {
        self.selector = Some(selector.into());
        self
    }
    /// Overrides the crawler policy's URL relationship strategy.
    pub fn strategy(mut self, strategy: EnqueueStrategy) -> Self {
        self.strategy = Some(strategy);
        self
    }
    /// Adds URL include patterns. A candidate may match any glob or regex include.
    pub fn globs<G: Into<GlobPattern>>(mut self, globs: impl IntoIterator<Item = G>) -> Self {
        self.globs.extend(globs.into_iter().map(Into::into));
        self
    }
    /// Adds regular-expression URL includes.
    pub fn regex(mut self, patterns: impl IntoIterator<Item = Regex>) -> Self {
        self.regex.extend(patterns);
        self
    }
    /// Adds URL exclusions. Exclusions take precedence over includes.
    pub fn exclude<P: Into<UrlPattern>>(mut self, patterns: impl IntoIterator<Item = P>) -> Self {
        self.exclude.extend(patterns.into_iter().map(Into::into));
        self
    }
    /// Installs an asynchronous request transform that may mutate or reject each candidate.
    pub fn transform<F>(mut self, transform: F) -> Self
    where
        F: for<'r> Fn(&'r mut Request) -> BoxFuture<'r, TransformResult> + Send + Sync + 'static,
    {
        self.transform = Some(Arc::new(transform));
        self
    }
    /// Caps the number of candidates after URL deduplication. Truncated candidates are silently
    /// omitted, and queue duplicates within the retained candidates consume the cap.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
    /// Chooses whether children are inserted at the queue front.
    pub fn forefront(mut self, forefront: bool) -> Self {
        self.forefront = forefront;
        self
    }

    /// Resolves, builds, and enqueues candidates through one admission-controlled operation.
    pub async fn send(mut self) -> Result<EnqueueResult, CrawlError> {
        let should_extract = self.selector.is_some()
            || (self.candidates.is_empty() && self.linker.extractor.is_some());
        if should_extract {
            let extractor = self.linker.extractor.as_ref().ok_or_else(|| {
                CrawlError::non_retryable(anyhow::anyhow!(
                    "selector-based enqueue requires an HTML or browser context; use urls()/raw_urls() on HTTP contexts"
                ))
            })?;
            let extracted = extractor.extract(self.selector.as_deref()).await?;
            self.candidates.extend(
                extracted
                    .into_iter()
                    .map(|ExtractedLink { url, base }| UrlCandidate::Raw { url, base }),
            );
        }

        let policy = self
            .linker
            .crawler
            .crawl_policy()
            .unwrap_or_else(|| Arc::new(CrawlPolicy::default()));
        let strategy = self.strategy.unwrap_or(policy.strategy);
        let compiled_globs = compile_include_patterns(self.globs)?;
        let compiled_excludes = compile_patterns(self.exclude)?;
        let has_globs = !compiled_globs.is_empty();
        let has_regex = !self.regex.is_empty();
        let mut skipped = Vec::new();
        let mut candidates = Vec::with_capacity(self.candidates.len());
        for candidate in self.candidates {
            let (resolved, apply_strategy) = match candidate {
                UrlCandidate::Absolute(url) => (Ok(url), false),
                UrlCandidate::Raw { url, base } => {
                    let base = base
                        .as_ref()
                        .or(self.base_url.as_ref())
                        .unwrap_or(&self.linker.parent_url);
                    (resolve_raw_url(base, &url).map_err(|_| url), true)
                }
            };
            let url = match resolved {
                Ok(candidate) => candidate,
                Err(raw) => {
                    report_skip(&policy, &mut skipped, raw, SkipReason::InvalidUrl);
                    continue;
                }
            };
            let url_text = url.to_string();
            if apply_strategy && !strategy_allows(strategy, &self.linker.parent_url, &url) {
                report_skip(
                    &policy,
                    &mut skipped,
                    url_text,
                    SkipReason::StrategyExcluded,
                );
                continue;
            }
            if let Some(reason) = first_exclusion(&compiled_excludes, &url_text) {
                report_skip(&policy, &mut skipped, url_text, reason);
                continue;
            }
            let include_override = compiled_globs
                .iter()
                .find(|pattern| pattern.matches(&url_text))
                .map(|pattern| pattern.overrides());
            if has_globs || has_regex {
                let regex_matches = self.regex.iter().any(|pattern| pattern.is_match(&url_text));
                if include_override.is_none() && !regex_matches {
                    let reason = if has_globs {
                        SkipReason::GlobExcluded
                    } else {
                        SkipReason::RegexExcluded
                    };
                    report_skip(&policy, &mut skipped, url_text, reason);
                    continue;
                }
            }
            candidates.push((url, include_override.unwrap_or_default()));
        }

        let mut seen = HashSet::new();
        candidates.retain(|(url, _)| seen.insert(url.to_string()));
        if let Some(limit) = self.limit {
            candidates.truncate(limit);
        }

        let mut requests = Vec::with_capacity(candidates.len());
        let mut admission_reservations = Vec::new();
        let child_depth = self.linker.parent_depth.saturating_add(1);
        for (url, overrides) in candidates {
            let url_text = url.to_string();
            let mut builder = Request::builder().url(url).crawl_depth(child_depth);
            if let Some(label) = overrides.label.as_deref().or(self.label.as_deref()) {
                builder = builder.label(label.to_owned());
            }
            if let Some(user_data) = overrides.user_data.as_ref().or(self.user_data.as_ref()) {
                builder = builder.user_data(user_data.clone());
            }
            if let Some(method) = overrides.method {
                builder = builder.method(method);
            }
            if let Some(headers) = overrides.headers {
                builder = builder.headers(headers);
            }
            let mut request = match builder.build() {
                Ok(request) => request,
                Err(_) => {
                    report_skip(&policy, &mut skipped, url_text, SkipReason::InvalidUrl);
                    continue;
                }
            };
            if let Some(limit) = policy.max_crawl_depth {
                if child_depth > limit {
                    report_skip(
                        &policy,
                        &mut skipped,
                        url_text,
                        SkipReason::MaxDepthExceeded {
                            depth: child_depth,
                            limit,
                        },
                    );
                    continue;
                }
            }
            let admission_reservation = if let Some(limit) = policy.max_requests_per_crawl {
                match reserve_request_slot(&self.linker.crawler, limit).await? {
                    Admission::Reserved(reservation) => Some(reservation),
                    Admission::Untracked => None,
                    Admission::Rejected => {
                        report_skip(
                            &policy,
                            &mut skipped,
                            url_text,
                            SkipReason::MaxRequestsReached { limit },
                        );
                        continue;
                    }
                }
            } else {
                None
            };
            let pre_transform_unique_key = request.unique_key.clone();
            if let Some(transform) = &self.transform {
                match transform(&mut request).await {
                    TransformResult::Enqueue => {}
                    TransformResult::Skip { reason } => {
                        report_skip(
                            &policy,
                            &mut skipped,
                            url_text,
                            SkipReason::TransformRejected { reason },
                        );
                        continue;
                    }
                }
            }
            if request.unique_key == pre_transform_unique_key {
                request.unique_key = Request::compute_unique_key(
                    &request.url,
                    &request.method,
                    request.body.as_ref(),
                );
                request.id = RequestId::from_unique_key(&request.unique_key);
            }
            requests.push(request);
            admission_reservations.push(admission_reservation);
        }

        let request_urls: Vec<_> = requests
            .iter()
            .map(|request| request.url.to_string())
            .collect();
        let batch = self
            .linker
            .crawler
            .add_requests_with_options(
                requests,
                AddOptions {
                    forefront: self.forefront,
                },
            )
            .await?;
        let batch = batch.wait().await?;
        let mut added = Vec::new();
        let mut admission_reservations = admission_reservations.into_iter();
        for (index, request) in batch.processed.into_iter().enumerate() {
            let admission_reservation = admission_reservations.next().flatten();
            if request.was_already_present || request.was_already_handled {
                let url = request_urls
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| request.unique_key.clone());
                report_skip(&policy, &mut skipped, url, SkipReason::DuplicateUniqueKey);
            } else {
                if let Some(reservation) = admission_reservation {
                    reservation.commit();
                }
                added.push(request);
            }
        }
        Ok(EnqueueResult { added, skipped })
    }
}

enum Admission {
    Reserved(EnqueueAdmissionReservation),
    Untracked,
    Rejected,
}

async fn reserve_request_slot(
    crawler: &CrawlerHandle,
    limit: u64,
) -> Result<Admission, CrawlError> {
    let _admission = crawler.lock_enqueue_admission().await?;
    let queue = match crawler.request_queue() {
        Some(queue) => queue,
        None => return Ok(Admission::Untracked),
    };
    let Some(queue_count) = request_count(queue.as_ref()).await else {
        return Ok(Admission::Untracked);
    };
    let admitted = crawler.synchronize_enqueue_admissions(queue_count)?;
    if admitted >= limit {
        return Ok(Admission::Rejected);
    }
    Ok(Admission::Reserved(crawler.reserve_enqueue_admission()?))
}

async fn request_count(queue: &dyn crate::storage::RequestQueue) -> Option<u64> {
    let handled = match queue.handled_count().await {
        Ok(count) => count,
        Err(error) => {
            tracing::debug!(%error, "could not read handled request count; skipping max-request admission check");
            return None;
        }
    };
    let pending = match queue.pending_count().await {
        Ok(count) => count,
        Err(error) => {
            tracing::debug!(%error, "could not read pending request count; skipping max-request admission check");
            return None;
        }
    };
    Some(handled.saturating_add(pending))
}

#[derive(Default)]
struct PatternOverrides {
    label: Option<String>,
    user_data: Option<UserData>,
    method: Option<crate::request::Method>,
    headers: Option<crate::request::HeaderMap>,
}

enum CompiledPattern {
    Glob(GlobSet),
    Regex(Regex),
}

impl CompiledPattern {
    fn matches(&self, url: &str) -> bool {
        match self {
            Self::Glob(pattern) => pattern.is_match(url),
            Self::Regex(pattern) => pattern.is_match(url),
        }
    }
}

struct CompiledInclude {
    pattern: CompiledPattern,
    source: GlobPattern,
}

impl CompiledInclude {
    fn matches(&self, url: &str) -> bool {
        self.pattern.matches(url)
    }

    fn overrides(&self) -> PatternOverrides {
        PatternOverrides {
            label: self.source.label().map(str::to_owned),
            user_data: self.source.user_data().cloned(),
            method: self.source.method().cloned(),
            headers: self.source.headers().cloned(),
        }
    }
}

fn compile_include_patterns(
    patterns: Vec<GlobPattern>,
) -> Result<Vec<CompiledInclude>, CrawlError> {
    patterns
        .into_iter()
        .map(|source| {
            let pattern = compile_pattern(source.pattern().clone())?;
            Ok(CompiledInclude { pattern, source })
        })
        .collect()
}

fn compile_patterns(patterns: Vec<UrlPattern>) -> Result<Vec<CompiledPattern>, CrawlError> {
    patterns.into_iter().map(compile_pattern).collect()
}

fn compile_pattern(pattern: UrlPattern) -> Result<CompiledPattern, CrawlError> {
    match pattern {
        UrlPattern::Glob(pattern) => compile_globs(std::slice::from_ref(&pattern))
            .map(CompiledPattern::Glob)
            .map_err(CrawlError::non_retryable),
        UrlPattern::Regex(pattern) => Ok(CompiledPattern::Regex(pattern)),
    }
}

fn first_exclusion(patterns: &[CompiledPattern], url: &str) -> Option<SkipReason> {
    patterns.iter().find_map(|pattern| {
        if !pattern.matches(url) {
            return None;
        }
        Some(match pattern {
            CompiledPattern::Glob(_) => SkipReason::GlobExcluded,
            CompiledPattern::Regex(_) => SkipReason::RegexExcluded,
        })
    })
}

fn report_skip(
    policy: &CrawlPolicy,
    skipped: &mut Vec<SkippedUrl>,
    url: String,
    reason: SkipReason,
) {
    if let Some(handler) = &policy.on_skipped {
        handler.on_skip(&url, &reason);
    }
    skipped.push(SkippedUrl { url, reason });
}

fn resolve_raw_url(base: &Url, raw: &str) -> Result<Url, url::ParseError> {
    if let Ok(absolute) = Url::parse(raw) {
        return Ok(absolute);
    }

    // RFC 3986's `path-noscheme` grammar forbids a colon in the first segment of a relative
    // reference. The WHATWG parser used by `Url::join` is deliberately more permissive and would
    // otherwise turn malformed inputs such as `::bad::` into an apparently valid child path.
    let first_path_segment = raw
        .split_once(['?', '#'])
        .map_or(raw, |(path, _)| path)
        .split('/')
        .next()
        .unwrap_or_default();
    if first_path_segment.contains(':') {
        return Err(url::ParseError::RelativeUrlWithoutBase);
    }

    base.join(raw)
}

/// Result of enqueueing a URL collection.
///
/// # Examples
///
/// ```
/// use millipede_core::enqueue::EnqueueResult;
///
/// fn accepted_count(result: &EnqueueResult) -> usize {
///     result.added_count()
/// }
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub struct EnqueueResult {
    /// Newly accepted requests.
    pub added: Vec<ProcessedRequest>,
    /// Rejected or duplicate candidates.
    pub skipped: Vec<SkippedUrl>,
}

impl EnqueueResult {
    /// Number of newly accepted requests.
    pub fn added_count(&self) -> usize {
        self.added.len()
    }
    /// Number of skipped candidates.
    pub fn skipped_count(&self) -> usize {
        self.skipped.len()
    }
}

/// A skipped URL candidate.
///
/// This stores a string rather than a `Url` so unparseable raw inputs can be reported.
///
/// # Examples
///
/// ```
/// use millipede_core::enqueue::{SkipReason, SkippedUrl};
///
/// let skipped = SkippedUrl {
///     url: "http://[".to_owned(),
///     reason: SkipReason::InvalidUrl,
/// };
/// assert_eq!(skipped.reason, SkipReason::InvalidUrl);
/// ```
#[derive(Debug, Clone)]
pub struct SkippedUrl {
    /// Original URL text or duplicate unique key.
    pub url: String,
    /// Why the candidate was skipped.
    pub reason: SkipReason,
}

/// Why an enqueue candidate was skipped.
///
/// Robots-based rejection remains deferred until robots policy support lands.
///
/// # Examples
///
/// ```
/// use millipede_core::enqueue::SkipReason;
///
/// let reason = SkipReason::DuplicateUniqueKey;
/// assert_eq!(reason.to_string(), "duplicate unique key");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SkipReason {
    /// The discovered child is deeper than the crawler policy permits.
    MaxDepthExceeded {
        /// The child's crawl depth.
        depth: u32,
        /// The configured maximum depth.
        limit: u32,
    },
    /// The crawl has reached its configured request limit.
    MaxRequestsReached {
        /// The configured maximum request count.
        limit: u64,
    },
    /// The candidate did not satisfy the enqueue strategy.
    StrategyExcluded,
    /// A glob exclusion matched or no glob include matched.
    GlobExcluded,
    /// A regex exclusion matched or no regex include matched.
    RegexExcluded,
    /// The transform explicitly rejected the request.
    TransformRejected {
        /// The transform's rejection explanation.
        reason: String,
    },
    /// The request queue already knew this unique key.
    DuplicateUniqueKey,
    /// The raw URL could not be resolved or a request could not be built.
    InvalidUrl,
}

impl fmt::Display for SkipReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MaxDepthExceeded { depth, limit } => {
                write!(formatter, "crawl depth {depth} exceeds limit {limit}")
            }
            Self::MaxRequestsReached { limit } => {
                write!(formatter, "maximum request count {limit} reached")
            }
            Self::StrategyExcluded => formatter.write_str("excluded by enqueue strategy"),
            Self::GlobExcluded => formatter.write_str("excluded by glob patterns"),
            Self::RegexExcluded => formatter.write_str("excluded by regex patterns"),
            Self::TransformRejected { reason } => {
                write!(formatter, "rejected by transform: {reason}")
            }
            Self::DuplicateUniqueKey => formatter.write_str("duplicate unique key"),
            Self::InvalidUrl => formatter.write_str("invalid URL"),
        }
    }
}
