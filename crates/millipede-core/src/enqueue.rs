//! Link enqueueing from handler contexts (URLs-only in Phase 3).

use crate::{
    crawler::CrawlerHandle,
    errors::CrawlError,
    request::{Request, UserData},
    storage::{AddOptions, ProcessedRequest},
};
use std::fmt;
use url::Url;

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
}

impl EnqueueLinker {
    /// Creates a linker from the current crawler handle and parent request.
    pub fn new(crawler: CrawlerHandle, parent: &Request) -> Self {
        Self {
            crawler,
            parent_url: parent.url.clone(),
            parent_depth: parent.crawl_depth,
        }
    }

    /// Starts configuring an enqueue operation.
    pub fn options(&self) -> EnqueueLinksOptions<'_> {
        EnqueueLinksOptions::new(self)
    }

    /// Enqueues absolute URLs using default options.
    pub async fn urls(
        &self,
        urls: impl IntoIterator<Item = Url>,
    ) -> Result<EnqueueResult, CrawlError> {
        self.options().urls(urls).send().await
    }
}

impl fmt::Debug for EnqueueLinker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnqueueLinker")
            .field("crawler", &self.crawler)
            .field("parent_url", &self.parent_url)
            .field("parent_depth", &self.parent_depth)
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
    limit: Option<usize>,
    forefront: bool,
}

enum UrlCandidate {
    Absolute(Url),
    Raw(String),
}

impl<'a> EnqueueLinksOptions<'a> {
    fn new(linker: &'a EnqueueLinker) -> Self {
        Self {
            linker,
            candidates: Vec::new(),
            base_url: None,
            label: None,
            user_data: None,
            limit: None,
            forefront: false,
        }
    }

    /// Extends the absolute URL candidates.
    pub fn urls(mut self, urls: impl IntoIterator<Item = Url>) -> Self {
        self.candidates
            .extend(urls.into_iter().map(UrlCandidate::Absolute));
        self
    }
    /// Extends raw absolute or relative URL candidates.
    pub fn raw_urls<S: Into<String>>(mut self, urls: impl IntoIterator<Item = S>) -> Self {
        self.candidates
            .extend(urls.into_iter().map(|url| UrlCandidate::Raw(url.into())));
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
    /// Caps the number of candidates. Truncated candidates are silently omitted, not reported as
    /// skips.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
    /// Chooses whether children are inserted at the queue front.
    pub fn forefront(mut self, forefront: bool) -> Self {
        self.forefront = forefront;
        self
    }

    /// Resolves, builds, and enqueues all candidates in one queue operation.
    pub async fn send(self) -> Result<EnqueueResult, CrawlError> {
        let base = self.base_url.as_ref().unwrap_or(&self.linker.parent_url);
        let mut skipped = Vec::new();
        let mut candidates = Vec::with_capacity(self.candidates.len());
        for candidate in self.candidates {
            match candidate {
                UrlCandidate::Absolute(url) => candidates.push(url),
                UrlCandidate::Raw(raw) => match resolve_raw_url(base, &raw) {
                    Ok(url) => candidates.push(url),
                    Err(_) => skipped.push(SkippedUrl {
                        url: raw,
                        reason: SkipReason::InvalidUrl,
                    }),
                },
            }
        }
        if let Some(limit) = self.limit {
            candidates.truncate(limit);
        }

        let mut requests = Vec::with_capacity(candidates.len());
        for url in candidates {
            let url_text = url.to_string();
            let mut builder = Request::builder()
                .url(url)
                .crawl_depth(self.linker.parent_depth + 1);
            if let Some(label) = &self.label {
                builder = builder.label(label.clone());
            }
            if let Some(user_data) = &self.user_data {
                builder = builder.user_data(user_data.clone());
            }
            match builder.build() {
                Ok(request) => requests.push(request),
                Err(_) => skipped.push(SkippedUrl {
                    url: url_text,
                    reason: SkipReason::InvalidUrl,
                }),
            }
        }

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
        let processed = batch.wait().await?;
        let mut added = Vec::new();
        for request in processed.processed {
            if request.was_already_present || request.was_already_handled {
                skipped.push(SkippedUrl {
                    url: request.unique_key.clone(),
                    reason: SkipReason::DuplicateUniqueKey,
                });
            } else {
                added.push(request);
            }
        }
        Ok(EnqueueResult { added, skipped })
    }
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
/// Phase 5 adds max-depth, robots, strategy, pattern, and transform reasons.
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
    /// The request queue already knew this unique key.
    DuplicateUniqueKey,
    /// The raw URL could not be resolved or a request could not be built.
    InvalidUrl,
}

impl fmt::Display for SkipReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateUniqueKey => formatter.write_str("duplicate unique key"),
            Self::InvalidUrl => formatter.write_str("invalid URL"),
        }
    }
}
