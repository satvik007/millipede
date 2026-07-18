//! Streaming, gzip-aware XML sitemap ingestion.

mod parser;
mod tandem;

pub use tandem::RequestQueueWithSitemap;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task::JoinHandle};
use url::Url;

use crate::{
    errors::CrawlError,
    http_client::{HttpClient, HttpRequest, StreamingResponse},
    request::{Request, UserData},
    storage::{KeyValueStore, KeyValueStoreExt},
};

use parser::{SitemapEvent, SitemapParseError, XmlPump};

/// Conventional key used to persist sitemap request-list progress.
pub const SITEMAP_STATE_KEY: &str = "SITEMAP_REQUEST_LIST_STATE";

/// Number of emitted requests between automatic progress snapshots.
const AUTO_PERSIST_INTERVAL: u64 = 100;
const MAX_DEPTH: u8 = 5;

/// One URL entry parsed from a sitemap document.
#[derive(Debug, Clone, PartialEq)]
pub struct SitemapEntry {
    /// Absolute URL contained in the `loc` element.
    pub loc: String,
    /// Optional sitemap modification timestamp.
    pub lastmod: Option<String>,
    /// Optional sitemap priority.
    pub priority: Option<f32>,
    /// Optional sitemap change frequency.
    pub changefreq: Option<String>,
}

/// Configures a streaming [`SitemapRequestList`].
#[derive(Default)]
#[must_use = "builders do nothing unless consumed by build"]
pub struct SitemapRequestListBuilder {
    sitemap_urls: Vec<Url>,
    http_client: Option<Arc<dyn HttpClient>>,
    persistence: Option<(Arc<dyn KeyValueStore>, String)>,
    label: Option<String>,
    user_data: UserData,
    limit: Option<u64>,
}

impl SitemapRequestListBuilder {
    /// Adds one root sitemap URL.
    pub fn sitemap_url(mut self, url: Url) -> Self {
        self.sitemap_urls.push(url);
        self
    }

    /// Adds root sitemap URLs.
    pub fn sitemap_urls(mut self, urls: impl IntoIterator<Item = Url>) -> Self {
        self.sitemap_urls.extend(urls);
        self
    }

    /// Sets the HTTP backend used to stream sitemap documents.
    pub fn http_client(mut self, client: Arc<dyn HttpClient>) -> Self {
        self.http_client = Some(client);
        self
    }

    /// Enables persisted progress under `key`.
    ///
    /// An in-progress sitemap is fetched again from byte zero after restart, and
    /// its already-emitted entries are skipped. This requires stable sitemap
    /// ordering across fetches, which is the practical norm for sitemap files.
    pub fn persist(mut self, kvs: Arc<dyn KeyValueStore>, key: impl Into<String>) -> Self {
        self.persistence = Some((kvs, key.into()));
        self
    }

    /// Applies a routing label to every emitted request.
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Applies user data to every emitted request.
    pub fn user_data(mut self, user_data: UserData) -> Self {
        self.user_data = user_data;
        self
    }

    /// Limits the total number of emitted requests.
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Builds a request list without performing network or storage I/O.
    pub fn build(self) -> Result<SitemapRequestList, CrawlError> {
        if self.sitemap_urls.is_empty() {
            return Err(CrawlError::non_retryable(anyhow::anyhow!(
                "at least one sitemap URL is required"
            )));
        }
        let http_client = self.http_client.ok_or_else(|| {
            CrawlError::non_retryable(anyhow::anyhow!("an HTTP client is required"))
        })?;
        let mut sitemap_urls = self.sitemap_urls;
        let mut unique_roots = HashSet::new();
        sitemap_urls.retain(|url| unique_roots.insert(url.clone()));
        let pending = sitemap_urls
            .iter()
            .rev()
            .cloned()
            .map(|url| PendingSitemap { url, depth: 0 })
            .collect();
        Ok(SitemapRequestList {
            inner: Mutex::new(State {
                roots: sitemap_urls,
                http_client,
                persistence: self.persistence,
                label: self.label,
                user_data: self.user_data,
                limit: self.limit,
                pending,
                completed: HashSet::new(),
                completed_failures: HashSet::new(),
                seen_sitemaps: unique_roots
                    .into_iter()
                    .map(|url| url.as_str().to_owned())
                    .collect(),
                emitted_urls: HashSet::new(),
                current: None,
                emitted_total: 0,
                loaded: false,
                finished: false,
                successful_roots: HashSet::new(),
                failed_roots: HashSet::new(),
                resume_skip: None,
                pending_emission: None,
            }),
        })
    }
}

/// A lazy, streaming source of requests parsed from XML sitemaps.
pub struct SitemapRequestList {
    inner: Mutex<State>,
}

impl SitemapRequestList {
    /// Returns the next unique request, fetching sitemap documents only on demand.
    pub async fn fetch_next(&self) -> Result<Option<Request>, CrawlError> {
        self.fetch_next_inner(true).await
    }

    pub(super) async fn fetch_next_for_tandem(&self) -> Result<Option<Request>, CrawlError> {
        self.fetch_next_inner(false).await
    }

    async fn fetch_next_inner(
        &self,
        auto_persist_emission: bool,
    ) -> Result<Option<Request>, CrawlError> {
        let mut state = self.inner.lock().await;
        state.load_persisted().await?;
        loop {
            if state.finished
                || state
                    .limit
                    .is_some_and(|limit| state.emitted_total >= limit)
            {
                state.finished = true;
                state.current = None;
                return Ok(None);
            }

            if state.pending_emission.is_some() {
                let emitted_total = state.emitted_total + 1;
                if auto_persist_emission && emitted_total % AUTO_PERSIST_INTERVAL == 0 {
                    if let Err(error) = state.persist_emission(emitted_total).await {
                        tracing::warn!(%error, "automatic sitemap checkpoint failed");
                    }
                }
                state.emitted_total = emitted_total;
                return Ok(state.pending_emission.take());
            }

            if state.current.is_none() {
                let Some(next) = state.peek_pending() else {
                    state.finished = true;
                    if !state.roots.is_empty()
                        && state.successful_roots.is_empty()
                        && state.failed_roots.len() == state.roots.len()
                    {
                        return Err(CrawlError::retry(anyhow::anyhow!(
                            "all configured root sitemaps failed"
                        )));
                    }
                    return Ok(None);
                };
                match state.open(next.clone()).await {
                    Ok(current) => {
                        state.pending.pop();
                        state.current = Some(current);
                    }
                    Err(error) => {
                        state.pending.pop();
                        state
                            .mark_fetch_failure(next, error, auto_persist_emission)
                            .await?;
                        continue;
                    }
                }
            }

            let event = {
                let current = state.current.as_mut().expect("current sitemap exists");
                current.events.recv().await
            };
            match event {
                Some(Ok(SitemapEvent::Entry(entry))) => {
                    let current = state.current.as_mut().expect("current sitemap exists");
                    current.entries_seen += 1;
                    let url = match Url::parse(&entry.loc) {
                        Ok(url) => url,
                        Err(error) => {
                            tracing::warn!(loc = %entry.loc, %error, "skipping invalid sitemap URL");
                            continue;
                        }
                    };
                    if current.entries_seen <= current.skip_entries {
                        state.emitted_urls.insert(url.as_str().to_owned());
                        continue;
                    }
                    if !state.emitted_urls.insert(url.as_str().to_owned()) {
                        continue;
                    }
                    let mut builder = Request::get(url)
                        .user_data(state.user_data.clone())
                        .crawl_depth(0);
                    if let Some(label) = &state.label {
                        builder = builder.label(label.clone());
                    }
                    let request = builder.build().map_err(CrawlError::non_retryable)?;
                    state.pending_emission = Some(request);
                }
                Some(Ok(SitemapEvent::Nested(location))) => {
                    state.add_nested(location);
                }
                Some(Err(error)) => {
                    tracing::warn!(%error, "sitemap parsing failed");
                    if error.is_body() {
                        state.fail_current(auto_persist_emission).await?;
                    } else {
                        state.complete_current(auto_persist_emission).await?;
                    }
                }
                None => state.complete_current(auto_persist_emission).await?,
            }
        }
    }

    /// Returns whether this list has permanently stopped producing requests.
    pub async fn is_finished(&self) -> bool {
        let state = self.inner.lock().await;
        state.is_finished()
    }

    /// Returns the number of requests emitted by this list.
    pub async fn processed_count(&self) -> u64 {
        let state = self.inner.lock().await;
        state.emitted_total
    }

    /// Persists current progress, or does nothing when persistence is disabled.
    pub async fn persist(&self) -> Result<(), CrawlError> {
        let mut state = self.inner.lock().await;
        state.load_persisted().await?;
        state.persist_now().await
    }
}

#[derive(Clone)]
struct PendingSitemap {
    url: Url,
    depth: u8,
}

struct ActiveSitemap {
    pending: PendingSitemap,
    events: tokio::sync::mpsc::Receiver<Result<SitemapEvent, SitemapParseError>>,
    feeder: JoinHandle<()>,
    entries_seen: u64,
    skip_entries: u64,
    nested: Vec<PendingSitemap>,
}

impl Drop for ActiveSitemap {
    fn drop(&mut self) {
        self.feeder.abort();
    }
}

struct State {
    roots: Vec<Url>,
    http_client: Arc<dyn HttpClient>,
    persistence: Option<(Arc<dyn KeyValueStore>, String)>,
    label: Option<String>,
    user_data: UserData,
    limit: Option<u64>,
    pending: Vec<PendingSitemap>,
    completed: HashSet<String>,
    completed_failures: HashSet<String>,
    seen_sitemaps: HashSet<String>,
    emitted_urls: HashSet<String>,
    current: Option<ActiveSitemap>,
    emitted_total: u64,
    loaded: bool,
    finished: bool,
    successful_roots: HashSet<String>,
    failed_roots: HashSet<String>,
    resume_skip: Option<u64>,
    pending_emission: Option<Request>,
}

impl State {
    async fn load_persisted(&mut self) -> Result<(), CrawlError> {
        if self.loaded {
            return Ok(());
        }
        let Some((kvs, key)) = self.persistence.clone() else {
            self.loaded = true;
            return Ok(());
        };
        let Some(saved) = kvs.get::<PersistedState>(&key).await? else {
            self.loaded = true;
            return Ok(());
        };
        if saved.version != 2 {
            return Err(CrawlError::non_retryable(anyhow::anyhow!(
                "unsupported sitemap state version {}",
                saved.version
            )));
        }
        self.completed = saved.completed.into_iter().collect();
        self.completed_failures = saved.completed_failures.into_iter().collect();
        self.emitted_total = saved.emitted_total;
        self.pending = saved
            .pending
            .into_iter()
            .filter_map(|value| Url::parse(&value).ok())
            .rev()
            .map(|url| {
                let depth = saved
                    .pending_depths
                    .get(url.as_str())
                    .copied()
                    .unwrap_or_else(|| if self.roots.contains(&url) { 0 } else { 1 });
                PendingSitemap { url, depth }
            })
            .collect();
        if let Some(value) = saved.in_progress {
            if let Ok(url) = Url::parse(&value) {
                let depth = saved
                    .in_progress_depth
                    .unwrap_or_else(|| if self.roots.contains(&url) { 0 } else { 1 });
                self.pending.push(PendingSitemap { url, depth });
                self.resume_skip = Some(saved.emitted_in_progress);
            }
        }
        self.seen_sitemaps.extend(self.completed.iter().cloned());
        self.seen_sitemaps.extend(
            self.pending
                .iter()
                .map(|pending| pending.url.as_str().to_owned()),
        );
        for root in &self.roots {
            if self.completed.contains(root.as_str()) {
                if self.completed_failures.contains(root.as_str()) {
                    self.failed_roots.insert(root.as_str().to_owned());
                } else {
                    self.successful_roots.insert(root.as_str().to_owned());
                }
            }
        }
        self.loaded = true;
        Ok(())
    }

    fn is_finished(&self) -> bool {
        if self.finished || self.limit.is_some_and(|limit| self.emitted_total >= limit) {
            return true;
        }
        self.current.is_none()
            && self
                .pending
                .iter()
                .all(|pending| self.completed.contains(pending.url.as_str()))
    }

    fn peek_pending(&mut self) -> Option<PendingSitemap> {
        while let Some(next) = self.pending.last() {
            if self.completed.contains(next.url.as_str()) {
                self.pending.pop();
            } else {
                let next = next.clone();
                self.seen_sitemaps.insert(next.url.as_str().to_owned());
                return Some(next);
            }
        }
        None
    }

    async fn open(&mut self, pending: PendingSitemap) -> Result<ActiveSitemap, CrawlError> {
        let response = self
            .http_client
            .stream(HttpRequest::new(pending.url.clone()))
            .await
            .map_err(CrawlError::retry)?;
        if !response.status.is_success() {
            return Err(CrawlError::retry(anyhow::anyhow!(
                "sitemap {} returned HTTP {}",
                pending.url,
                response.status
            )));
        }
        let (events, feeder) = start_pump(response).await?;
        // Keep the resume cursor cancellation-safe while start_pump awaits the
        // first response-body bytes used for gzip detection.
        let skip_entries = self.resume_skip.take().unwrap_or(0);
        if pending.depth == 0 {
            self.successful_roots
                .insert(pending.url.as_str().to_owned());
        }
        Ok(ActiveSitemap {
            pending,
            events,
            feeder,
            entries_seen: 0,
            skip_entries,
            nested: Vec::new(),
        })
    }

    fn add_nested(&mut self, location: String) {
        let Some(current) = &mut self.current else {
            return;
        };
        if current.pending.depth >= MAX_DEPTH {
            tracing::warn!(url = %location, "skipping sitemap beyond nesting depth cap");
            return;
        }
        match Url::parse(&location) {
            Ok(url) => {
                if self.seen_sitemaps.insert(url.as_str().to_owned()) {
                    let depth = if self.roots.contains(&url) {
                        0
                    } else {
                        current.pending.depth + 1
                    };
                    current.nested.push(PendingSitemap { url, depth });
                }
            }
            Err(error) => {
                tracing::warn!(loc = %location, %error, "skipping invalid nested sitemap URL")
            }
        }
    }

    async fn complete_current(&mut self, persist: bool) -> Result<(), CrawlError> {
        if let Some(mut current) = self.current.take() {
            for nested in current.nested.drain(..).rev() {
                self.pending.push(nested);
            }
            let url = current.pending.url.as_str().to_owned();
            self.completed_failures.remove(&url);
            self.completed.insert(url);
        }
        if persist {
            self.persist_now().await?;
        }
        Ok(())
    }

    async fn mark_fetch_failure(
        &mut self,
        failed: PendingSitemap,
        error: CrawlError,
        persist: bool,
    ) -> Result<(), CrawlError> {
        let url = failed.url.as_str().to_owned();
        tracing::warn!(%url, %error, "sitemap fetch failed; continuing");
        self.resume_skip = None;
        if failed.depth == 0 {
            self.failed_roots.insert(url.clone());
        }
        self.completed_failures.insert(url.clone());
        self.completed.insert(url);
        if persist {
            self.persist_now().await?;
        }
        Ok(())
    }

    async fn fail_current(&mut self, persist: bool) -> Result<(), CrawlError> {
        if let Some(current) = self.current.take() {
            for nested in &current.nested {
                self.seen_sitemaps.remove(nested.url.as_str());
            }
            let url = current.pending.url.as_str().to_owned();
            if current.pending.depth == 0 {
                self.successful_roots.remove(&url);
                self.failed_roots.insert(url.clone());
            }
            self.completed_failures.insert(url.clone());
            self.completed.insert(url);
        }
        if persist {
            self.persist_now().await?;
        }
        Ok(())
    }

    async fn persist_now(&self) -> Result<(), CrawlError> {
        self.persist_snapshot(self.emitted_total, false).await
    }

    async fn persist_emission(&self, emitted_total: u64) -> Result<(), CrawlError> {
        self.persist_snapshot(emitted_total, true).await
    }

    async fn persist_snapshot(
        &self,
        emitted_total: u64,
        include_pending_emission: bool,
    ) -> Result<(), CrawlError> {
        let Some((kvs, key)) = &self.persistence else {
            return Ok(());
        };
        let staged_resume = if self.current.is_none() && self.resume_skip.is_some() {
            self.pending.last()
        } else {
            None
        };
        let pending_end = self.pending.len() - usize::from(staged_resume.is_some());
        let state = PersistedState {
            version: 2,
            completed: self.completed.iter().cloned().collect(),
            completed_failures: self.completed_failures.iter().cloned().collect(),
            in_progress: self
                .current
                .as_ref()
                .map(|current| current.pending.url.as_str().to_owned())
                .or_else(|| staged_resume.map(|pending| pending.url.as_str().to_owned())),
            in_progress_depth: self
                .current
                .as_ref()
                .map(|current| current.pending.depth)
                .or_else(|| staged_resume.map(|pending| pending.depth)),
            emitted_in_progress: self.current.as_ref().map_or_else(
                || self.resume_skip.unwrap_or(0),
                |current| {
                    if include_pending_emission {
                        current.entries_seen
                    } else {
                        current.entries_seen - u64::from(self.pending_emission.is_some())
                    }
                },
            ),
            pending: self.pending[..pending_end]
                .iter()
                .rev()
                .map(|pending| pending.url.as_str().to_owned())
                .collect(),
            pending_depths: self.pending[..pending_end]
                .iter()
                .map(|pending| (pending.url.as_str().to_owned(), pending.depth))
                .collect(),
            emitted_total,
        };
        kvs.set(key, &state).await?;
        Ok(())
    }
}

async fn start_pump(
    mut response: StreamingResponse,
) -> Result<
    (
        tokio::sync::mpsc::Receiver<Result<SitemapEvent, SitemapParseError>>,
        JoinHandle<()>,
    ),
    CrawlError,
> {
    let mut initial = Vec::new();
    let mut prefix = Vec::new();
    while prefix.len() < 2 {
        match response.body.next().await {
            Some(Ok(chunk)) => {
                prefix.extend_from_slice(&chunk[..chunk.len().min(2 - prefix.len())]);
                initial.push(chunk);
            }
            Some(Err(error)) => return Err(CrawlError::retry(error)),
            None => break,
        }
    }
    let gzip = response.url.path().ends_with(".gz") || prefix.as_slice() == [0x1f, 0x8b];
    let (chunk_tx, event_tx, events) = XmlPump::spawn(gzip);
    let feeder = tokio::spawn(async move {
        for chunk in initial {
            let sender = chunk_tx.clone();
            if tokio::task::spawn_blocking(move || sender.send(chunk))
                .await
                .ok()
                .and_then(Result::ok)
                .is_none()
            {
                return;
            }
        }
        while let Some(result) = response.body.next().await {
            match result {
                Ok(chunk) => {
                    let sender = chunk_tx.clone();
                    if tokio::task::spawn_blocking(move || sender.send(chunk))
                        .await
                        .ok()
                        .and_then(Result::ok)
                        .is_none()
                    {
                        return;
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "sitemap response body failed");
                    let _ = event_tx
                        .send(Err(SitemapParseError::body(error.to_string())))
                        .await;
                    return;
                }
            }
        }
    });
    Ok((events, feeder))
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedState {
    version: u8,
    completed: Vec<String>,
    // Version 2 deliberately extends the original six-field version-1 schema:
    // failed-root classification and nesting depth cannot be derived reliably
    // after a restart, but both affect retry and depth-cap correctness.
    #[serde(default)]
    completed_failures: Vec<String>,
    in_progress: Option<String>,
    #[serde(default)]
    in_progress_depth: Option<u8>,
    emitted_in_progress: u64,
    pending: Vec<String>,
    #[serde(default)]
    pending_depths: HashMap<String, u8>,
    emitted_total: u64,
}
