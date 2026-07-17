//! Integration tests for lazy sitemap-to-queue tandem ingestion.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures_util::{StreamExt, stream};
use http::{HeaderMap, StatusCode};
use millipede_core::{
    crawler::{BasicContext, BasicCrawler, BasicKind, Crawler},
    http_client::{HttpClient, HttpClientError, HttpRequest, HttpResponse, StreamingResponse},
    request::Request,
    sitemap::{RequestQueueWithSitemap, SitemapRequestList, SitemapRequestListBuilder},
    storage::{
        AddOptions, BatchAddHandle, KeyList, KeyValueStore, KvEntry, Lease, LeaseId,
        ListKeysOptions, QueueOpInfo, ReclaimOptions, RequestQueue, RequestSource, StorageError,
        StorageResult,
    },
};
use millipede_storage_memory::{MemoryKeyValueStore, MemoryRequestQueue, MemoryStorageClient};
use tokio::sync::Notify;
use url::Url;

#[derive(Clone)]
struct ResponseFixture {
    chunks: Vec<Bytes>,
    fetched: Arc<AtomicUsize>,
}

#[derive(Default)]
struct FakeHttpClient {
    fixtures: Mutex<HashMap<String, ResponseFixture>>,
}

struct FaultInjectingQueue {
    inner: MemoryRequestQueue,
    add_attempts: AtomicUsize,
    fail_first_add: bool,
    block_first_add: bool,
    add_started: Notify,
}

impl FaultInjectingQueue {
    fn failing(id: &str) -> Self {
        Self {
            inner: MemoryRequestQueue::new(id),
            add_attempts: AtomicUsize::new(0),
            fail_first_add: true,
            block_first_add: false,
            add_started: Notify::new(),
        }
    }

    fn blocking(id: &str) -> Self {
        Self {
            inner: MemoryRequestQueue::new(id),
            add_attempts: AtomicUsize::new(0),
            fail_first_add: false,
            block_first_add: true,
            add_started: Notify::new(),
        }
    }
}

#[async_trait::async_trait]
impl RequestQueue for FaultInjectingQueue {
    async fn add(&self, request: Request, opts: AddOptions) -> StorageResult<QueueOpInfo> {
        let attempt = self.add_attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            if self.fail_first_add {
                return Err(StorageError::Backend(anyhow::anyhow!(
                    "injected add failure"
                )));
            }
            if self.block_first_add {
                self.add_started.notify_one();
                std::future::pending::<()>().await;
            }
        }
        self.inner.add(request, opts).await
    }

    async fn add_batch(
        &self,
        requests: Vec<RequestSource>,
        opts: AddOptions,
    ) -> StorageResult<BatchAddHandle> {
        self.inner.add_batch(requests, opts).await
    }

    async fn fetch_next(&self) -> StorageResult<Option<Lease>> {
        self.inner.fetch_next().await
    }

    async fn mark_handled(&self, lease: Lease) -> StorageResult<()> {
        self.inner.mark_handled(lease).await
    }

    async fn reclaim(&self, lease: Lease, opts: ReclaimOptions) -> StorageResult<()> {
        self.inner.reclaim(lease, opts).await
    }

    async fn renew(&self, lease_id: &LeaseId, extend_by: Duration) -> StorageResult<()> {
        self.inner.renew(lease_id, extend_by).await
    }

    async fn abandon(&self, lease: Lease) -> StorageResult<()> {
        self.inner.abandon(lease).await
    }

    async fn is_empty(&self) -> StorageResult<bool> {
        self.inner.is_empty().await
    }

    async fn is_finished(&self) -> StorageResult<bool> {
        self.inner.is_finished().await
    }

    async fn handled_count(&self) -> StorageResult<u64> {
        self.inner.handled_count().await
    }

    async fn pending_count(&self) -> StorageResult<u64> {
        self.inner.pending_count().await
    }
}

struct FailingSetKvs {
    inner: MemoryKeyValueStore,
}

struct CountingSetKvs {
    inner: MemoryKeyValueStore,
    sets: AtomicUsize,
}

#[async_trait::async_trait]
impl KeyValueStore for FailingSetKvs {
    async fn get_bytes(&self, key: &str) -> StorageResult<Option<KvEntry>> {
        self.inner.get_bytes(key).await
    }

    async fn set_bytes(&self, _key: &str, _bytes: Bytes, _content_type: &str) -> StorageResult<()> {
        Err(StorageError::Backend(anyhow::anyhow!(
            "injected checkpoint failure"
        )))
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        self.inner.delete(key).await
    }

    async fn list_keys(&self, opts: ListKeysOptions) -> StorageResult<KeyList> {
        self.inner.list_keys(opts).await
    }
}

#[async_trait::async_trait]
impl KeyValueStore for CountingSetKvs {
    async fn get_bytes(&self, key: &str) -> StorageResult<Option<KvEntry>> {
        self.inner.get_bytes(key).await
    }

    async fn set_bytes(&self, key: &str, bytes: Bytes, content_type: &str) -> StorageResult<()> {
        self.sets.fetch_add(1, Ordering::SeqCst);
        self.inner.set_bytes(key, bytes, content_type).await
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        self.inner.delete(key).await
    }

    async fn list_keys(&self, opts: ListKeysOptions) -> StorageResult<KeyList> {
        self.inner.list_keys(opts).await
    }
}

impl FakeHttpClient {
    fn add(&self, url: &str, body: impl AsRef<[u8]>, chunk_size: usize) -> ResponseFixture {
        let fixture = ResponseFixture {
            chunks: body
                .as_ref()
                .chunks(chunk_size)
                .map(Bytes::copy_from_slice)
                .collect(),
            fetched: Arc::new(AtomicUsize::new(0)),
        };
        self.fixtures
            .lock()
            .expect("fixture lock")
            .insert(url.to_owned(), fixture.clone());
        fixture
    }
}

#[async_trait::async_trait]
impl HttpClient for FakeHttpClient {
    async fn send(&self, _request: HttpRequest) -> Result<HttpResponse, HttpClientError> {
        panic!("sitemap request lists must use streaming HTTP")
    }

    async fn stream(&self, request: HttpRequest) -> Result<StreamingResponse, HttpClientError> {
        let fixture = self
            .fixtures
            .lock()
            .expect("fixture lock")
            .get(request.url.as_str())
            .cloned()
            .ok_or_else(|| HttpClientError::connect(anyhow::anyhow!("missing fixture")))?;
        fixture.fetched.fetch_add(1, Ordering::SeqCst);
        let body = stream::iter(fixture.chunks.into_iter().map(Ok)).boxed();
        Ok(StreamingResponse::new(
            request.url,
            StatusCode::OK,
            HeaderMap::new(),
            body,
        ))
    }
}

fn urlset(count: usize) -> String {
    let entries = (0..count)
        .map(|index| format!("<url><loc>https://example.com/{index}</loc></url>"))
        .collect::<String>();
    format!("<?xml version=\"1.0\"?><urlset>{entries}</urlset>")
}

fn sitemap_list(client: Arc<FakeHttpClient>, sitemap_url: &str) -> SitemapRequestList {
    SitemapRequestListBuilder::default()
        .sitemap_url(Url::parse(sitemap_url).expect("sitemap URL"))
        .http_client(client)
        .build()
        .expect("sitemap list")
}

#[tokio::test]
async fn tandem_drains_lazily_and_finishes_with_inner_queue() -> anyhow::Result<()> {
    let client = Arc::new(FakeHttpClient::default());
    let fixture = client.add("https://example.com/sitemap.xml", urlset(10), 13);
    let inner: Arc<dyn RequestQueue> = Arc::new(MemoryRequestQueue::new("tandem"));
    let queue = RequestQueueWithSitemap::new(
        inner,
        sitemap_list(client, "https://example.com/sitemap.xml"),
    )
    .batch_size(3);

    assert!(!queue.is_finished().await?);
    let first = queue.fetch_next().await?.expect("first sitemap request");
    assert_eq!(fixture.fetched.load(Ordering::SeqCst), 1);
    assert!(queue.pending_count().await? <= 3);
    let mut urls = HashSet::new();
    urls.insert(first.request.url.to_string());
    queue.mark_handled(first).await?;

    let second = queue.fetch_next().await?.expect("second sitemap request");
    assert_eq!(fixture.fetched.load(Ordering::SeqCst), 1);
    urls.insert(second.request.url.to_string());
    queue.mark_handled(second).await?;

    let mut handled = 2usize;
    let mut final_lease = None;
    while let Some(lease) = queue.fetch_next().await? {
        urls.insert(lease.request.url.to_string());
        handled += 1;
        if handled == 10 {
            final_lease = Some(lease);
            break;
        }
        queue.mark_handled(lease).await?;
    }

    assert_eq!(handled, 10);
    assert_eq!(urls.len(), 10);
    assert!(!queue.is_finished().await?);
    queue
        .mark_handled(final_lease.expect("final sitemap lease"))
        .await?;
    assert!(queue.is_finished().await?);
    assert_eq!(queue.handled_count().await?, 10);
    Ok(())
}

#[tokio::test]
async fn manual_and_sitemap_duplicates_are_handled_once() -> anyhow::Result<()> {
    let client = Arc::new(FakeHttpClient::default());
    client.add("https://example.com/sitemap.xml", urlset(3), 9);
    let queue = RequestQueueWithSitemap::new(
        Arc::new(MemoryRequestQueue::new("dedupe")),
        sitemap_list(client, "https://example.com/sitemap.xml"),
    );
    queue
        .add(
            Request::get("https://example.com/1").build()?,
            AddOptions::default(),
        )
        .await?;

    let mut urls = Vec::new();
    while let Some(lease) = queue.fetch_next().await? {
        urls.push(lease.request.url.to_string());
        queue.mark_handled(lease).await?;
    }

    urls.sort();
    assert_eq!(
        urls,
        [
            "https://example.com/0",
            "https://example.com/1",
            "https://example.com/2"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_fetchers_do_not_double_drain() -> anyhow::Result<()> {
    const URL_COUNT: usize = 40;
    let client = Arc::new(FakeHttpClient::default());
    let fixture = client.add("https://example.com/sitemap.xml", urlset(URL_COUNT), 7);
    let queue: Arc<dyn RequestQueue> = Arc::new(
        RequestQueueWithSitemap::new(
            Arc::new(MemoryRequestQueue::new("concurrent")),
            sitemap_list(client, "https://example.com/sitemap.xml"),
        )
        .batch_size(4),
    );

    let mut workers = Vec::new();
    for _ in 0..8 {
        let queue = queue.clone();
        workers.push(tokio::spawn(async move {
            let mut urls = Vec::new();
            while let Some(lease) = queue.fetch_next().await? {
                urls.push(lease.request.url.to_string());
                queue.mark_handled(lease).await?;
            }
            Ok::<_, millipede_core::storage::StorageError>(urls)
        }));
    }

    let mut all_urls = Vec::new();
    for worker in workers {
        all_urls.extend(worker.await??);
    }
    let distinct: HashSet<_> = all_urls.iter().collect();
    assert_eq!(all_urls.len(), URL_COUNT);
    assert_eq!(distinct.len(), URL_COUNT);
    assert_eq!(fixture.fetched.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn failed_inner_add_is_retried_without_losing_the_request() -> anyhow::Result<()> {
    let client = Arc::new(FakeHttpClient::default());
    client.add("https://example.com/sitemap.xml", urlset(3), 7);
    let inner = Arc::new(FaultInjectingQueue::failing("failed-add"));
    let queue = RequestQueueWithSitemap::new(
        inner.clone(),
        sitemap_list(client, "https://example.com/sitemap.xml"),
    );

    assert!(queue.fetch_next().await.is_err());
    assert!(!queue.is_empty().await?);

    let mut urls = Vec::new();
    while let Some(lease) = queue.fetch_next().await? {
        urls.push(lease.request.url.to_string());
        queue.mark_handled(lease).await?;
    }

    urls.sort();
    assert_eq!(
        urls,
        [
            "https://example.com/0",
            "https://example.com/1",
            "https://example.com/2"
        ]
    );
    assert_eq!(inner.add_attempts.load(Ordering::SeqCst), 4);
    Ok(())
}

#[tokio::test]
async fn cancelled_inner_add_is_retried_without_losing_the_request() -> anyhow::Result<()> {
    let client = Arc::new(FakeHttpClient::default());
    client.add("https://example.com/sitemap.xml", urlset(2), 7);
    let inner = Arc::new(FaultInjectingQueue::blocking("cancelled-add"));
    let queue = Arc::new(RequestQueueWithSitemap::new(
        inner.clone(),
        sitemap_list(client, "https://example.com/sitemap.xml"),
    ));

    let fetch = tokio::spawn({
        let queue = queue.clone();
        async move { queue.fetch_next().await }
    });
    inner.add_started.notified().await;
    fetch.abort();
    assert!(
        fetch
            .await
            .expect_err("fetch task should be cancelled")
            .is_cancelled()
    );
    assert!(!queue.is_finished().await?);

    let first = tokio::time::timeout(Duration::from_secs(1), queue.fetch_next())
        .await??
        .expect("staged request should be retried");
    assert_eq!(first.request.url.as_str(), "https://example.com/0");
    queue.mark_handled(first).await?;

    let second = queue.fetch_next().await?.expect("second request");
    assert_eq!(second.request.url.as_str(), "https://example.com/1");
    queue.mark_handled(second).await?;
    assert!(queue.fetch_next().await?.is_none());
    assert_eq!(inner.add_attempts.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn checkpoint_failures_never_fail_tandem_fetch() -> anyhow::Result<()> {
    let client = Arc::new(FakeHttpClient::default());
    let sitemap_url = "https://example.com/sitemap.xml";
    client.add(sitemap_url, urlset(2), 7);
    let list = SitemapRequestListBuilder::default()
        .sitemap_url(Url::parse(sitemap_url)?)
        .http_client(client)
        .persist(
            Arc::new(FailingSetKvs {
                inner: MemoryKeyValueStore::new("failing-checkpoint"),
            }),
            "state",
        )
        .build()?;
    let queue = RequestQueueWithSitemap::new(
        Arc::new(MemoryRequestQueue::new("checkpoint-failure")),
        list,
    );

    let mut handled = 0;
    while let Some(lease) = queue.fetch_next().await? {
        handled += 1;
        queue.mark_handled(lease).await?;
    }

    assert_eq!(handled, 2);
    assert!(queue.is_finished().await?);
    Ok(())
}

#[tokio::test]
async fn standalone_completion_checkpoint_failure_is_returned() -> anyhow::Result<()> {
    let client = Arc::new(FakeHttpClient::default());
    let sitemap_url = "https://example.com/sitemap.xml";
    client.add(sitemap_url, urlset(1), 7);
    let list = SitemapRequestListBuilder::default()
        .sitemap_url(Url::parse(sitemap_url)?)
        .http_client(client)
        .persist(
            Arc::new(FailingSetKvs {
                inner: MemoryKeyValueStore::new("standalone-failing-checkpoint"),
            }),
            "state",
        )
        .build()?;

    assert!(list.fetch_next().await?.is_some());
    let error = list
        .fetch_next()
        .await
        .expect_err("completion checkpoint failure must be returned");
    assert!(error.to_string().contains("injected checkpoint failure"));
    Ok(())
}

#[tokio::test]
async fn tandem_persists_once_per_drained_batch() -> anyhow::Result<()> {
    const URL_COUNT: usize = 70;
    const BATCH_SIZE: usize = 32;
    let client = Arc::new(FakeHttpClient::default());
    let sitemap_url = "https://example.com/sitemap.xml";
    client.add(sitemap_url, urlset(URL_COUNT), 11);
    let kvs = Arc::new(CountingSetKvs {
        inner: MemoryKeyValueStore::new("counting-checkpoint"),
        sets: AtomicUsize::new(0),
    });
    let list = SitemapRequestListBuilder::default()
        .sitemap_url(Url::parse(sitemap_url)?)
        .http_client(client)
        .persist(kvs.clone(), "state")
        .build()?;
    let queue =
        RequestQueueWithSitemap::new(Arc::new(MemoryRequestQueue::new("batch-checkpoint")), list)
            .batch_size(BATCH_SIZE);

    while let Some(lease) = queue.fetch_next().await? {
        queue.mark_handled(lease).await?;
    }

    assert_eq!(
        kvs.sets.load(Ordering::SeqCst),
        URL_COUNT.div_ceil(BATCH_SIZE)
    );
    Ok(())
}

#[tokio::test]
async fn crawler_builder_accepts_tandem_queue() -> anyhow::Result<()> {
    const URL_COUNT: usize = 10;
    let client = Arc::new(FakeHttpClient::default());
    client.add("https://example.com/sitemap.xml", urlset(URL_COUNT), 11);
    let tandem = RequestQueueWithSitemap::new(
        Arc::new(MemoryRequestQueue::new("crawler-tandem")),
        sitemap_list(client, "https://example.com/sitemap.xml"),
    );
    let handled = Arc::new(AtomicUsize::new(0));
    let count = handled.clone();
    let crawler: BasicCrawler = Crawler::builder(BasicKind)
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .request_queue(Arc::new(tandem))
        .request_handler(move |_ctx: BasicContext| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .build()
        .await?;

    let stats = crawler.run(["https://example.com/start"]).await?;
    assert_eq!(handled.load(Ordering::SeqCst), URL_COUNT + 1);
    assert_eq!(stats.requests_finished, (URL_COUNT + 1) as u64);
    Ok(())
}

#[tokio::test]
async fn final_drain_persists_completed_sitemap_state() -> anyhow::Result<()> {
    let client = Arc::new(FakeHttpClient::default());
    let sitemap_url = "https://example.com/sitemap.xml";
    client.add(sitemap_url, urlset(5), 8);
    let kvs = Arc::new(MemoryKeyValueStore::new("sitemap-state"));
    let key = "tandem-state";
    let list = SitemapRequestListBuilder::default()
        .sitemap_url(Url::parse(sitemap_url)?)
        .http_client(client)
        .persist(kvs.clone(), key)
        .build()?;
    let queue =
        RequestQueueWithSitemap::new(Arc::new(MemoryRequestQueue::new("persistent-tandem")), list);

    while let Some(lease) = queue.fetch_next().await? {
        queue.mark_handled(lease).await?;
    }

    let entry = kvs.get_bytes(key).await?.expect("persisted sitemap state");
    let state: serde_json::Value = serde_json::from_slice(&entry.value)?;
    assert_eq!(state["emitted_total"], 5);
    assert_eq!(state["in_progress"], serde_json::Value::Null);
    assert_eq!(state["pending"], serde_json::json!([]));
    assert_eq!(state["completed"].as_array().expect("completed").len(), 1);
    assert_eq!(state["completed"][0], sitemap_url);
    Ok(())
}
