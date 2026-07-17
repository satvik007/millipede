//! Integration tests for streaming sitemap request lists.

use std::{
    collections::HashMap,
    io::Write,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use flate2::{Compression, write::GzEncoder};
use futures_util::{StreamExt, stream};
use http::{HeaderMap, StatusCode};
use millipede_core::{
    http_client::{HttpClient, HttpClientError, HttpRequest, HttpResponse, StreamingResponse},
    request::UserData,
    sitemap::{SitemapRequestList, SitemapRequestListBuilder},
    storage::{KeyList, KeyValueStore, KvEntry, ListKeysOptions, StorageError, StorageResult},
};
use millipede_storage_memory::MemoryKeyValueStore;
use tokio::sync::Notify;
use url::Url;

#[derive(Clone)]
struct ResponseFixture {
    status: StatusCode,
    chunks: Vec<Bytes>,
    fetched: Arc<AtomicUsize>,
    served: Arc<AtomicUsize>,
    body_error: bool,
}

#[derive(Default)]
struct FakeHttpClient {
    fixtures: Mutex<HashMap<String, ResponseFixture>>,
}

struct FaultyKvs {
    inner: Arc<MemoryKeyValueStore>,
    fail_first_get: bool,
    fail_first_set: bool,
    get_calls: AtomicUsize,
    set_calls: AtomicUsize,
}

struct BlockingHttpClient {
    inner: Arc<FakeHttpClient>,
    block_first: AtomicBool,
    started: Notify,
}

struct BlockingBodyHttpClient {
    inner: Arc<FakeHttpClient>,
    block_first: AtomicBool,
    started: Arc<Notify>,
}

struct BlockingSetKvs {
    inner: Arc<MemoryKeyValueStore>,
    block_first: AtomicBool,
    started: Notify,
}

impl FaultyKvs {
    fn new(inner: Arc<MemoryKeyValueStore>, fail_first_get: bool, fail_first_set: bool) -> Self {
        Self {
            inner,
            fail_first_get,
            fail_first_set,
            get_calls: AtomicUsize::new(0),
            set_calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl KeyValueStore for FaultyKvs {
    async fn get_bytes(&self, key: &str) -> StorageResult<Option<KvEntry>> {
        if self.fail_first_get && self.get_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(StorageError::Backend(anyhow::anyhow!(
                "transient get failure"
            )));
        }
        self.inner.get_bytes(key).await
    }

    async fn set_bytes(&self, key: &str, bytes: Bytes, content_type: &str) -> StorageResult<()> {
        if self.fail_first_set && self.set_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(StorageError::Backend(anyhow::anyhow!(
                "transient set failure"
            )));
        }
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
        self.add_status(url, StatusCode::OK, body, chunk_size)
    }

    fn add_status(
        &self,
        url: &str,
        status: StatusCode,
        body: impl AsRef<[u8]>,
        chunk_size: usize,
    ) -> ResponseFixture {
        let fixture = ResponseFixture {
            status,
            chunks: body
                .as_ref()
                .chunks(chunk_size)
                .map(Bytes::copy_from_slice)
                .collect(),
            fetched: Arc::new(AtomicUsize::new(0)),
            served: Arc::new(AtomicUsize::new(0)),
            body_error: false,
        };
        self.fixtures
            .lock()
            .expect("fixture lock")
            .insert(url.to_owned(), fixture.clone());
        fixture
    }

    fn add_body_error(
        &self,
        url: &str,
        body: impl AsRef<[u8]>,
        chunk_size: usize,
    ) -> ResponseFixture {
        let mut fixture = self.add(url, body, chunk_size);
        fixture.body_error = true;
        self.fixtures
            .lock()
            .expect("fixture lock")
            .insert(url.to_owned(), fixture.clone());
        fixture
    }
}

#[async_trait::async_trait]
impl HttpClient for BlockingHttpClient {
    async fn send(&self, _request: HttpRequest) -> Result<HttpResponse, HttpClientError> {
        panic!("sitemap request lists must use streaming HTTP")
    }

    async fn stream(&self, request: HttpRequest) -> Result<StreamingResponse, HttpClientError> {
        if self.block_first.swap(false, Ordering::SeqCst) {
            self.started.notify_one();
            std::future::pending::<()>().await;
        }
        self.inner.stream(request).await
    }
}

#[async_trait::async_trait]
impl HttpClient for BlockingBodyHttpClient {
    async fn send(&self, _request: HttpRequest) -> Result<HttpResponse, HttpClientError> {
        panic!("sitemap request lists must use streaming HTTP")
    }

    async fn stream(&self, request: HttpRequest) -> Result<StreamingResponse, HttpClientError> {
        if self.block_first.swap(false, Ordering::SeqCst) {
            let started = self.started.clone();
            let body = stream::once(async move {
                started.notify_one();
                std::future::pending::<Result<Bytes, HttpClientError>>().await
            })
            .boxed();
            return Ok(StreamingResponse::new(
                request.url,
                StatusCode::OK,
                HeaderMap::new(),
                body,
            ));
        }
        self.inner.stream(request).await
    }
}

#[async_trait::async_trait]
impl KeyValueStore for BlockingSetKvs {
    async fn get_bytes(&self, key: &str) -> StorageResult<Option<KvEntry>> {
        self.inner.get_bytes(key).await
    }

    async fn set_bytes(&self, key: &str, bytes: Bytes, content_type: &str) -> StorageResult<()> {
        if self.block_first.swap(false, Ordering::SeqCst) {
            self.started.notify_one();
            std::future::pending::<()>().await;
        }
        self.inner.set_bytes(key, bytes, content_type).await
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        self.inner.delete(key).await
    }

    async fn list_keys(&self, opts: ListKeysOptions) -> StorageResult<KeyList> {
        self.inner.list_keys(opts).await
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
        let served = fixture.served.clone();
        let body_error = fixture.body_error;
        let body = stream::iter(fixture.chunks.into_iter().map(move |chunk| {
            served.fetch_add(1, Ordering::SeqCst);
            Ok(chunk)
        }))
        .chain(stream::iter(body_error.then(|| {
            Err(HttpClientError::io(anyhow::anyhow!("fixture body failure")))
        })))
        .boxed();
        Ok(StreamingResponse::new(
            request.url,
            fixture.status,
            HeaderMap::new(),
            body,
        ))
    }
}

fn urlset(start: usize, count: usize) -> String {
    let entries = (start..start + count)
        .map(|index| {
            format!(
                "<url><loc>https://example.com/{index}?a=1&amp;b=2</loc><lastmod>2026-01-01</lastmod><priority>0.5</priority><changefreq>daily</changefreq></url>"
            )
        })
        .collect::<String>();
    format!("<?xml version=\"1.0\"?><urlset xmlns=\"x\">{entries}</urlset>")
}

fn list(client: Arc<FakeHttpClient>, roots: &[&str]) -> SitemapRequestList {
    SitemapRequestListBuilder::default()
        .sitemap_urls(roots.iter().map(|url| Url::parse(url).expect("valid URL")))
        .http_client(client)
        .build()
        .expect("valid sitemap list")
}

async fn drain(list: &SitemapRequestList) -> Vec<String> {
    let mut urls = Vec::new();
    while let Some(request) = list.fetch_next().await.expect("fetch next") {
        urls.push(request.url.to_string());
    }
    urls
}

#[tokio::test]
async fn plain_urlset_applies_request_metadata() {
    let client = Arc::new(FakeHttpClient::default());
    client.add("https://example.com/sitemap.xml", urlset(0, 5), 11);
    let mut user_data = UserData::default();
    user_data.set_typed("source", &"sitemap").expect("JSON");
    let list = SitemapRequestListBuilder::default()
        .sitemap_url(Url::parse("https://example.com/sitemap.xml").expect("URL"))
        .http_client(client)
        .label("product")
        .user_data(user_data.clone())
        .build()
        .expect("list");

    for index in 0..5 {
        let request = list.fetch_next().await.expect("fetch").expect("request");
        assert_eq!(request.label.as_deref(), Some("product"));
        assert_eq!(request.user_data, user_data);
        assert_eq!(request.crawl_depth, 0);
        assert!(request.url.as_str().contains(&format!("/{index}?")));
    }
    assert!(list.fetch_next().await.expect("fetch").is_none());
    assert!(list.is_finished().await);
    assert_eq!(list.processed_count().await, 5);
}

#[tokio::test]
async fn text_and_cdata_fragments_are_concatenated() {
    let client = Arc::new(FakeHttpClient::default());
    let body = "<urlset><url><loc>https://example.com/<![CDATA[path]]></loc><priority>0.<![CDATA[5]]></priority></url></urlset>";
    client.add("https://example.com/sitemap.xml", body, 4);

    assert_eq!(
        drain(&list(client, &["https://example.com/sitemap.xml"])).await,
        vec!["https://example.com/path"]
    );
}

#[tokio::test]
async fn gzip_is_detected_by_suffix_and_magic() {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(urlset(0, 2).as_bytes())
        .expect("compress");
    let compressed = encoder.finish().expect("finish gzip");
    let client = Arc::new(FakeHttpClient::default());
    client.add("https://example.com/sitemap.xml.gz", &compressed, 7);
    client.add("https://example.com/magic.xml", &compressed, 1);

    assert_eq!(
        drain(&list(
            client.clone(),
            &["https://example.com/sitemap.xml.gz"]
        ))
        .await
        .len(),
        2
    );
    assert_eq!(
        drain(&list(client, &["https://example.com/magic.xml"]))
            .await
            .len(),
        2
    );
}

#[tokio::test]
async fn sitemap_index_fetches_children_lazily() {
    let client = Arc::new(FakeHttpClient::default());
    let index = "<sitemapindex><sitemap><loc>https://example.com/one.xml</loc></sitemap><sitemap><loc>https://example.com/two.xml</loc></sitemap></sitemapindex>";
    client.add("https://example.com/index.xml", index, 9);
    client.add("https://example.com/one.xml", urlset(0, 2), 8);
    let second = client.add("https://example.com/two.xml", urlset(2, 2), 8);
    let list = list(client, &["https://example.com/index.xml"]);

    assert!(list.fetch_next().await.expect("fetch").is_some());
    assert!(list.fetch_next().await.expect("fetch").is_some());
    assert_eq!(second.fetched.load(Ordering::SeqCst), 0);
    assert!(list.fetch_next().await.expect("fetch").is_some());
    assert_eq!(second.fetched.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_index_releases_staged_children_for_later_roots() {
    let client = Arc::new(FakeHttpClient::default());
    let child = "https://example.com/child.xml";
    let index = format!(
        "<sitemapindex><sitemap><loc>{child}</loc></sitemap></sitemapindex>{}",
        " ".repeat(1_024)
    );
    client.add_body_error("https://example.com/bad.xml", index, 1);
    client.add(
        "https://example.com/good.xml",
        format!("<sitemapindex><sitemap><loc>{child}</loc></sitemap></sitemapindex>"),
        7,
    );
    let child_fixture = client.add(child, urlset(0, 1), 7);

    assert_eq!(
        drain(&list(
            client,
            &[
                "https://example.com/bad.xml",
                "https://example.com/good.xml",
            ],
        ))
        .await,
        vec!["https://example.com/0?a=1&b=2"]
    );
    assert_eq!(child_fixture.fetched.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn configured_root_referenced_by_an_index_keeps_depth_zero() {
    let client = Arc::new(FakeHttpClient::default());
    client.add(
        "https://example.com/a.xml",
        "<sitemapindex><sitemap><loc>https://example.com/b.xml</loc></sitemap></sitemapindex>",
        7,
    );
    for depth in 0..5 {
        let current = if depth == 0 {
            "https://example.com/b.xml".to_owned()
        } else {
            format!("https://example.com/l{depth}.xml")
        };
        let next = if depth == 4 {
            "https://example.com/content.xml".to_owned()
        } else {
            format!("https://example.com/l{}.xml", depth + 1)
        };
        client.add(
            &current,
            format!("<sitemapindex><sitemap><loc>{next}</loc></sitemap></sitemapindex>"),
            7,
        );
    }
    client.add("https://example.com/content.xml", urlset(0, 1), 7);

    assert_eq!(
        drain(&list(
            client,
            &["https://example.com/a.xml", "https://example.com/b.xml"],
        ))
        .await,
        vec!["https://example.com/0?a=1&b=2"]
    );
}

#[tokio::test]
async fn malformed_entries_are_skipped_without_losing_following_entries() {
    let body = "<urlset><url><lastmod>today</lastmod></url><url><loc>https://example.com/broken</oops></url><url><loc>https://example.com/good</loc></url></urlset>";
    let client = Arc::new(FakeHttpClient::default());
    client.add("https://example.com/sitemap.xml", body, 5);
    assert_eq!(
        drain(&list(client, &["https://example.com/sitemap.xml"])).await,
        vec!["https://example.com/good"]
    );
}

#[tokio::test]
async fn malformed_sitemap_index_entries_are_skipped() {
    let body = "<sitemapindex><sitemap><loc>https://example.com/broken.xml</oops></sitemap><sitemap><loc>https://example.com/good.xml</loc></sitemap></sitemapindex>";
    let client = Arc::new(FakeHttpClient::default());
    client.add("https://example.com/index.xml", body, 5);
    client.add("https://example.com/good.xml", urlset(0, 1), 5);

    assert_eq!(
        drain(&list(client, &["https://example.com/index.xml"])).await,
        vec!["https://example.com/0?a=1&b=2"]
    );
}

#[tokio::test]
async fn limit_stops_at_exactly_three_requests() {
    let client = Arc::new(FakeHttpClient::default());
    client.add("https://example.com/sitemap.xml", urlset(0, 10), 13);
    let list = SitemapRequestListBuilder::default()
        .sitemap_url(Url::parse("https://example.com/sitemap.xml").expect("URL"))
        .http_client(client)
        .limit(3)
        .build()
        .expect("list");
    assert_eq!(drain(&list).await.len(), 3);
    assert!(list.is_finished().await);
}

#[tokio::test]
async fn persisted_progress_resumes_without_refetching_completed_sitemap() {
    let client = Arc::new(FakeHttpClient::default());
    let first = client.add("https://example.com/one.xml", urlset(0, 5), 7);
    client.add("https://example.com/two.xml", urlset(5, 5), 7);
    let kvs: Arc<dyn KeyValueStore> = Arc::new(MemoryKeyValueStore::new("sitemap"));
    let build = || {
        SitemapRequestListBuilder::default()
            .sitemap_urls([
                Url::parse("https://example.com/one.xml").expect("URL"),
                Url::parse("https://example.com/two.xml").expect("URL"),
            ])
            .http_client(client.clone())
            .persist(kvs.clone(), "progress")
            .build()
            .expect("list")
    };
    let original = build();
    let mut initial = Vec::new();
    for _ in 0..7 {
        initial.push(
            original
                .fetch_next()
                .await
                .expect("fetch")
                .expect("request")
                .url
                .to_string(),
        );
    }
    original.persist().await.expect("persist");
    let resumed = build();
    let remaining = drain(&resumed).await;
    assert_eq!(remaining.len(), 3);
    assert!(initial.iter().all(|url| !remaining.contains(url)));
    assert_eq!(first.fetched.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn all_root_failures_return_an_error() {
    let client = Arc::new(FakeHttpClient::default());
    client.add_status(
        "https://example.com/one.xml",
        StatusCode::INTERNAL_SERVER_ERROR,
        "",
        1,
    );
    client.add_status("https://example.com/two.xml", StatusCode::NOT_FOUND, "", 1);
    let list = list(
        client,
        &["https://example.com/one.xml", "https://example.com/two.xml"],
    );
    let error = list.fetch_next().await.expect_err("all roots should fail");
    assert!(error.is_retryable());
    assert!(error.counts_against_retries());
}

#[tokio::test]
async fn duplicated_failing_root_still_returns_an_error() {
    let client = Arc::new(FakeHttpClient::default());
    client.add_status(
        "https://example.com/failing.xml",
        StatusCode::INTERNAL_SERVER_ERROR,
        "",
        1,
    );
    let list = list(
        client,
        &[
            "https://example.com/failing.xml",
            "https://example.com/failing.xml",
        ],
    );

    assert!(list.fetch_next().await.is_err());
}

#[tokio::test]
async fn cancellation_while_opening_does_not_lose_the_pending_sitemap() {
    let inner = Arc::new(FakeHttpClient::default());
    inner.add("https://example.com/sitemap.xml", urlset(0, 1), 8);
    let client = Arc::new(BlockingHttpClient {
        inner,
        block_first: AtomicBool::new(true),
        started: Notify::new(),
    });
    let list = Arc::new(
        SitemapRequestListBuilder::default()
            .sitemap_url(Url::parse("https://example.com/sitemap.xml").expect("URL"))
            .http_client(client.clone())
            .build()
            .expect("list"),
    );
    let task_list = list.clone();
    let task = tokio::spawn(async move { task_list.fetch_next().await });
    client.started.notified().await;
    task.abort();
    assert!(
        task.await
            .expect_err("fetch should be cancelled")
            .is_cancelled()
    );

    assert!(list.fetch_next().await.expect("retry fetch").is_some());
}

#[tokio::test]
async fn cancellation_during_resumed_body_peek_preserves_the_skip_count() {
    let inner = Arc::new(FakeHttpClient::default());
    inner.add("https://example.com/sitemap.xml", urlset(0, 3), 8);
    let kvs: Arc<dyn KeyValueStore> = Arc::new(MemoryKeyValueStore::new("cancel-body-peek"));
    let original = SitemapRequestListBuilder::default()
        .sitemap_url(Url::parse("https://example.com/sitemap.xml").expect("URL"))
        .http_client(inner.clone())
        .persist(kvs.clone(), "progress")
        .build()
        .expect("list");
    assert_eq!(
        original
            .fetch_next()
            .await
            .expect("fetch")
            .expect("request")
            .url
            .as_str(),
        "https://example.com/0?a=1&b=2"
    );
    original.persist().await.expect("persist");

    let started = Arc::new(Notify::new());
    let client = Arc::new(BlockingBodyHttpClient {
        inner,
        block_first: AtomicBool::new(true),
        started: started.clone(),
    });
    let resumed = Arc::new(
        SitemapRequestListBuilder::default()
            .sitemap_url(Url::parse("https://example.com/sitemap.xml").expect("URL"))
            .http_client(client)
            .persist(kvs, "progress")
            .build()
            .expect("list"),
    );
    let task_list = resumed.clone();
    let task = tokio::spawn(async move { task_list.fetch_next().await });
    started.notified().await;
    task.abort();
    assert!(
        task.await
            .expect_err("fetch should be cancelled")
            .is_cancelled()
    );

    assert_eq!(
        resumed
            .fetch_next()
            .await
            .expect("retry fetch")
            .expect("request")
            .url
            .as_str(),
        "https://example.com/1?a=1&b=2"
    );
    assert_eq!(drain(&resumed).await, vec!["https://example.com/2?a=1&b=2"]);
}

#[tokio::test]
async fn first_request_arrives_before_the_document_is_fully_served() {
    let client = Arc::new(FakeHttpClient::default());
    let fixture = client.add("https://example.com/large.xml", urlset(0, 1_000), 16);
    let total_chunks = fixture.chunks.len();
    let list = list(client, &["https://example.com/large.xml"]);

    assert!(list.fetch_next().await.expect("fetch").is_some());
    assert!(fixture.served.load(Ordering::SeqCst) < total_chunks);
}

#[tokio::test]
async fn extension_locations_do_not_replace_direct_child_locations() {
    let client = Arc::new(FakeHttpClient::default());
    let extension_urlset = "<urlset xmlns:image=\"image\"><url><loc>https://example.com/page</loc><image:image><image:loc>https://example.com/image.jpg</image:loc></image:image></url></urlset>";
    client.add("https://example.com/urls.xml", extension_urlset, 9);
    assert_eq!(
        drain(&list(client.clone(), &["https://example.com/urls.xml"])).await,
        vec!["https://example.com/page"]
    );

    let index = "<sitemapindex xmlns:ext=\"extension\"><sitemap><loc>https://example.com/child.xml</loc><ext:data><ext:loc>https://example.com/wrong.xml</ext:loc></ext:data></sitemap></sitemapindex>";
    client.add("https://example.com/index.xml", index, 7);
    client.add("https://example.com/child.xml", urlset(0, 1), 7);
    assert_eq!(
        drain(&list(client, &["https://example.com/index.xml"])).await,
        vec!["https://example.com/0?a=1&b=2"]
    );
}

#[tokio::test]
async fn extension_entry_end_tags_do_not_close_sitemap_entries() {
    let client = Arc::new(FakeHttpClient::default());
    let urlset_body = "<urlset xmlns:ext=\"extension\"><url><ext:url></ext:url><loc>https://example.com/good</loc></url></urlset>";
    client.add("https://example.com/urls.xml", urlset_body, 7);
    assert_eq!(
        drain(&list(client.clone(), &["https://example.com/urls.xml"])).await,
        vec!["https://example.com/good"]
    );

    let index = "<sitemapindex xmlns:ext=\"extension\"><sitemap><ext:sitemap></ext:sitemap><loc>https://example.com/child.xml</loc></sitemap></sitemapindex>";
    client.add("https://example.com/index.xml", index, 7);
    client.add("https://example.com/child.xml", urlset(0, 1), 7);
    assert_eq!(
        drain(&list(client, &["https://example.com/index.xml"])).await,
        vec!["https://example.com/0?a=1&b=2"]
    );
}

#[tokio::test]
async fn failed_resumed_fetch_does_not_skip_entries_in_the_next_sitemap() {
    let client = Arc::new(FakeHttpClient::default());
    client.add("https://example.com/resume.xml", urlset(0, 2), 8);
    client.add("https://example.com/next.xml", urlset(10, 3), 8);
    let kvs: Arc<dyn KeyValueStore> = Arc::new(MemoryKeyValueStore::new("resume-failure"));
    let build = || {
        SitemapRequestListBuilder::default()
            .sitemap_urls([
                Url::parse("https://example.com/resume.xml").expect("URL"),
                Url::parse("https://example.com/next.xml").expect("URL"),
            ])
            .http_client(client.clone())
            .persist(kvs.clone(), "progress")
            .build()
            .expect("list")
    };
    let original = build();
    assert!(original.fetch_next().await.expect("fetch").is_some());
    original.persist().await.expect("persist");
    client.add_status(
        "https://example.com/resume.xml",
        StatusCode::INTERNAL_SERVER_ERROR,
        "",
        1,
    );

    assert_eq!(drain(&build()).await.len(), 3);
}

#[tokio::test]
async fn transient_load_failure_is_retried() {
    let client = Arc::new(FakeHttpClient::default());
    client.add("https://example.com/sitemap.xml", urlset(0, 3), 8);
    let inner = Arc::new(MemoryKeyValueStore::new("load-retry"));
    let original = SitemapRequestListBuilder::default()
        .sitemap_url(Url::parse("https://example.com/sitemap.xml").expect("URL"))
        .http_client(client.clone())
        .persist(inner.clone(), "progress")
        .build()
        .expect("list");
    assert!(original.fetch_next().await.expect("fetch").is_some());
    original.persist().await.expect("persist");

    let faulty: Arc<dyn KeyValueStore> = Arc::new(FaultyKvs::new(inner, true, false));
    let resumed = SitemapRequestListBuilder::default()
        .sitemap_url(Url::parse("https://example.com/sitemap.xml").expect("URL"))
        .http_client(client)
        .persist(faulty, "progress")
        .build()
        .expect("list");
    assert!(resumed.fetch_next().await.is_err());
    assert_eq!(
        resumed
            .fetch_next()
            .await
            .expect("retried load")
            .expect("request")
            .url
            .as_str(),
        "https://example.com/1?a=1&b=2"
    );
}

#[tokio::test]
async fn automatic_checkpoint_failure_does_not_drop_the_request() {
    let client = Arc::new(FakeHttpClient::default());
    client.add("https://example.com/sitemap.xml", urlset(0, 101), 32);
    let faulty: Arc<dyn KeyValueStore> = Arc::new(FaultyKvs::new(
        Arc::new(MemoryKeyValueStore::new("set-failure")),
        false,
        true,
    ));
    let list = SitemapRequestListBuilder::default()
        .sitemap_url(Url::parse("https://example.com/sitemap.xml").expect("URL"))
        .http_client(client)
        .persist(faulty, "progress")
        .build()
        .expect("list");
    for index in 0..101 {
        let request = list
            .fetch_next()
            .await
            .expect("checkpoint failure is best effort")
            .expect("request");
        assert!(request.url.as_str().contains(&format!("/{index}?")));
    }
    assert_eq!(list.processed_count().await, 101);
}

#[tokio::test]
async fn cancellation_during_checkpoint_preserves_the_pending_request() {
    let client = Arc::new(FakeHttpClient::default());
    client.add("https://example.com/sitemap.xml", urlset(0, 101), 32);
    let kvs = Arc::new(BlockingSetKvs {
        inner: Arc::new(MemoryKeyValueStore::new("cancel-checkpoint")),
        block_first: AtomicBool::new(true),
        started: Notify::new(),
    });
    let list = Arc::new(
        SitemapRequestListBuilder::default()
            .sitemap_url(Url::parse("https://example.com/sitemap.xml").expect("URL"))
            .http_client(client)
            .persist(kvs.clone(), "progress")
            .build()
            .expect("list"),
    );
    for _ in 0..99 {
        assert!(list.fetch_next().await.expect("fetch").is_some());
    }
    let task_list = list.clone();
    let task = tokio::spawn(async move { task_list.fetch_next().await });
    kvs.started.notified().await;
    task.abort();
    assert!(
        task.await
            .expect_err("fetch should be cancelled")
            .is_cancelled()
    );
    assert_eq!(list.processed_count().await, 99);

    let request = list
        .fetch_next()
        .await
        .expect("retry checkpoint")
        .expect("preserved request");
    assert!(request.url.as_str().contains("/99?"));
    assert_eq!(list.processed_count().await, 100);
}

#[tokio::test]
async fn duplicate_locations_stay_deduplicated_across_resume() {
    let client = Arc::new(FakeHttpClient::default());
    let body = "<urlset><url><loc>https://example.com/a</loc></url><url><loc>https://example.com/a</loc></url><url><loc>https://example.com/b</loc></url></urlset>";
    client.add("https://example.com/sitemap.xml", body, 7);
    let kvs: Arc<dyn KeyValueStore> = Arc::new(MemoryKeyValueStore::new("dedup-resume"));
    let build = || {
        SitemapRequestListBuilder::default()
            .sitemap_url(Url::parse("https://example.com/sitemap.xml").expect("URL"))
            .http_client(client.clone())
            .persist(kvs.clone(), "progress")
            .build()
            .expect("list")
    };
    let original = build();
    assert_eq!(
        original
            .fetch_next()
            .await
            .expect("fetch")
            .expect("request")
            .url
            .as_str(),
        "https://example.com/a"
    );
    original.persist().await.expect("persist");
    assert_eq!(drain(&build()).await, vec!["https://example.com/b"]);
}

#[tokio::test]
async fn root_failure_status_survives_restart() {
    let client = Arc::new(FakeHttpClient::default());
    client.add_status(
        "https://example.com/one.xml",
        StatusCode::INTERNAL_SERVER_ERROR,
        "",
        1,
    );
    client.add("https://example.com/two.xml", urlset(0, 1), 8);
    let kvs: Arc<dyn KeyValueStore> = Arc::new(MemoryKeyValueStore::new("root-status"));
    let build = || {
        SitemapRequestListBuilder::default()
            .sitemap_urls([
                Url::parse("https://example.com/one.xml").expect("URL"),
                Url::parse("https://example.com/two.xml").expect("URL"),
            ])
            .http_client(client.clone())
            .persist(kvs.clone(), "progress")
            .build()
            .expect("list")
    };
    let original = build();
    assert!(original.fetch_next().await.expect("fetch").is_some());
    original.persist().await.expect("persist");
    client.add_status(
        "https://example.com/two.xml",
        StatusCode::INTERNAL_SERVER_ERROR,
        "",
        1,
    );
    assert!(build().fetch_next().await.is_err());
}

#[tokio::test]
async fn state_queries_are_in_memory_only_and_persist_loads_before_writing() {
    let client = Arc::new(FakeHttpClient::default());
    client.add("https://example.com/sitemap.xml", urlset(0, 4), 8);
    let kvs: Arc<dyn KeyValueStore> = Arc::new(MemoryKeyValueStore::new("lazy-load"));
    let build = || {
        SitemapRequestListBuilder::default()
            .sitemap_url(Url::parse("https://example.com/sitemap.xml").expect("URL"))
            .http_client(client.clone())
            .persist(kvs.clone(), "progress")
            .build()
            .expect("list")
    };
    let original = build();
    assert!(original.fetch_next().await.expect("fetch").is_some());
    assert!(original.fetch_next().await.expect("fetch").is_some());
    original.persist().await.expect("persist");

    let observer = build();
    assert_eq!(observer.processed_count().await, 0);
    assert!(!observer.is_finished().await);
    observer
        .persist()
        .await
        .expect("preserve loaded checkpoint");
    assert_eq!(drain(&build()).await.len(), 2);
}

#[tokio::test]
async fn persisted_state_has_the_strict_version_two_schema() {
    let client = Arc::new(FakeHttpClient::default());
    client.add("https://example.com/sitemap.xml", urlset(0, 2), 8);
    let kvs = Arc::new(MemoryKeyValueStore::new("state-shape"));
    let list = SitemapRequestListBuilder::default()
        .sitemap_url(Url::parse("https://example.com/sitemap.xml").expect("URL"))
        .http_client(client)
        .persist(kvs.clone(), "progress")
        .build()
        .expect("list");
    assert!(list.fetch_next().await.expect("fetch").is_some());
    list.persist().await.expect("persist");

    let entry = kvs
        .get_bytes("progress")
        .await
        .expect("read state")
        .expect("stored state");
    let value: serde_json::Value = serde_json::from_slice(&entry.value).expect("JSON state");
    let object = value.as_object().expect("state object");
    assert_eq!(object.get("version"), Some(&serde_json::json!(2)));
    let mut fields = object.keys().map(String::as_str).collect::<Vec<_>>();
    fields.sort_unstable();
    assert_eq!(
        fields,
        [
            "completed",
            "completed_failures",
            "emitted_in_progress",
            "emitted_total",
            "in_progress",
            "in_progress_depth",
            "pending",
            "pending_depths",
            "version",
        ]
    );
}

#[tokio::test]
async fn pending_depth_survives_restart_and_enforces_the_cap() {
    let client = Arc::new(FakeHttpClient::default());
    for depth in 0..4 {
        let current = format!("https://example.com/l{depth}.xml");
        let next = format!("https://example.com/l{}.xml", depth + 1);
        client.add(
            &current,
            format!("<sitemapindex><sitemap><loc>{next}</loc></sitemap></sitemapindex>"),
            9,
        );
    }
    client.add(
        "https://example.com/l4.xml",
        "<sitemapindex><sitemap><loc>https://example.com/content.xml</loc></sitemap><sitemap><loc>https://example.com/l5.xml</loc></sitemap></sitemapindex>",
        9,
    );
    client.add("https://example.com/content.xml", urlset(0, 1), 9);
    client.add(
        "https://example.com/l5.xml",
        "<sitemapindex><sitemap><loc>https://example.com/too-deep.xml</loc></sitemap></sitemapindex>",
        9,
    );
    client.add("https://example.com/too-deep.xml", urlset(10, 1), 9);
    let kvs: Arc<dyn KeyValueStore> = Arc::new(MemoryKeyValueStore::new("depth"));
    let build = || {
        SitemapRequestListBuilder::default()
            .sitemap_url(Url::parse("https://example.com/l0.xml").expect("URL"))
            .http_client(client.clone())
            .persist(kvs.clone(), "progress")
            .build()
            .expect("list")
    };
    let original = build();
    assert!(original.fetch_next().await.expect("fetch").is_some());
    original.persist().await.expect("persist");
    assert!(drain(&build()).await.is_empty());
}

#[tokio::test]
async fn pending_roots_keep_root_failure_semantics_after_restart() {
    let client = Arc::new(FakeHttpClient::default());
    client.add_status(
        "https://example.com/one.xml",
        StatusCode::INTERNAL_SERVER_ERROR,
        "",
        1,
    );
    client.add_status(
        "https://example.com/two.xml",
        StatusCode::INTERNAL_SERVER_ERROR,
        "",
        1,
    );
    let kvs: Arc<dyn KeyValueStore> = Arc::new(MemoryKeyValueStore::new("pending-roots"));
    let build = || {
        SitemapRequestListBuilder::default()
            .sitemap_urls([
                Url::parse("https://example.com/one.xml").expect("URL"),
                Url::parse("https://example.com/two.xml").expect("URL"),
            ])
            .http_client(client.clone())
            .persist(kvs.clone(), "progress")
            .build()
            .expect("list")
    };
    build().persist().await.expect("persist initial roots");
    assert!(build().fetch_next().await.is_err());
}
