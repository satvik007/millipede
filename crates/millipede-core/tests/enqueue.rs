//! Public URLs-only enqueue-linker behavior.

use millipede_core::prelude::*;
use millipede_storage_memory::MemoryStorageClient;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use url::Url;

use millipede_core::link_extraction::{
    EnqueueStrategy, ExtractedLink, GlobPattern, LinkExtractor, TransformResult, UrlMatch,
    UrlPattern,
};
use regex::Regex;

#[derive(Clone)]
struct FakeExtractor {
    links: Vec<ExtractedLink>,
    selectors: Arc<Mutex<Vec<Option<String>>>>,
}

impl FakeExtractor {
    fn new(links: Vec<ExtractedLink>) -> Self {
        Self {
            links,
            selectors: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait::async_trait]
impl LinkExtractor for FakeExtractor {
    async fn extract(&self, selector: Option<&str>) -> Result<Vec<ExtractedLink>, CrawlError> {
        self.selectors
            .lock()
            .unwrap()
            .push(selector.map(str::to_owned));
        Ok(self.links.clone())
    }
}

#[tokio::test]
async fn dyn_link_extractor_is_awaitable_through_arc() {
    let fixture = vec![ExtractedLink {
        url: "/fixture".to_owned(),
        base: None,
    }];
    let e: Arc<dyn LinkExtractor> = Arc::new(FakeExtractor::new(fixture.clone()));

    let extracted = e.extract(None).await.expect("extraction should succeed");

    assert_eq!(extracted.len(), fixture.len());
    assert_eq!(extracted[0].url, fixture[0].url);
}

#[tokio::test]
async fn genuinely_async_extractor_enqueues_and_receives_selector()
-> Result<(), Box<dyn std::error::Error>> {
    struct YieldingExtractor {
        selectors: Arc<Mutex<Vec<Option<String>>>>,
    }

    #[async_trait::async_trait]
    impl LinkExtractor for YieldingExtractor {
        async fn extract(&self, selector: Option<&str>) -> Result<Vec<ExtractedLink>, CrawlError> {
            self.selectors
                .lock()
                .unwrap()
                .push(selector.map(str::to_owned));
            tokio::task::yield_now().await;
            Ok(vec![ExtractedLink {
                url: "next".to_owned(),
                base: None,
            }])
        }
    }

    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler(|_: BasicContext| async { Ok(()) })
        .build()
        .await?;
    let parent = Request::get("https://example.test/catalog/page").build()?;
    let selectors = Arc::new(Mutex::new(Vec::new()));
    let result = EnqueueLinker::with_extractor(
        crawler.handle(),
        &parent,
        Arc::new(YieldingExtractor {
            selectors: selectors.clone(),
        }),
    )
    .options()
    .selector("a.next")
    .send()
    .await?;

    assert_eq!(result.added_count(), 1);
    assert_eq!(
        selectors.lock().unwrap().as_slice(),
        &[Some("a.next".into())]
    );
    let queue = crawler
        .handle()
        .request_queue()
        .expect("crawler queue should remain available");
    let lease = queue
        .fetch_next()
        .await?
        .expect("extracted link was enqueued");
    assert_eq!(
        lease.request.url.as_str(),
        "https://example.test/catalog/next"
    );
    Ok(())
}

fn storage() -> Arc<dyn StorageClient> {
    Arc::new(MemoryStorageClient::new())
}

#[tokio::test]
async fn explicit_urls_bypass_default_hostname_strategy() -> Result<(), Box<dyn std::error::Error>>
{
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler(|_: BasicContext| async { Ok(()) })
        .build()
        .await?;
    let parent = Request::get("https://parent.example/root").build()?;
    let external = Url::parse("https://external.example/selected")?;

    let result = EnqueueLinker::new(crawler.handle(), &parent)
        .urls([external.clone()])
        .await?;

    assert_eq!(result.added_count(), 1);
    assert!(result.skipped.is_empty());
    let queue = crawler
        .handle()
        .request_queue()
        .expect("crawler queue should remain available");
    let lease = queue
        .fetch_next()
        .await?
        .expect("external URL was enqueued");
    assert_eq!(lease.request.url, external);
    Ok(())
}

#[tokio::test]
async fn queue_duplicates_report_resolved_url_and_invoke_callback()
-> Result<(), Box<dyn std::error::Error>> {
    use millipede_core::link_extraction::CrawlPolicy;

    let callbacks = Arc::new(Mutex::new(Vec::new()));
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .crawl_policy(CrawlPolicy::new().on_skipped({
            let callbacks = callbacks.clone();
            move |url: &str, reason: &SkipReason| {
                callbacks
                    .lock()
                    .unwrap()
                    .push((url.to_owned(), reason.clone()));
            }
        }))
        .request_handler(|_: BasicContext| async { Ok(()) })
        .build()
        .await?;
    let parent = Request::get("http://example.local/dir/root").build()?;

    let first = EnqueueLinker::new(crawler.handle(), &parent)
        .options()
        .raw_urls(["child"])
        .send()
        .await?;
    assert_eq!(first.added_count(), 1);
    let second = EnqueueLinker::new(crawler.handle(), &parent)
        .options()
        .raw_urls(["child"])
        .send()
        .await?;

    assert_eq!(second.added_count(), 0);
    assert_eq!(second.skipped_count(), 1);
    assert_eq!(second.skipped[0].reason, SkipReason::DuplicateUniqueKey);
    assert_eq!(second.skipped[0].url, "http://example.local/dir/child");
    assert_eq!(
        callbacks.lock().unwrap().as_slice(),
        &[(
            "http://example.local/dir/child".to_owned(),
            SkipReason::DuplicateUniqueKey,
        )]
    );
    Ok(())
}

#[tokio::test]
async fn transform_recomputes_unchanged_unique_keys_and_queue_reports_collisions()
-> Result<(), Box<dyn std::error::Error>> {
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler(|_: BasicContext| async { Ok(()) })
        .build()
        .await?;
    let parent = Request::get("http://example.local/root").build()?;
    let canonical = Url::parse("http://example.local/canonical")?;

    let result = EnqueueLinker::new(crawler.handle(), &parent)
        .options()
        .raw_urls(["/one", "/two"])
        .transform(move |request| {
            let canonical = canonical.clone();
            Box::pin(async move {
                request.url = canonical;
                request.method = Method::POST;
                TransformResult::Enqueue
            })
        })
        .send()
        .await?;

    assert_eq!(result.added_count(), 1);
    assert_eq!(result.skipped_count(), 1);
    assert_eq!(result.skipped[0].reason, SkipReason::DuplicateUniqueKey);
    assert!(result.added[0].unique_key.starts_with("POST("));
    Ok(())
}

#[tokio::test]
async fn transform_preserves_an_explicit_unique_key() -> Result<(), Box<dyn std::error::Error>> {
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler(|_: BasicContext| async { Ok(()) })
        .build()
        .await?;
    let parent = Request::get("http://example.local/root").build()?;

    let result = EnqueueLinker::new(crawler.handle(), &parent)
        .options()
        .raw_urls(["/one"])
        .transform(|request| {
            Box::pin(async move {
                request.url = Url::parse("http://example.local/rewritten").unwrap();
                request.method = Method::POST;
                request.unique_key = "explicit-canonical-key".into();
                TransformResult::Enqueue
            })
        })
        .send()
        .await?;

    assert_eq!(result.added_count(), 1);
    assert_eq!(result.added[0].unique_key, "explicit-canonical-key");
    Ok(())
}

#[tokio::test]
async fn queue_duplicates_consume_candidate_limit_slots() -> Result<(), Box<dyn std::error::Error>>
{
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler(|_: BasicContext| async { Ok(()) })
        .build()
        .await?;
    let parent = Request::get("http://example.local/root").build()?;
    EnqueueLinker::new(crawler.handle(), &parent)
        .options()
        .raw_urls(["/duplicate-a", "/duplicate-b"])
        .send()
        .await?;

    let result = EnqueueLinker::new(crawler.handle(), &parent)
        .options()
        .raw_urls([
            "/duplicate-a",
            "/duplicate-b",
            "/new-a",
            "/new-b",
            "/new-c",
            "/beyond-limit",
        ])
        .limit(3)
        .send()
        .await?;

    assert_eq!(result.added_count(), 1);
    assert_eq!(result.skipped_count(), 2);
    assert!(
        result
            .skipped
            .iter()
            .all(|skip| skip.reason == SkipReason::DuplicateUniqueKey)
    );
    Ok(())
}

#[tokio::test]
async fn urls_deduplicate_and_increment_depth() -> Result<(), Box<dyn std::error::Error>> {
    let result = Arc::new(Mutex::new(None));
    let child_depth = Arc::new(Mutex::new(None));
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler({
            let result = result.clone();
            let child_depth = child_depth.clone();
            move |ctx: BasicContext| {
                let result = result.clone();
                let child_depth = child_depth.clone();
                async move {
                    if ctx.request.url.path() == "/root" {
                        let linker = EnqueueLinker::new(ctx.crawler.clone(), &ctx.request);
                        *result.lock().unwrap() = Some(
                            linker
                                .urls([
                                    Url::parse("http://example.local/a").unwrap(),
                                    Url::parse("http://example.local/a").unwrap(),
                                    Url::parse("http://example.local/b").unwrap(),
                                ])
                                .await?,
                        );
                    } else {
                        *child_depth.lock().unwrap() = Some(ctx.request.crawl_depth);
                    }
                    Ok(())
                }
            }
        })
        .build()
        .await?;
    let stats = crawler.run(["http://example.local/root"]).await?;
    let result = result.lock().unwrap().take().unwrap();
    assert_eq!(result.added_count(), 2);
    assert_eq!(result.skipped_count(), 0);
    assert_eq!(*child_depth.lock().unwrap(), Some(1));
    assert_eq!(stats.requests_finished, 3);
    Ok(())
}

#[tokio::test]
async fn raw_urls_resolve_against_parent_and_report_invalid()
-> Result<(), Box<dyn std::error::Error>> {
    let result = Arc::new(Mutex::new(None));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler({
            let result = result.clone();
            let seen = seen.clone();
            move |ctx: BasicContext| {
                let result = result.clone();
                let seen = seen.clone();
                async move {
                    if ctx.request.url.path() == "/dir/index" {
                        let linker = EnqueueLinker::new(ctx.crawler.clone(), &ctx.request);
                        *result.lock().unwrap() = Some(
                            linker
                                .options()
                                .raw_urls(["a", "/b", "http://other.local/c", "::bad::"])
                                .send()
                                .await?,
                        );
                    } else {
                        seen.lock().unwrap().push(ctx.request.url.clone());
                    }
                    Ok(())
                }
            }
        })
        .build()
        .await?;
    let stats = crawler.run(["http://example.local/dir/index"]).await?;
    let result = result.lock().unwrap().take().unwrap();
    assert_eq!(result.added_count(), 2);
    assert_eq!(result.skipped_count(), 2);
    assert!(
        result
            .skipped
            .iter()
            .any(|skip| skip.reason == SkipReason::InvalidUrl && skip.url == "::bad::")
    );
    assert!(result.skipped.iter().any(|skip| {
        skip.reason == SkipReason::StrategyExcluded && skip.url == "http://other.local/c"
    }));
    let seen: Vec<_> = seen
        .lock()
        .unwrap()
        .iter()
        .map(ToString::to_string)
        .collect();
    assert!(seen.contains(&"http://example.local/dir/a".to_owned()));
    assert!(seen.contains(&"http://example.local/b".to_owned()));
    assert!(!seen.contains(&"http://other.local/c".to_owned()));
    assert_eq!(stats.requests_finished, 3);
    Ok(())
}

#[tokio::test]
async fn base_url_override_is_used() -> Result<(), Box<dyn std::error::Error>> {
    let observed = Arc::new(Mutex::new(false));
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler({
            let observed = observed.clone();
            move |ctx: BasicContext| {
                let observed = observed.clone();
                async move {
                    if ctx.request.url.path() == "/root" {
                        EnqueueLinker::new(ctx.crawler.clone(), &ctx.request)
                            .options()
                            .raw_urls(["x"])
                            .base_url(Url::parse("http://base.local/sub/").unwrap())
                            .strategy(EnqueueStrategy::All)
                            .send()
                            .await?;
                    } else {
                        *observed.lock().unwrap() =
                            ctx.request.url.as_str() == "http://base.local/sub/x";
                    }
                    Ok(())
                }
            }
        })
        .build()
        .await?;
    crawler.run(["http://example.local/root"]).await?;
    assert!(*observed.lock().unwrap());
    Ok(())
}

#[tokio::test]
async fn limit_is_a_silent_cap() -> Result<(), Box<dyn std::error::Error>> {
    let result = Arc::new(Mutex::new(None));
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler({
            let result = result.clone();
            move |ctx: BasicContext| {
                let result = result.clone();
                async move {
                    if ctx.request.url.path() == "/root" {
                        let linker = EnqueueLinker::new(ctx.crawler.clone(), &ctx.request);
                        let urls = (0..5)
                            .map(|n| Url::parse(&format!("http://example.local/{n}")).unwrap());
                        *result.lock().unwrap() =
                            Some(linker.options().urls(urls).limit(2).send().await?);
                    }
                    Ok(())
                }
            }
        })
        .build()
        .await?;
    let stats = crawler.run(["http://example.local/root"]).await?;
    let result = result.lock().unwrap().take().unwrap();
    assert_eq!(result.added_count(), 2);
    assert!(result.skipped.is_empty());
    assert_eq!(stats.requests_finished, 3);
    Ok(())
}

#[tokio::test]
async fn limit_caps_transform_invocations_after_url_deduplication()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler(|_: BasicContext| async { Ok(()) })
        .build()
        .await?;
    let parent = Request::get("http://example.local/root").build()?;
    let result = EnqueueLinker::new(crawler.handle(), &parent)
        .options()
        .raw_urls(["/one", "/one", "/two", "/three", "/four"])
        .limit(3)
        .transform({
            let calls = calls.clone();
            move |_request| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { TransformResult::Enqueue })
            }
        })
        .send()
        .await?;

    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(result.added_count(), 3);
    Ok(())
}

#[tokio::test]
async fn limit_preserves_interleaved_candidate_call_order() -> Result<(), Box<dyn std::error::Error>>
{
    let seen = Arc::new(Mutex::new(Vec::new()));
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler({
            let seen = seen.clone();
            move |ctx: BasicContext| {
                let seen = seen.clone();
                async move {
                    if ctx.request.url.path() == "/dir/root" {
                        EnqueueLinker::new(ctx.crawler.clone(), &ctx.request)
                            .options()
                            .raw_urls(["first"])
                            .urls([Url::parse("http://example.local/second").unwrap()])
                            .limit(1)
                            .send()
                            .await?;
                    } else {
                        seen.lock().unwrap().push(ctx.request.url.clone());
                    }
                    Ok(())
                }
            }
        })
        .build()
        .await?;
    let stats = crawler.run(["http://example.local/dir/root"]).await?;
    assert_eq!(
        *seen.lock().unwrap(),
        vec![Url::parse("http://example.local/dir/first")?]
    );
    assert_eq!(stats.requests_finished, 2);
    Ok(())
}

#[tokio::test]
async fn label_and_user_data_are_explicit_and_not_inherited()
-> Result<(), Box<dyn std::error::Error>> {
    let observations = Arc::new(Mutex::new(HashMap::new()));
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler({
            let observations = observations.clone();
            move |ctx: BasicContext| {
                let observations = observations.clone();
                async move {
                    if ctx.request.url.path() == "/root" {
                        let linker = EnqueueLinker::new(ctx.crawler.clone(), &ctx.request);
                        let mut data = UserData::default();
                        data.set_typed("id", &7_u32)?;
                        linker
                            .options()
                            .urls([Url::parse("http://example.local/labeled").unwrap()])
                            .label("detail")
                            .user_data(data)
                            .send()
                            .await?;
                        linker
                            .urls([Url::parse("http://example.local/plain").unwrap()])
                            .await?;
                    } else {
                        observations.lock().unwrap().insert(
                            ctx.request.url.path().to_owned(),
                            (
                                ctx.request.label.clone(),
                                ctx.request.user_data.get_typed::<u32>("id").transpose()?,
                            ),
                        );
                    }
                    Ok(())
                }
            }
        })
        .build()
        .await?;
    let root = Request::get("http://example.local/root")
        .label("parent")
        .build()?;
    crawler.run([root]).await?;
    let observations = observations.lock().unwrap();
    assert_eq!(observations.len(), 2);
    assert_eq!(
        observations.get("/labeled"),
        Some(&(Some("detail".into()), Some(7)))
    );
    assert_eq!(observations.get("/plain"), Some(&(None, None)));
    Ok(())
}

#[tokio::test]
async fn forefront_children_run_before_normal_children() -> Result<(), Box<dyn std::error::Error>> {
    let order = Arc::new(Mutex::new(Vec::new()));
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .max_concurrency(1)
        .request_handler({
            let order = order.clone();
            move |ctx: BasicContext| {
                let order = order.clone();
                async move {
                    match ctx.request.url.path() {
                        "/root" => {
                            let linker = EnqueueLinker::new(ctx.crawler.clone(), &ctx.request);
                            linker
                                .urls([Url::parse("http://example.local/a").unwrap()])
                                .await?;
                            linker
                                .options()
                                .urls([Url::parse("http://example.local/b").unwrap()])
                                .forefront(true)
                                .send()
                                .await?;
                        }
                        path => order.lock().unwrap().push(path.to_owned()),
                    }
                    Ok(())
                }
            }
        })
        .build()
        .await?;
    crawler.run(["http://example.local/root"]).await?;
    assert_eq!(*order.lock().unwrap(), vec!["/b", "/a"]);
    Ok(())
}

#[tokio::test]
async fn dead_crawler_error_propagates() -> Result<(), Box<dyn std::error::Error>> {
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler(|_: BasicContext| async { Ok(()) })
        .build()
        .await?;
    let parent = Request::get("http://example.local/root").build()?;
    let linker = EnqueueLinker::new(crawler.handle(), &parent);
    drop(crawler);
    assert!(
        linker
            .urls([Url::parse("http://example.local/a")?])
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn selector_requires_an_extractor() -> Result<(), Box<dyn std::error::Error>> {
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler(|_: BasicContext| async { Ok(()) })
        .build()
        .await?;
    let parent = Request::get("http://example.local/root").build()?;
    let error = EnqueueLinker::new(crawler.handle(), &parent)
        .options()
        .selector("a.product")
        .send()
        .await
        .expect_err("selector use without HTML must fail");
    assert!(
        error
            .to_string()
            .contains("requires an HTML or browser context")
    );
    Ok(())
}

#[tokio::test]
async fn extractor_conveniences_apply_strategy_and_default_selector()
-> Result<(), Box<dyn std::error::Error>> {
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler(|_: BasicContext| async { Ok(()) })
        .build()
        .await?;
    let parent = Request::get("https://shop.example.com/root").build()?;
    let extractor = FakeExtractor::new(vec![
        ExtractedLink {
            url: "/same".into(),
            base: None,
        },
        ExtractedLink {
            url: "https://other.example.com/domain".into(),
            base: None,
        },
        ExtractedLink {
            url: "https://outside.test/no".into(),
            base: None,
        },
    ]);
    let selectors = extractor.selectors.clone();
    let same_hostname =
        EnqueueLinker::with_extractor(crawler.handle(), &parent, Arc::new(extractor.clone()))
            .same_hostname()
            .await?;
    assert_eq!(same_hostname.added_count(), 1);
    assert_eq!(same_hostname.skipped_count(), 2);
    assert!(
        same_hostname
            .skipped
            .iter()
            .all(|skip| skip.reason == SkipReason::StrategyExcluded)
    );

    let domain = EnqueueLinker::with_extractor(crawler.handle(), &parent, Arc::new(extractor))
        .same_domain()
        .await?;
    assert_eq!(domain.added_count(), 1); // `/same` is now a queue duplicate.
    let all = EnqueueLinker::with_extractor(
        crawler.handle(),
        &parent,
        Arc::new(FakeExtractor {
            links: vec![
                ExtractedLink {
                    url: "/same".into(),
                    base: None,
                },
                ExtractedLink {
                    url: "https://other.example.com/domain".into(),
                    base: None,
                },
                ExtractedLink {
                    url: "https://outside.test/no".into(),
                    base: None,
                },
            ],
            selectors: selectors.clone(),
        }),
    )
    .all()
    .await?;
    assert_eq!(all.added_count(), 1);
    EnqueueLinker::with_extractor(
        crawler.handle(),
        &parent,
        Arc::new(FakeExtractor {
            links: Vec::new(),
            selectors: selectors.clone(),
        }),
    )
    .options()
    .selector("a.product")
    .send()
    .await?;
    assert_eq!(
        selectors.lock().unwrap().as_slice(),
        &[None, None, None, Some("a.product".into())],
        "convenience methods use the default selector and options forwards an override"
    );
    Ok(())
}

#[tokio::test]
async fn patterns_overrides_transform_dedupe_and_counts() -> Result<(), Box<dyn std::error::Error>>
{
    let observed = Arc::new(Mutex::new(HashMap::new()));
    let enqueue_result = Arc::new(Mutex::new(None));
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .request_handler({
            let observed = observed.clone();
            let enqueue_result = enqueue_result.clone();
            move |ctx: BasicContext| {
                let observed = observed.clone();
                let enqueue_result = enqueue_result.clone();
                async move {
                    if ctx.request.url.path() == "/root" {
                        let mut override_data = UserData::default();
                        override_data.set_typed("kind", &"product")?;
                        let mut headers = HeaderMap::new();
                        headers.insert("x-pattern", "yes".parse().unwrap());
                        let include: GlobPattern = UrlMatch::new("**/products/*")
                            .label("pattern")
                            .user_data(override_data)
                            .method(Method::POST)
                            .headers(headers)
                            .into();
                        let result = EnqueueLinker::new(ctx.crawler.clone(), &ctx.request)
                            .options()
                            .raw_urls([
                                "/products/one",
                                "/products/one",
                                "/products/two",
                                "/news/keep",
                                "/news/reject",
                                "/ignored",
                            ])
                            .strategy(EnqueueStrategy::SameHostname)
                            .globs([include])
                            .regex([Regex::new(r"/news/(keep|reject)$").unwrap()])
                            .exclude([UrlPattern::from("**/products/two")])
                            .label("fallback")
                            .transform(|request| {
                                Box::pin(async move {
                                    if request.url.path() == "/news/reject" {
                                        return TransformResult::Skip {
                                            reason: "news disabled".into(),
                                        };
                                    }
                                    if request.url.path() == "/news/keep" {
                                        request.label = Some("transformed".into());
                                    }
                                    TransformResult::Enqueue
                                })
                            })
                            .send()
                            .await?;
                        *enqueue_result.lock().unwrap() = Some(result);
                    } else {
                        observed.lock().unwrap().insert(
                            ctx.request.url.path().to_owned(),
                            (
                                ctx.request.label.clone(),
                                ctx.request
                                    .user_data
                                    .get_typed::<String>("kind")
                                    .transpose()?,
                                ctx.request.method.clone(),
                                ctx.request.headers.get("x-pattern").cloned(),
                            ),
                        );
                    }
                    Ok(())
                }
            }
        })
        .build()
        .await?;
    crawler.run(["http://example.local/root"]).await?;
    let result = enqueue_result.lock().unwrap().take().unwrap();
    assert_eq!(result.added_count(), 2);
    assert_eq!(result.skipped_count(), 3, "{:#?}", result.skipped);
    assert!(result.skipped.iter().any(|skip| {
        matches!(
            &skip.reason,
            SkipReason::TransformRejected { reason } if reason == "news disabled"
        )
    }));
    assert!(
        result
            .skipped
            .iter()
            .any(|skip| skip.url.ends_with("/products/two")
                && skip.reason == SkipReason::GlobExcluded)
    );
    let observed = observed.lock().unwrap();
    let product = observed.get("/products/one").cloned().unwrap();
    assert_eq!(product.0.as_deref(), Some("pattern"));
    assert_eq!(product.1.as_deref(), Some("product"));
    assert_eq!(product.2, Method::POST);
    assert_eq!(product.3.unwrap(), "yes");
    let news = observed.get("/news/keep").cloned().unwrap();
    assert_eq!(news.0.as_deref(), Some("transformed"));
    assert_eq!(news.1, None);
    assert_eq!(news.2, Method::GET);
    assert_eq!(news.3, None);
    Ok(())
}

#[tokio::test]
async fn regex_only_include_and_depth_limit_report_reasons()
-> Result<(), Box<dyn std::error::Error>> {
    use millipede_core::link_extraction::CrawlPolicy;

    let result = Arc::new(Mutex::new(None));
    let crawler = Crawler::builder(BasicKind)
        .storage_client(storage())
        .crawl_policy(CrawlPolicy::new().max_crawl_depth(0))
        .request_handler({
            let result = result.clone();
            move |ctx: BasicContext| {
                let result = result.clone();
                async move {
                    *result.lock().unwrap() = Some(
                        EnqueueLinker::new(ctx.crawler.clone(), &ctx.request)
                            .options()
                            .raw_urls(["/accepted", "/wrong"])
                            .regex([Regex::new(r"/accepted$").unwrap()])
                            .send()
                            .await?,
                    );
                    Ok(())
                }
            }
        })
        .build()
        .await?;
    crawler.run(["http://example.local/root"]).await?;
    let result = result.lock().unwrap().take().unwrap();
    assert_eq!(result.added_count(), 0);
    assert_eq!(result.skipped_count(), 2);
    assert!(
        result
            .skipped
            .iter()
            .any(|skip| { skip.reason == SkipReason::MaxDepthExceeded { depth: 1, limit: 0 } })
    );
    assert!(
        result
            .skipped
            .iter()
            .any(|skip| skip.reason == SkipReason::RegexExcluded)
    );
    Ok(())
}
