//! Public URLs-only enqueue-linker behavior.

use millipede_core::prelude::*;
use millipede_storage_memory::MemoryStorageClient;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use url::Url;

fn storage() -> Arc<dyn StorageClient> {
    Arc::new(MemoryStorageClient::new())
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
    assert_eq!(result.skipped_count(), 1);
    assert_eq!(result.skipped[0].reason, SkipReason::DuplicateUniqueKey);
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
    assert_eq!(result.added_count(), 3);
    assert_eq!(result.skipped_count(), 1);
    assert_eq!(result.skipped[0].reason, SkipReason::InvalidUrl);
    assert_eq!(result.skipped[0].url, "::bad::");
    let seen: Vec<_> = seen
        .lock()
        .unwrap()
        .iter()
        .map(ToString::to_string)
        .collect();
    assert!(seen.contains(&"http://example.local/dir/a".to_owned()));
    assert!(seen.contains(&"http://example.local/b".to_owned()));
    assert!(seen.contains(&"http://other.local/c".to_owned()));
    assert_eq!(stats.requests_finished, 4);
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
