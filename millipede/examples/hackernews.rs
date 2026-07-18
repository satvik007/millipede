//! Crawls Hacker News front pages and item pages into typed story and comment records.
//!
//! Run with: `cargo run -p millipede --example hackernews --all-features`
//!
//! This example hits the live Hacker News site. It is intentionally polite: the crawl is capped
//! at 30 requests and waits at least 500 ms between requests to the same domain. Set
//! `HACKERNEWS_START_URL` to retarget it or `HACKERNEWS_MAX_REQUESTS` to lower the cap.

use std::{sync::Arc, time::Duration};

use millipede::{
    CrawlPolicy, DatasetExt, EnqueueStrategy, HtmlContext, HtmlCrawler, HtmlKind, ListOptions,
    MemoryStorageClient, Request, Router, StorageClient,
};
use millipede_html::scraper::ElementRef;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// IMPORTANT: millipede/tests/hackernews_mock.rs mirrors these selectors and must be updated in
// lockstep whenever Hacker News markup handling changes.
millipede_html::selectors! {
    story_row_selector = "tr.athing";
    rank_selector = "span.rank";
    title_link_selector = "span.titleline > a";
    subtext_selector = "td.subtext";
    score_selector = "span.score";
    author_selector = "a.hnuser";
    item_link_selector = "td.subtext a[href^='item?id=']";
    comment_row_selector = "tr.athing.comtr";
    indent_selector = "td.ind";
    indent_image_selector = "img";
    comment_text_selector = "div.commtext, span.commtext";
}

#[derive(Debug, Serialize, Deserialize)]
struct Story {
    rank: u32,
    id: Option<String>,
    title: String,
    url: Option<String>,
    points: Option<u32>,
    author: Option<String>,
    comment_count: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Comment {
    story_id: String,
    author: Option<String>,
    text: String,
    indent_depth: u32,
}

fn trimmed_text(element: ElementRef<'_>) -> String {
    element.text().collect::<String>().trim().to_owned()
}

fn leading_number(value: &str) -> Option<u32> {
    value.split_whitespace().next()?.parse().ok()
}

fn story_from_row(row: ElementRef<'_>) -> Option<Story> {
    let rank_text = match row.select(rank_selector()).next() {
        Some(element) => trimmed_text(element),
        None => {
            eprintln!("skipping story row without span.rank");
            return None;
        }
    };
    let rank = match rank_text.trim_end_matches('.').parse() {
        Ok(rank) => rank,
        Err(error) => {
            eprintln!("skipping story row with invalid rank {rank_text:?}: {error}");
            return None;
        }
    };

    let title_link = match row.select(title_link_selector()).next() {
        Some(element) => element,
        None => {
            eprintln!("skipping rank {rank} without span.titleline > a");
            return None;
        }
    };
    let title = trimmed_text(title_link);
    if title.is_empty() {
        eprintln!("skipping rank {rank} with an empty title");
        return None;
    }
    let url = title_link.value().attr("href").map(str::to_owned);
    let id = row.value().attr("id").map(str::to_owned);
    if id.is_none() {
        eprintln!("rank {rank} has no story id; storing it as None");
    }

    let mut subtext = None;
    for sibling in row.next_siblings() {
        if let Some(element) = ElementRef::wrap(sibling) {
            if element.value().name() == "tr"
                && element.value().attr("class").is_some_and(|classes| {
                    classes.split_whitespace().any(|class| class == "athing")
                })
            {
                eprintln!("skipping rank {rank}: reached the next tr.athing before td.subtext");
                return None;
            }
            if let Some(found) = element.select(subtext_selector()).next() {
                subtext = Some(found);
                break;
            }
        }
    }

    let (points, author, comment_count) = match subtext {
        Some(element) => {
            let points = element
                .select(score_selector())
                .next()
                .and_then(|score| leading_number(&trimmed_text(score)));
            let author = element.select(author_selector()).next().and_then(|author| {
                let author = trimmed_text(author);
                if author.is_empty() {
                    None
                } else {
                    Some(author)
                }
            });
            let comment_count = element
                .select(item_link_selector())
                .last()
                .and_then(|link| leading_number(&trimmed_text(link)));
            (points, author, comment_count)
        }
        None => {
            eprintln!("rank {rank} has no following td.subtext; optional metadata is empty");
            (None, None, None)
        }
    };

    Some(Story {
        rank,
        id,
        title,
        url,
        points,
        author,
        comment_count,
    })
}

fn comment_depth(row: ElementRef<'_>) -> u32 {
    let Some(indent) = row.select(indent_selector()).next() else {
        eprintln!("comment row has no td.ind; using depth 0");
        return 0;
    };

    if let Some(depth) = indent
        .value()
        .attr("indent")
        .and_then(|value| value.parse::<u32>().ok())
    {
        return depth;
    }

    if let Some(width) = indent
        .select(indent_image_selector())
        .next()
        .and_then(|image| image.value().attr("width"))
        .and_then(|value| value.parse::<u32>().ok())
    {
        return width / 40;
    }

    eprintln!("comment row has no usable indent or image width; using depth 0");
    0
}

fn comments_from_document(ctx: &HtmlContext, story_id: &str) -> Vec<Comment> {
    ctx.html
        .select(comment_row_selector(), |row| {
            let author = row.select(author_selector()).next().and_then(|author| {
                let author = trimmed_text(author);
                if author.is_empty() {
                    None
                } else {
                    Some(author)
                }
            });
            let text = row.select(comment_text_selector()).next().map(trimmed_text);
            (author, text, comment_depth(row))
        })
        .into_iter()
        .filter_map(|(author, text, indent_depth)| match text {
            Some(text) if !text.is_empty() => Some(Comment {
                story_id: story_id.to_owned(),
                author,
                text,
                indent_depth,
            }),
            _ => {
                eprintln!("skipping comment row without non-empty div.commtext or span.commtext");
                None
            }
        })
        .collect()
}

fn max_requests() -> u64 {
    match std::env::var("HACKERNEWS_MAX_REQUESTS") {
        Ok(value) => match value.parse::<u64>() {
            Ok(limit) => limit.min(30),
            Err(error) => {
                eprintln!("invalid HACKERNEWS_MAX_REQUESTS={value:?}: {error}; using 30");
                30
            }
        },
        Err(_) => 30,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let storage = Arc::new(MemoryStorageClient::new());
    let router = Router::<HtmlContext>::new()
        .route("front", |ctx: HtmlContext| async move {
            let stories = ctx.html.select(story_row_selector(), story_from_row);
            for story in stories.into_iter().flatten() {
                ctx.storage.dataset().push(&story).await?;
            }

            let _ = ctx
                .enqueue
                .options()
                .selector("a.morelink")
                .strategy(EnqueueStrategy::SameHostname)
                .label("front")
                .send()
                .await?;
            let _ = ctx
                .enqueue
                .options()
                .selector("td.subtext a[href^='item?id=']")
                .strategy(EnqueueStrategy::SameHostname)
                .label("item")
                .limit(3)
                .send()
                .await?;
            Ok(())
        })
        .route("item", |ctx: HtmlContext| async move {
            let story_id = ctx
                .request
                .url
                .query_pairs()
                .find_map(|(key, value)| (key == "id").then(|| value.into_owned()));
            let Some(story_id) = story_id else {
                eprintln!(
                    "skipping item page without an id query parameter: {}",
                    ctx.request.url
                );
                return Ok(());
            };

            for comment in comments_from_document(&ctx, &story_id) {
                ctx.storage.dataset().push(&comment).await?;
            }
            Ok(())
        });

    let crawler = HtmlCrawler::builder(HtmlKind::new()?)
        .storage_client(storage.clone())
        .crawl_policy(CrawlPolicy::new().max_requests_per_crawl(max_requests()))
        .same_domain_delay(Duration::from_millis(500))
        .max_concurrency(2)
        .max_request_retries(2)
        .request_handler(router)
        .build()
        .await?;

    let start_url = std::env::var("HACKERNEWS_START_URL")
        .unwrap_or_else(|_| "https://news.ycombinator.com/news".to_owned());
    let seed = Request::get(start_url).label("front").build()?;
    let stats = crawler.run(seed).await?;

    let dataset = storage.open_dataset(None).await?;
    let records = dataset.list::<Value>(ListOptions::default()).await?.items;
    let mut stories = Vec::new();
    let mut comment_count = 0_usize;
    let mut max_depth = 0_u32;
    for record in records {
        if record.get("rank").is_some() {
            let title = record.get("title").and_then(Value::as_str);
            let points = record.get("points").and_then(Value::as_u64);
            match title {
                Some(title) => stories.push((title.to_owned(), points)),
                None => eprintln!("skipping malformed stored story without a title"),
            }
        } else if record.get("story_id").is_some() {
            comment_count += 1;
            if let Some(depth) = record.get("indent_depth").and_then(Value::as_u64) {
                max_depth = max_depth.max(u32::try_from(depth).unwrap_or(u32::MAX));
            }
        }
    }

    println!("{} stories", stories.len());
    for (title, points) in stories.iter().take(5) {
        println!("- {title} ({} points)", points.unwrap_or(0));
    }
    println!("{comment_count} comments, max depth {max_depth}");
    println!("crawl statistics: {stats:#?}");

    if stories.is_empty() {
        eprintln!("no stories parsed; Hacker News selectors may have drifted");
        std::process::exit(1);
    }
    Ok(())
}
