//! Local, network-free end-to-end coverage for the Hacker News example.

#![cfg(all(feature = "http", feature = "html", feature = "storage-memory"))]
#![allow(missing_docs)]

use std::{collections::BTreeMap, sync::Arc};

use millipede::{
    CrawlPolicy, DatasetExt, EnqueueStrategy, HtmlContext, HtmlCrawler, HtmlKind, ListOptions,
    MemoryStorageClient, Request, Router, StorageClient,
};
use millipede_html::scraper::ElementRef;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::path};

// IMPORTANT: millipede/examples/hackernews.rs mirrors these selectors and must be updated in
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

fn story_rows(stories: &[(&str, u32, &str, u32, &str, u32)], external_origin: &str) -> String {
    stories
        .iter()
        .map(|(id, rank, title, points, author, comments)| {
            let item_id = if rank % 2 == 0 { "102" } else { "101" };
            // The extra class makes this external title link an enqueue candidate, so the test
            // exercises hostname filtering instead of excluding it accidentally by selector.
            format!(
                "<tr class='athing' id='{id}'><td><span class='rank'>{rank}.</span></td>\
                 <td class='title'><span class='titleline'><a class='morelink' \
                 href='{external_origin}/external/{id}'>\
                 {title}</a></span></td></tr>\
                 <tr><td></td><td class='subtext'><span class='score'>{points} points</span> by \
                 <a class='hnuser'>{author}</a> | <a href='item?id={item_id}'>{comments} comments</a>\
                 </td></tr>"
            )
        })
        .collect()
}

fn front_fixture(external_origin: &str) -> String {
    format!(
        "<html><body><table>\
         <tr class='athing' id='missing-subtext'><td><span class='rank'>99.</span></td>\
         <td class='title'><span class='titleline'><a href='/missing'>Missing subtext</a></span></td></tr>\
         {}<tr><td><a class='morelink' href='/news?p=2'>More</a></td></tr>\
         </table></body></html>",
        story_rows(
            &[
                ("501", 1, "Alpha compiler", 120, "alice", 12),
                ("502", 2, "Beta database", 85, "bob", 7),
                ("503", 3, "Gamma protocol", 42, "carol", 3),
            ],
            external_origin,
        )
    )
}

fn second_page_fixture(external_origin: &str) -> String {
    format!(
        "<html><body><table>{}</table></body></html>",
        story_rows(
            &[
                ("504", 4, "Delta runtime", 31, "dave", 2),
                ("505", 5, "Epsilon storage", 17, "erin", 1),
            ],
            external_origin,
        )
    )
}

fn item_fixture(comments: &[(&str, &str, Option<u32>, Option<u32>)]) -> String {
    let rows = comments
        .iter()
        .map(|(author, text, indent, width)| {
            let indent = indent.map_or_else(String::new, |value| format!(" indent='{value}'"));
            let image = width.map_or_else(String::new, |value| {
                format!("<img src='s.gif' width='{value}' height='1'>")
            });
            format!(
                "<tr class='athing comtr'><td class='ind'{indent}>{image}</td><td><a class='hnuser'>\
                 {author}</a><div class='comment'><span class='commtext'>{text}</span>\
                 <div class='reply'><a href='reply'>reply</a></div></div></td></tr>"
            )
        })
        .collect::<String>();
    format!("<html><body><table>{rows}</table></body></html>")
}

async fn mount_site(server: &MockServer) {
    let external_origin = server.uri().replace("127.0.0.1", "localhost");
    Mock::given(path("/news"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            front_fixture(&external_origin).into_bytes(),
            "text/html; charset=utf-8",
        ))
        .with_priority(2)
        .mount(server)
        .await;
    Mock::given(path("/news"))
        .and(wiremock::matchers::query_param("p", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            second_page_fixture(&external_origin).into_bytes(),
            "text/html; charset=utf-8",
        ))
        .with_priority(1)
        .mount(server)
        .await;
    Mock::given(path("/item"))
        .and(wiremock::matchers::query_param("id", "101"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(
                item_fixture(&[
                    ("alice", "Root comment", Some(0), None),
                    ("bob", "Nested comment", Some(2), None),
                ])
                .into_bytes(),
                "text/html; charset=utf-8",
            ),
        )
        .mount(server)
        .await;
    Mock::given(path("/item"))
        .and(wiremock::matchers::query_param("id", "102"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(
                item_fixture(&[
                    ("carol", "Width fallback", None, Some(120)),
                    ("dave", "Another root", Some(0), None),
                ])
                .into_bytes(),
                "text/html; charset=utf-8",
            ),
        )
        .mount(server)
        .await;
}

fn hackernews_router() -> Router<HtmlContext> {
    Router::<HtmlContext>::new()
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
        })
}

#[tokio::test]
async fn crawls_hackernews_routes_pagination_and_comments() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    mount_site(&server).await;
    let storage = Arc::new(MemoryStorageClient::new());
    let crawler = HtmlCrawler::builder(HtmlKind::new()?)
        .storage_client(storage.clone())
        .crawl_policy(CrawlPolicy::new().max_requests_per_crawl(10))
        .max_concurrency(2)
        .request_handler(hackernews_router())
        .build()
        .await?;
    let seed = Request::get(format!("{}/news", server.uri()))
        .label("front")
        .build()?;
    let stats = crawler.run(seed).await?;

    assert_eq!(stats.requests_finished, 4);
    assert_eq!(stats.requests_failed, 0);
    let records = storage
        .open_dataset(None)
        .await?
        .list::<Value>(ListOptions::default())
        .await?
        .items;
    let mut stories = BTreeMap::new();
    let mut comments = Vec::new();
    for record in records {
        if record.get("rank").is_some() {
            if let Some(id) = record.get("id").and_then(Value::as_str) {
                stories.insert(id.to_owned(), record);
            }
        } else if record.get("story_id").is_some() {
            comments.push(record);
        }
    }

    assert_eq!(stories.len(), 5, "the morelink page must be followed");
    let alpha = stories.get("501").expect("Alpha story");
    assert_eq!(
        alpha.get("title").and_then(Value::as_str),
        Some("Alpha compiler")
    );
    assert_eq!(alpha.get("points").and_then(Value::as_u64), Some(120));
    assert_eq!(alpha.get("comment_count").and_then(Value::as_u64), Some(12));
    let epsilon = stories.get("505").expect("paginated Epsilon story");
    assert_eq!(
        epsilon.get("title").and_then(Value::as_str),
        Some("Epsilon storage")
    );
    assert_eq!(epsilon.get("points").and_then(Value::as_u64), Some(17));
    assert_eq!(
        epsilon.get("comment_count").and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(comments.len(), 4);
    assert_eq!(
        comments
            .iter()
            .filter_map(|comment| comment.get("indent_depth").and_then(Value::as_u64))
            .max(),
        Some(3_u64)
    );
    assert!(
        comments
            .iter()
            .any(|comment| comment.get("text").and_then(Value::as_str) == Some("Root comment"))
    );
    assert!(comments.iter().all(|comment| {
        !comment
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| text.contains("reply"))
    }));

    let requests = server.received_requests().await.unwrap_or_default();
    let requested_paths = requests
        .iter()
        .map(|request| request.url.path().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        requested_paths
            .iter()
            .filter(|path| *path == "/news")
            .count(),
        2
    );
    assert_eq!(
        requested_paths
            .iter()
            .filter(|path| *path == "/item")
            .count(),
        2
    );
    assert!(
        requested_paths
            .iter()
            .all(|path| !path.starts_with("/external"))
    );
    Ok(())
}
