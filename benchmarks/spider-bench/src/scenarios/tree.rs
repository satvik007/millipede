//! `tree` scenario — complete binary tree, scheduler + dedup dominated
//! (PLAN.md §4 row 1).
//!
//! Depth `d` (default 13, CLI-overridable; CI smoke uses 9) yields
//! `2^d - 1` pages at `/<nonce>/p/{i}`. Internal page `i` links to its
//! children `2i+1` and `2i+2` as relative hrefs; EVERY page additionally
//! carries one duplicate link back to the root page (dedup check) and one
//! off-host trap link with hostname `localhost` (seeds use `127.0.0.1`, so a
//! same-hostname filtering failure stays on loopback and is observable via
//! the server's `Host: localhost` counter — see the note in [`off_host_href`]
//! about the port). Bodies are valid HTML padded with an inert comment to
//! exactly 4096 bytes. Per-page work is Accounting only (raw row: count +
//! decoded-byte-sum + seahash checksum; no re-parse on either engine).

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::scenario::{PageWork, ScenarioSpec, SiteSpec, expected_from_site};

/// Exact decoded size of every page body.
const PAGE_SIZE: usize = 4096;
/// Default depth: 2^13 - 1 = 8,191 pages (PLAN.md §4).
const DEFAULT_DEPTH: u32 = 13;

/// Builds the `tree` scenario for the given run-nonce and optional depth
/// override (`--depth 9` = 511 pages is the CI smoke shape).
pub fn build(nonce: &str, depth: Option<u32>) -> anyhow::Result<ScenarioSpec> {
    let depth = depth.unwrap_or(DEFAULT_DEPTH);
    anyhow::ensure!(
        (1..=22).contains(&depth),
        "tree: --depth must be in 1..=22, got {depth}"
    );
    let page_count: u64 = (1u64 << depth) - 1;

    let mut pages = BTreeMap::new();
    for i in 0..page_count {
        pages.insert(format!("/{nonce}/p/{i}"), render_page(nonce, i, page_count)?);
    }
    let site = SiteSpec {
        pages,
        latency: None,
        redirects: BTreeMap::new(),
        gzip: false,
    };

    let expected = expected_from_site(&site, None, None);
    // Sanity: the ground truth must match the closed-form shape.
    anyhow::ensure!(expected.pages == page_count, "tree: page count mismatch");
    anyhow::ensure!(
        expected.decoded_bytes == page_count * PAGE_SIZE as u64,
        "tree: decoded byte total mismatch"
    );

    Ok(ScenarioSpec {
        name: "tree",
        root_path: format!("/{nonce}/p/0"),
        site,
        expected,
        work: PageWork::Accounting,
    })
}

/// Off-host trap href shared with `wide`.
///
/// PLAN.md §4 sketches `http://localhost:<port>/...`, but the scenario
/// contract (`build(nonce, depth)`) pre-renders bodies before the server
/// binds its ephemeral port, so the port cannot be embedded. Hostname
/// `localhost` without a port is used instead: it still resolves to loopback
/// only (never remote), the server's `Host: localhost` off-host counter still
/// fires for anything that reaches it, and an engine that follows the link to
/// `localhost:80` fails locally (connection refused / unknown path), which
/// the engine-side gates surface.
pub(crate) fn off_host_href(nonce: &str) -> String {
    format!("http://localhost/{nonce}/p/0")
}

/// Appends an inert padding comment plus the closing tags so the finished
/// document is exactly `target` bytes. Shared with `wide`.
pub(crate) fn pad_to_exact(mut html: String, target: usize) -> anyhow::Result<Bytes> {
    const SUFFIX: &str = "</body></html>";
    const COMMENT_OVERHEAD: usize = "<!---->".len();
    let used = html.len() + SUFFIX.len();
    anyhow::ensure!(
        used + COMMENT_OVERHEAD <= target,
        "page content ({used} bytes + {COMMENT_OVERHEAD} pad overhead) exceeds target {target}"
    );
    let filler = target - used - COMMENT_OVERHEAD;
    html.reserve(target - html.len());
    html.push_str("<!--");
    html.extend(std::iter::repeat_n('p', filler));
    html.push_str("-->");
    html.push_str(SUFFIX);
    debug_assert_eq!(html.len(), target);
    Ok(Bytes::from(html))
}

/// Renders page `i` of a `total`-page complete binary tree, exactly
/// [`PAGE_SIZE`] bytes.
fn render_page(nonce: &str, i: u64, total: u64) -> anyhow::Result<Bytes> {
    let mut html = String::with_capacity(PAGE_SIZE);
    html.push_str("<!doctype html><html><head><meta charset=\"utf-8\"><title>tree ");
    html.push_str(&i.to_string());
    html.push_str("</title></head><body><h1>tree page ");
    html.push_str(&i.to_string());
    html.push_str("</h1>");

    // Children as RELATIVE hrefs: resolved against `/<nonce>/p/{i}`, the base
    // directory is `/<nonce>/p/`, so `href="7"` -> `/<nonce>/p/7`.
    let left = 2 * i + 1;
    let right = 2 * i + 2;
    if left < total {
        html.push_str("<p><a href=\"");
        html.push_str(&left.to_string());
        html.push_str("\">left</a> <a href=\"");
        html.push_str(&right.to_string());
        html.push_str("\">right</a></p>");
    }

    // Duplicate link to the root page (dedup check; relative).
    html.push_str("<p><a href=\"0\">root</a></p>");
    // Off-host trap link (must never be fetched).
    html.push_str("<p><a href=\"");
    html.push_str(&off_host_href(nonce));
    html.push_str("\">mirror</a></p>");

    pad_to_exact(html, PAGE_SIZE)
}
