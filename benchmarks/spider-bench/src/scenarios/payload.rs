//! `payload` scenario — PLAN.md §4 row 5: binary tree, depth 10 by default
//! (1,023 pages), every body exactly 256 KiB of valid HTML. ~256 MiB
//! transferred per trial; the report shows MiB/s for this row.
//!
//! This is the parser-cost-on-large-bodies row: each engine parses every page
//! exactly once with its own native parser as the cost of link discovery
//! (millipede via `HtmlKind`/scraper, spider via lol_html). Work is
//! [`PageWork::Accounting`] only — there is deliberately NO re-parse anywhere
//! in either drain (PLAN.md §7, review A-4).
//!
//! Standard link rules (PLAN.md §4 common controls): each page links to its
//! two children (`2i+1`, `2i+2`) near the top of the body, plus one duplicate
//! root link (dedup check) and one off-host link. The off-host link uses
//! `http://localhost/...` (no port: the site is rendered before the ephemeral
//! port is known, so the trap cannot embed it; seeds use `127.0.0.1`, so a
//! SameHostname filtering failure surfaces engine-side as a failed/extra
//! request instead of via the server's `Host: localhost` counter).
//!
//! `--depth` is honored as a scaling override for smoke runs (the tree shape
//! scales naturally); the §4 headline row is the default depth 10.

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::scenario::{PageWork, ScenarioSpec, SiteSpec, expected_from_site};

/// Exact decoded body length of every page: 256 KiB.
const BODY_LEN: usize = 262_144;

/// Headline depth per PLAN.md §4 (1,023 pages, ~256 MiB per trial).
const DEFAULT_DEPTH: u32 = 10;

/// Builds the `payload` scenario for the given run-nonce (and optional depth
/// override for smoke runs; the headline row uses the default depth 10).
pub fn build(nonce: &str, depth: Option<u32>) -> anyhow::Result<ScenarioSpec> {
    let depth = depth.unwrap_or(DEFAULT_DEPTH);
    anyhow::ensure!(
        (1..=12).contains(&depth),
        "payload depth must be in 1..=12 (256 KiB bodies; depth {depth} would need too much memory)"
    );
    let n: u64 = (1u64 << depth) - 1;

    let mut pages = BTreeMap::new();
    for i in 0..n {
        pages.insert(format!("/{nonce}/p/{i}"), render_page(nonce, i, n));
    }

    let site = SiteSpec {
        pages,
        latency: None,
        redirects: BTreeMap::new(),
        gzip: false,
    };
    let expected = expected_from_site(&site, None, None);
    Ok(ScenarioSpec {
        name: "payload",
        root_path: format!("/{nonce}/p/0"),
        site,
        expected,
        work: PageWork::Accounting,
    })
}

/// Renders page `i` of an `n`-page binary tree to exactly [`BODY_LEN`] bytes:
/// links near the top, then one inert comment pads to the exact size.
fn render_page(nonce: &str, i: u64, n: u64) -> Bytes {
    let mut links = String::new();
    for child in [2 * i + 1, 2 * i + 2] {
        if child < n {
            links.push_str(&format!(
                "<li><a href=\"/{nonce}/p/{child}\">child {child}</a></li>\n"
            ));
        }
    }
    let head = format!(
        "<!DOCTYPE html>\n<html><head><title>payload {i}</title></head>\n<body>\n\
<h1>payload page {i} of {n}</h1>\n<ul>\n{links}\
<li><a href=\"/{nonce}/p/0\">root (duplicate)</a></li>\n\
<li><a href=\"http://localhost/{nonce}/p/0\">off-host trap</a></li>\n</ul>\n"
    );
    let tail = "</body></html>\n";

    let pad = BODY_LEN
        .checked_sub(head.len() + tail.len())
        .expect("BODY_LEN exceeds fixed markup");
    assert!(pad >= 7, "padding must fit an empty `<!---->` comment");
    let mut body = String::with_capacity(BODY_LEN);
    body.push_str(&head);
    body.push_str("<!--");
    body.extend(std::iter::repeat_n('x', pad - 7));
    body.push_str("-->");
    body.push_str(tail);
    debug_assert_eq!(body.len(), BODY_LEN);
    Bytes::from(body)
}
