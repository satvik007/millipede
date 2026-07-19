//! `wide` scenario — flat frontier fan-out stress (PLAN.md §4 row 2,
//! `autoscale_demo` shape).
//!
//! One root page `/<nonce>/p/0` links to 5,000 leaves `/p/1..=5000`
//! (5,001 pages total). The root body is padded to exactly 220 KiB (the exact
//! size lands in `Expected::decoded_bytes` via `expected_from_site`); leaves
//! are exactly 2,048 bytes and carry only the duplicate-root and off-host
//! trap links — no children. Per-page work is Accounting only (raw row).
//!
//! Not depth-scalable: a `--depth` override is rejected.

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::scenario::{PageWork, ScenarioSpec, SiteSpec, expected_from_site};
use crate::scenarios::tree::{off_host_href, pad_to_exact};

/// Number of leaf pages linked from the root.
const LEAVES: u64 = 5000;
/// Exact decoded size of the root body (~220 KiB, per PLAN.md §4).
const ROOT_SIZE: usize = 220 * 1024;
/// Exact decoded size of every leaf body.
const LEAF_SIZE: usize = 2048;

/// Builds the `wide` scenario for the given run-nonce. `depth` is not
/// supported (the shape is fixed at 5,001 pages).
pub fn build(nonce: &str, depth: Option<u32>) -> anyhow::Result<ScenarioSpec> {
    anyhow::ensure!(
        depth.is_none(),
        "wide: --depth is not supported (fixed 5,001-page shape)"
    );

    let mut pages = BTreeMap::new();
    pages.insert(format!("/{nonce}/p/0"), render_root(nonce)?);
    for i in 1..=LEAVES {
        pages.insert(format!("/{nonce}/p/{i}"), render_leaf(nonce, i)?);
    }
    let site = SiteSpec {
        pages,
        latency: None,
        redirects: BTreeMap::new(),
        gzip: false,
    };

    let expected = expected_from_site(&site, None, None);
    // Sanity: the ground truth must match the closed-form shape.
    anyhow::ensure!(expected.pages == LEAVES + 1, "wide: page count mismatch");
    anyhow::ensure!(
        expected.decoded_bytes == ROOT_SIZE as u64 + LEAVES * LEAF_SIZE as u64,
        "wide: decoded byte total mismatch"
    );

    Ok(ScenarioSpec {
        name: "wide",
        root_path: format!("/{nonce}/p/0"),
        site,
        expected,
        work: PageWork::Accounting,
    })
}

/// Renders the root page: 5,000 relative leaf links plus the duplicate-root
/// and off-host trap links, exactly [`ROOT_SIZE`] bytes.
fn render_root(nonce: &str) -> anyhow::Result<Bytes> {
    let mut html = String::with_capacity(ROOT_SIZE);
    html.push_str(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>wide root</title></head>\
         <body><h1>wide root</h1>",
    );

    // Duplicate link to the root page itself (dedup check; relative).
    html.push_str("<p><a href=\"0\">root</a></p>");
    // Off-host trap link (must never be fetched).
    html.push_str("<p><a href=\"");
    html.push_str(&off_host_href(nonce));
    html.push_str("\">mirror</a></p>");

    // Leaf fan-out as RELATIVE hrefs: base directory is `/<nonce>/p/`.
    html.push_str("<ul>");
    for i in 1..=LEAVES {
        html.push_str("<li><a href=\"");
        html.push_str(&i.to_string());
        html.push_str("\">item ");
        html.push_str(&i.to_string());
        html.push_str("</a></li>");
    }
    html.push_str("</ul>");

    pad_to_exact(html, ROOT_SIZE)
}

/// Renders leaf `i`: duplicate-root + off-host links only, no children,
/// exactly [`LEAF_SIZE`] bytes.
fn render_leaf(nonce: &str, i: u64) -> anyhow::Result<Bytes> {
    let mut html = String::with_capacity(LEAF_SIZE);
    html.push_str("<!doctype html><html><head><meta charset=\"utf-8\"><title>wide leaf ");
    html.push_str(&i.to_string());
    html.push_str("</title></head><body><h1>wide leaf ");
    html.push_str(&i.to_string());
    html.push_str("</h1>");

    // Duplicate link to the root page (dedup check; relative).
    html.push_str("<p><a href=\"0\">root</a></p>");
    // Off-host trap link (must never be fetched).
    html.push_str("<p><a href=\"");
    html.push_str(&off_host_href(nonce));
    html.push_str("\">mirror</a></p>");

    pad_to_exact(html, LEAF_SIZE)
}
