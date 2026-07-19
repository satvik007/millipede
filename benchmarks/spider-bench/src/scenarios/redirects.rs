//! `redirects` scenario — PLAN.md §4 row 6: binary tree, depth 11 by default
//! (2,047 pages, plus 2,047 redirect hops), 4 KiB bodies. Every href targets
//! `/<nonce>/r/{i}`, which the server answers with `301 Location:
//! /<nonce>/p/{i}`; both engines are configured with redirect limit 7 in the
//! shared drivers.
//!
//! **Seed choice (documented per task spec): the seed is the redirect source
//! `/<nonce>/r/0`, not the direct `/p/0`.** Rationale: every page carries the
//! standard duplicate-root link, and that link must target `/r/0` (like every
//! other href) so engines dedup it against the already-visited seed URL. If
//! the seed were `/p/0`, either the dup-root link would hit `/r/0` 2,047 extra
//! times (failing the exactly-one-hit-per-redirect gate) or it would point at
//! `/p/0` directly (a URL engines have not requested, causing a duplicate
//! page fetch). Seeding at `/r/0` makes every `/r/{i}` and every `/p/{i}` be
//! hit exactly once, for all three engines (the baseline driver already
//! enters each page through its redirect source).
//!
//! What this row measures: each engine's redirect-following cost.
//! millipede-http builds reqwest with `Policy::none` and follows redirects
//! manually inside one logical request (so `requests_finished` still counts
//! each page once); spider delegates to its own client's redirect policy.
//! That implementation difference IS the measurement, not a bug.
//!
//! Off-host trap: `http://localhost/...` without a port — the site is
//! rendered before the ephemeral port is known, so the trap cannot embed it;
//! seeds use `127.0.0.1`, so a SameHostname filtering failure surfaces
//! engine-side (failed/extra request) rather than via the server's
//! `Host: localhost` counter.
//!
//! `--depth` is honored as a scaling override for smoke runs; the §4 headline
//! row is the default depth 11.

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::scenario::{PageWork, ScenarioSpec, SiteSpec, expected_from_site};

/// Exact decoded body length of every page: 4 KiB.
const BODY_LEN: usize = 4096;

/// Headline depth per PLAN.md §4 (2,047 pages + 2,047 redirect hops).
const DEFAULT_DEPTH: u32 = 11;

/// Builds the `redirects` scenario for the given run-nonce (and optional
/// depth override for smoke runs; the headline row uses the default depth 11).
pub fn build(nonce: &str, depth: Option<u32>) -> anyhow::Result<ScenarioSpec> {
    let depth = depth.unwrap_or(DEFAULT_DEPTH);
    anyhow::ensure!(
        (1..=16).contains(&depth),
        "redirects depth must be in 1..=16 (got {depth})"
    );
    let n: u64 = (1u64 << depth) - 1;

    let mut pages = BTreeMap::new();
    let mut redirects = BTreeMap::new();
    for i in 0..n {
        pages.insert(format!("/{nonce}/p/{i}"), render_page(nonce, i, n));
        // One 301 hop in front of every page, seed included.
        redirects.insert(format!("/{nonce}/r/{i}"), format!("/{nonce}/p/{i}"));
    }

    let site = SiteSpec {
        pages,
        latency: None,
        redirects,
        gzip: false,
    };
    let expected = expected_from_site(&site, None, None);
    Ok(ScenarioSpec {
        name: "redirects",
        // Seed through the redirect source (see module docs).
        root_path: format!("/{nonce}/r/0"),
        site,
        expected,
        work: PageWork::Accounting,
    })
}

/// Renders page `i` of an `n`-page binary tree to exactly [`BODY_LEN`] bytes.
/// Every internal href targets the `/r/{i}` redirect source; an inert comment
/// pads to the exact size.
fn render_page(nonce: &str, i: u64, n: u64) -> Bytes {
    let mut links = String::new();
    for child in [2 * i + 1, 2 * i + 2] {
        if child < n {
            links.push_str(&format!(
                "<li><a href=\"/{nonce}/r/{child}\">child {child}</a></li>\n"
            ));
        }
    }
    let head = format!(
        "<!DOCTYPE html>\n<html><head><title>redirects {i}</title></head>\n<body>\n\
<h1>redirects page {i} of {n}</h1>\n<ul>\n{links}\
<li><a href=\"/{nonce}/r/0\">root (duplicate)</a></li>\n\
<li><a href=\"http://localhost/{nonce}/r/0\">off-host trap</a></li>\n</ul>\n"
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
