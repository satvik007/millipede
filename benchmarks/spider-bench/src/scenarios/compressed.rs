//! `compressed` scenario — PLAN.md §4 row 7: binary tree, depth 12 by default
//! (4,095 pages), every body exactly 32 KiB **decoded** of realistic,
//! compressible HTML (repeated markup rows, not random bytes).
//!
//! `SiteSpec.gzip = true`: the scaffold server pre-compresses each body once
//! at startup and serves the gzip form when the request's `Accept-Encoding`
//! permits it, identity otherwise, recording per-request `Accept-Encoding`
//! (`accept_encoding_gzip` / `accept_encoding_identity`) and body
//! bytes-on-wire in the per-trial server snapshot.
//!
//! Work is [`PageWork::Accounting`] over DECODED bytes: `Expected.checksum`
//! and `Expected.decoded_bytes` (= pages x 32,768) are computed from the
//! identity bodies, so a valid trial proves each engine actually decoded the
//! content. Bytes-on-wire is NOT gated (PLAN.md §8 item 7): millipede's
//! reqwest 0.12 is built with gzip/brotli/deflate and should negotiate gzip;
//! what spider's `sync`-only build negotiates is observed at runtime and
//! surfaced per engine in samples.jsonl via the embedded server snapshot
//! (`server.accept_encoding_gzip` vs `server.accept_encoding_identity`, plus
//! the trial's authoritative `bytes_on_wire`). A negotiation difference is a
//! reportable finding carried as a row note, not a validation error — the
//! decoded checksum gate still holds either way, because the server falls
//! back to identity for non-gzip clients.
//!
//! Standard link rules apply (two children near the top, duplicate root link,
//! off-host trap). The off-host link uses `http://localhost/...` (no port:
//! the site is rendered before the ephemeral port is known; seeds use
//! `127.0.0.1`, so a SameHostname filtering failure surfaces engine-side).
//!
//! `--depth` is honored as a scaling override for smoke runs; the §4 headline
//! row is the default depth 12.

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::scenario::{PageWork, ScenarioSpec, SiteSpec, expected_from_site};

/// Exact DECODED body length of every page: 32 KiB.
const BODY_LEN: usize = 32_768;

/// Headline depth per PLAN.md §4 (4,095 pages).
const DEFAULT_DEPTH: u32 = 12;

/// Realistic, highly compressible filler: repeated catalog-style table rows
/// (repeated markup compresses like real templated HTML, unlike random bytes).
const FILLER_ROW: &str = "<tr><td class=\"sku\">SKU-4711</td>\
<td class=\"name\">Widget, standard finish</td>\
<td class=\"qty\">42</td><td class=\"price\">$3.99</td>\
<td class=\"stock\">in stock</td></tr>\n";

/// Builds the `compressed` scenario for the given run-nonce (and optional
/// depth override for smoke runs; the headline row uses the default depth 12).
pub fn build(nonce: &str, depth: Option<u32>) -> anyhow::Result<ScenarioSpec> {
    let depth = depth.unwrap_or(DEFAULT_DEPTH);
    anyhow::ensure!(
        (1..=16).contains(&depth),
        "compressed depth must be in 1..=16 (got {depth})"
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
        gzip: true,
    };
    // expected_from_site sums the identity (decoded) bodies, so for the
    // default depth Expected.decoded_bytes == 4095 * 32768 exactly.
    let expected = expected_from_site(&site, None, None);
    Ok(ScenarioSpec {
        name: "compressed",
        root_path: format!("/{nonce}/p/0"),
        site,
        expected,
        work: PageWork::Accounting,
    })
}

/// Renders page `i` of an `n`-page binary tree to exactly [`BODY_LEN`]
/// decoded bytes: links near the top, then repeated compressible table rows,
/// then one inert comment pads the remainder to the exact size.
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
        "<!DOCTYPE html>\n<html><head><title>compressed {i}</title></head>\n<body>\n\
<h1>compressed page {i} of {n}</h1>\n<ul>\n{links}\
<li><a href=\"/{nonce}/p/0\">root (duplicate)</a></li>\n\
<li><a href=\"http://localhost/{nonce}/p/0\">off-host trap</a></li>\n</ul>\n\
<table>\n"
    );
    let tail = "</table>\n</body></html>\n";

    // Fill with whole rows, keeping >= 7 bytes for the exact-size comment pad.
    let budget = BODY_LEN
        .checked_sub(head.len() + tail.len())
        .expect("BODY_LEN exceeds fixed markup");
    assert!(budget >= 7, "padding must fit an empty `<!---->` comment");
    let rows = (budget - 7) / FILLER_ROW.len();
    let pad = budget - rows * FILLER_ROW.len();
    debug_assert!(pad >= 7);

    let mut body = String::with_capacity(BODY_LEN);
    body.push_str(&head);
    for _ in 0..rows {
        body.push_str(FILLER_ROW);
    }
    body.push_str("<!--");
    body.extend(std::iter::repeat_n('x', pad - 7));
    body.push_str("-->");
    body.push_str(tail);
    debug_assert_eq!(body.len(), BODY_LEN);
    Bytes::from(body)
}
