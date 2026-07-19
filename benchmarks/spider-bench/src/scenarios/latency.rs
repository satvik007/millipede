//! `latency` scenario — pipeline-keeping under symmetric server latency
//! (PLAN.md §4, row 4).
//!
//! Binary tree of depth 11 (2^11 - 1 = 2,047 pages `/<nonce>/p/{i}`, page `i`
//! linking to children `2i+1` and `2i+2`), exactly 4,096-byte bodies, and the
//! same per-page trap links as the `tree` scenario:
//! - 1 duplicate root link (`/<nonce>/p/0`),
//! - 1 off-host trap link (`http://localhost/...` while seeds use
//!   `127.0.0.1`; portless because the server binds its ephemeral port only
//!   after generation — hostname mismatch is what both engines must filter
//!   on, and the server's `Host: localhost` detector catches any leak).
//!
//! `SiteSpec.latency = Some(10 ms)`: the scaffold server sleeps 10 ms before
//! EVERY response, identically for all engines — server-side latency, not
//! client politeness. At C = 32 the theoretical floor is ~2047 * 10 ms / 32
//! ≈ 0.64 s plus transfer; the scenario measures how well each engine keeps
//! its pipeline full. Work: `Accounting` (raw row; no re-parse).
//!
//! `--depth` scales the tree like the `tree` scenario (default 11).
//!
//! Owned by its scenario task; the scaffold never edits this file again.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::Duration;

use bytes::Bytes;

use crate::scenario::{PageWork, ScenarioSpec, SiteSpec, expected_from_site};

/// Default tree depth (2,047 pages).
const DEFAULT_DEPTH: u32 = 11;
/// Exact body size in bytes for every page.
const PAGE_SIZE: usize = 4096;
/// Server-side sleep applied to every response, identically for all engines.
const SERVER_LATENCY: Duration = Duration::from_millis(10);

/// Renders one page body, padded with an inert comment to exactly
/// [`PAGE_SIZE`] bytes.
fn render_page(nonce: &str, i: usize, pages: usize) -> anyhow::Result<Bytes> {
    let mut html = String::with_capacity(PAGE_SIZE);
    write!(
        html,
        "<!doctype html><html><head><title>latency {i}</title></head><body>\n<h1>latency page {i}</h1>\n"
    )?;
    for child in [2 * i + 1, 2 * i + 2] {
        if child < pages {
            writeln!(html, "<a href=\"/{nonce}/p/{child}\">p{child}</a>")?;
        }
    }
    // 1 duplicate root link.
    writeln!(html, "<a href=\"/{nonce}/p/0\">root</a>")?;
    // 1 off-host trap link (portless `localhost`; see module docs).
    writeln!(html, "<a href=\"http://localhost/{nonce}/p/{i}\">offhost</a>")?;
    html.push_str("</body></html>\n");

    // Pad with `<!--xxx...-->` to the exact size.
    let used = html.len();
    anyhow::ensure!(
        used + 7 <= PAGE_SIZE,
        "latency page {i} base HTML is {used} bytes; no room to pad to {PAGE_SIZE}"
    );
    let pad = PAGE_SIZE - used;
    html.push_str("<!--");
    for _ in 0..pad - 7 {
        html.push('x');
    }
    html.push_str("-->");
    anyhow::ensure!(
        html.len() == PAGE_SIZE,
        "latency page {i} rendered to {} bytes, want exactly {PAGE_SIZE}",
        html.len()
    );
    Ok(Bytes::from(html.into_bytes()))
}

/// Builds the `latency` scenario for the given run-nonce (and optional depth
/// override; default depth 11 = 2,047 pages).
pub fn build(nonce: &str, depth: Option<u32>) -> anyhow::Result<ScenarioSpec> {
    let depth = depth.unwrap_or(DEFAULT_DEPTH);
    anyhow::ensure!(
        (1..=20).contains(&depth),
        "latency depth {depth} out of range 1..=20"
    );
    let pages_count = (1usize << depth) - 1;

    let mut pages = BTreeMap::new();
    for i in 0..pages_count {
        pages.insert(
            format!("/{nonce}/p/{i}"),
            render_page(nonce, i, pages_count)?,
        );
    }

    let site = SiteSpec {
        pages,
        latency: Some(SERVER_LATENCY),
        redirects: BTreeMap::new(),
        gzip: false,
    };
    let expected = expected_from_site(&site, None, None);
    anyhow::ensure!(expected.pages == pages_count as u64);
    anyhow::ensure!(expected.decoded_bytes == (pages_count * PAGE_SIZE) as u64);

    Ok(ScenarioSpec {
        name: "latency",
        root_path: format!("/{nonce}/p/0"),
        site,
        expected,
        work: PageWork::Accounting,
    })
}
