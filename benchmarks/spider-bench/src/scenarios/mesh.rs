//! `mesh` scenario — 8,192-page dedup stress (PLAN.md §4, row 3).
//!
//! 8,192 pages `/<nonce>/p/{0..8191}`, exactly 4,096 bytes each. Page `i`
//! links to up to 8 UNIQUE forward pages (targets strictly greater than `i`,
//! guaranteeing termination), chosen by a fixed seeded formula:
//!
//! - candidate 0: `i + 1` (chain link; proves reachability by construction)
//! - candidate k (k = 1..=7): `(i * P[k-1] + k) mod 8192`, kept only if `> i`
//!
//! with distinct odd prime multipliers `P`. Candidates are deduplicated in
//! order and capped at 8. Full reachability from page 0 is additionally
//! PROVEN at generation time by BFS; generation fails if any page is
//! unreachable (belt and braces — the `i + 1` chain already guarantees it).
//!
//! On top of the forward links, every page carries:
//! - 2 exact duplicate copies of its first forward link (dedup stress),
//! - 1 duplicate root link (`/<nonce>/p/0`),
//! - 1 off-host trap link (`http://localhost/...` while seeds use
//!   `127.0.0.1`; the port cannot be embedded because the server binds its
//!   ephemeral port only after generation — hostname mismatch is what both
//!   engines must filter on, and the server's `Host: localhost` detector
//!   catches any leak that does reach it).
//!
//! That is ~90k link candidates total while every page must be fetched
//! exactly once: the server-side duplicate-hit gate (== 0) is the point of
//! this scenario. Work: `Accounting` (raw row; no re-parse).
//!
//! Owned by its scenario task; the scaffold never edits this file again.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use bytes::Bytes;

use crate::scenario::{PageWork, ScenarioSpec, SiteSpec, expected_from_site};

/// Fixed page count (not depth-scalable).
const PAGES: usize = 8192;
/// Exact body size in bytes for every page.
const PAGE_SIZE: usize = 4096;
/// Distinct odd prime multipliers for forward-link candidates k = 1..=7.
/// Odd => units mod 8192 (2^13), so `i * P[k]` cycles through all residues.
const PRIMES: [usize; 7] = [3271, 5501, 6151, 7013, 7919, 4099, 2003];

/// Forward-link targets for page `i`: up to 8 unique targets, all `> i`.
fn forward_targets(i: usize) -> Vec<usize> {
    let mut targets = Vec::with_capacity(8);
    // Candidate 0: the chain link i+1 (guarantees reachability 0 -> 8191).
    if i + 1 < PAGES {
        targets.push(i + 1);
    }
    for (k, prime) in PRIMES.iter().enumerate() {
        let t = (i * prime + (k + 1)) % PAGES;
        if t > i && !targets.contains(&t) {
            targets.push(t);
        }
    }
    debug_assert!(targets.len() <= 8);
    targets
}

/// Renders one page body, padded with an inert comment to exactly
/// [`PAGE_SIZE`] bytes.
fn render_page(nonce: &str, i: usize, targets: &[usize]) -> anyhow::Result<Bytes> {
    let mut html = String::with_capacity(PAGE_SIZE);
    write!(
        html,
        "<!doctype html><html><head><title>mesh {i}</title></head><body>\n<h1>mesh page {i}</h1>\n"
    )?;
    for t in targets {
        writeln!(html, "<a href=\"/{nonce}/p/{t}\">p{t}</a>")?;
    }
    // 2 exact duplicate copies of the first forward link (dedup stress).
    if let Some(first) = targets.first() {
        for _ in 0..2 {
            writeln!(html, "<a href=\"/{nonce}/p/{first}\">dup{first}</a>")?;
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
        "mesh page {i} base HTML is {used} bytes; no room to pad to {PAGE_SIZE}"
    );
    let pad = PAGE_SIZE - used;
    html.push_str("<!--");
    for _ in 0..pad - 7 {
        html.push('x');
    }
    html.push_str("-->");
    anyhow::ensure!(
        html.len() == PAGE_SIZE,
        "mesh page {i} rendered to {} bytes, want exactly {PAGE_SIZE}",
        html.len()
    );
    Ok(Bytes::from(html.into_bytes()))
}

/// Builds the `mesh` scenario for the given run-nonce. The site shape is
/// fixed at 8,192 pages; a `--depth` override does not apply and is ignored
/// (PLAN.md §4 defines no depth knob for `mesh`, and rejecting would break
/// `orchestrate --scenario all --depth N` runs).
pub fn build(nonce: &str, _depth: Option<u32>) -> anyhow::Result<ScenarioSpec> {
    // Generate all forward-link sets once (used for both the reachability
    // proof and page rendering).
    let all_targets: Vec<Vec<usize>> = (0..PAGES).map(forward_targets).collect();

    // PROVE full reachability from page 0 by BFS; fail generation otherwise.
    let mut seen = vec![false; PAGES];
    let mut queue = std::collections::VecDeque::from([0usize]);
    seen[0] = true;
    let mut reached = 1usize;
    while let Some(i) = queue.pop_front() {
        for &t in &all_targets[i] {
            if !seen[t] {
                seen[t] = true;
                reached += 1;
                queue.push_back(t);
            }
        }
    }
    anyhow::ensure!(
        reached == PAGES,
        "mesh link formula leaves {} of {PAGES} pages unreachable from page 0; \
adjust the formula constants",
        PAGES - reached
    );

    let mut pages = BTreeMap::new();
    for (i, targets) in all_targets.iter().enumerate() {
        pages.insert(format!("/{nonce}/p/{i}"), render_page(nonce, i, targets)?);
    }

    let site = SiteSpec {
        pages,
        latency: None,
        redirects: BTreeMap::new(),
        gzip: false,
    };
    let expected = expected_from_site(&site, None, None);
    anyhow::ensure!(expected.pages == PAGES as u64);
    anyhow::ensure!(expected.decoded_bytes == (PAGES * PAGE_SIZE) as u64);

    Ok(ScenarioSpec {
        name: "mesh",
        root_path: format!("/{nonce}/p/0"),
        site,
        expected,
        work: PageWork::Accounting,
    })
}
