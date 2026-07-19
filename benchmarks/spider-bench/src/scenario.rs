//! The scenario contract: everything a scenario task must produce.
//!
//! Scenario constructors (`scenarios/*.rs`) build a [`ScenarioSpec`] from a
//! run-nonce; the scaffold (server, engines, orchestrator) consumes it. This
//! file is owned by the scaffold and must not be edited by scenario tasks.

// Parts of the contract (Extract, Digest::record, expected_from_site, ...) are
// exercised only once scenario implementations land; keep the scaffold
// warning-free until then.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;

/// Pre-rendered synthetic site served by the axum server.
///
/// All paths are absolute and MUST embed the run-nonce prefix (e.g.
/// `/<nonce>/p/0`), including redirect sources and targets. Bodies are
/// immutable [`Bytes`] so per-request clones are O(1).
pub struct SiteSpec {
    /// path -> pre-rendered body (decoded/identity form).
    pub pages: BTreeMap<String, Bytes>,
    /// Optional per-response server-side latency (applied to every page hit).
    pub latency: Option<Duration>,
    /// path -> `301` Location target.
    pub redirects: BTreeMap<String, String>,
    /// When true the server pre-compresses each body once with gzip and serves
    /// the compressed form when `Accept-Encoding` permits, identity otherwise.
    pub gzip: bool,
}

/// Precomputed ground truth a valid trial must reproduce exactly.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Expected {
    /// Unique page count (redirect hops not included).
    pub pages: u64,
    /// Sum of decoded body lengths over all unique pages.
    pub decoded_bytes: u64,
    /// Wrapping sum of `seahash` over each decoded body (see [`body_checksum`]).
    pub checksum: u64,
    /// Extraction rows: number of records the shared extractor must yield.
    pub records: Option<u64>,
    /// Extraction rows: commutative digest value (see [`Digest`]).
    pub digest: Option<u64>,
}

/// Per-page work an engine driver performs for a scenario.
#[derive(Clone, Copy)]
pub enum PageWork {
    /// Raw rows (1-7): count + decoded-byte-sum + checksum only; no re-parse.
    Accounting,
    /// Extraction rows (8-9): the shared `scraper` selector function. Runs
    /// against millipede's already-parsed DOM and against a fresh
    /// `scraper::Html` re-parse in spider's drain (documented asymmetry).
    Extract(fn(&scraper::Html, &mut Digest)),
}

/// What the orchestrator sends a child over stdin (one JSON line between
/// `ready` and `go`).
///
/// The child must NEVER build the full [`SiteSpec`]: pre-rendering every page
/// body in the child would push the child's `ru_maxrss` high-water mark up by
/// the whole site size (~256 MiB for `payload`) and charge site
/// rendering/checksum CPU to the trial process, contaminating the published
/// RSS/CPU columns. The parent (which must render the site anyway to serve
/// it) computes the ground truth once and ships only these few values.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TrialWire {
    /// Ground truth for validation gates.
    pub expected: Expected,
    /// Baseline entry URLs: every unique page, entered through its redirect
    /// source when one exists (full URLs including scheme/host/port).
    pub entry_urls: Vec<String>,
}

/// Everything an engine driver needs for one child trial: the parent-supplied
/// [`TrialWire`] plus the locally-resolved per-page work fn.
pub struct TrialSpec {
    /// Ground truth for validation gates.
    pub expected: Expected,
    /// Work performed per fetched page (resolved from the scenario name).
    pub work: PageWork,
    /// Baseline entry URL list (unused by the crawler engines).
    pub entry_urls: Vec<String>,
}

/// Computes the baseline "speed of light" entry URL list for a site: every
/// unique page, but entered through its redirect source when one exists, so
/// the baseline pays the same 301 hops the crawlers pay and the per-hop
/// server gate holds for all engines. `base` is `http://127.0.0.1:PORT`
/// (no trailing slash).
pub fn baseline_entry_urls(site: &SiteSpec, base: &str) -> Vec<String> {
    let mut source_for_target: BTreeMap<&str, &str> = site
        .redirects
        .iter()
        .map(|(source, target)| (target.as_str(), source.as_str()))
        .collect();
    site.pages
        .keys()
        .map(|path| {
            let entry = source_for_target.remove(path.as_str()).unwrap_or(path);
            format!("{base}{entry}")
        })
        .collect()
}

/// A complete benchmark scenario.
pub struct ScenarioSpec {
    /// Registry name (`tree`, `wide`, ...).
    pub name: &'static str,
    /// Root path (nonce-prefixed) the crawl starts from, e.g. `/<nonce>/p/0`.
    pub root_path: String,
    /// The site to serve.
    pub site: SiteSpec,
    /// Ground truth for validation gates.
    pub expected: Expected,
    /// Work performed per fetched page.
    pub work: PageWork,
}

/// Commutative record accumulator: `count` plus wrapping-sum and XOR of the
/// per-record `seahash` values. Accumulation order does not matter, so results
/// are independent of crawl scheduling.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Digest {
    /// Number of records accumulated.
    pub count: u64,
    /// Wrapping sum of per-record hashes.
    pub sum: u64,
    /// XOR of per-record hashes.
    pub xor: u64,
}

impl Digest {
    /// Folds one record (already serialized to bytes) into the digest.
    pub fn record(&mut self, record_bytes: &[u8]) {
        let h = seahash::hash(record_bytes);
        self.count += 1;
        self.sum = self.sum.wrapping_add(h);
        self.xor ^= h;
    }

    /// Collapses the digest to the single `u64` stored in [`Expected::digest`].
    pub fn value(&self) -> u64 {
        self.sum.wrapping_add(self.xor.rotate_left(17))
    }

    /// Merges another digest (commutative).
    pub fn merge(&mut self, other: &Digest) {
        self.count += other.count;
        self.sum = self.sum.wrapping_add(other.sum);
        self.xor ^= other.xor;
    }
}

/// Checksum of one decoded body: `seahash` over the bytes.
pub fn body_checksum(decoded_body: &[u8]) -> u64 {
    seahash::hash(decoded_body)
}

/// Accumulates per-body checksums into a running total (wrapping sum, so the
/// result is independent of fetch order).
pub fn accumulate_checksum(running: u64, decoded_body: &[u8]) -> u64 {
    running.wrapping_add(body_checksum(decoded_body))
}

/// Computes [`Expected`] site-level fields (pages/bytes/checksum) directly
/// from a [`SiteSpec`]. Scenario constructors may use this instead of
/// maintaining the numbers by hand; `records`/`digest` stay scenario-provided.
pub fn expected_from_site(site: &SiteSpec, records: Option<u64>, digest: Option<u64>) -> Expected {
    let mut checksum = 0u64;
    let mut decoded_bytes = 0u64;
    for body in site.pages.values() {
        decoded_bytes += body.len() as u64;
        checksum = accumulate_checksum(checksum, body);
    }
    Expected {
        pages: site.pages.len() as u64,
        decoded_bytes,
        checksum,
        records,
        digest,
    }
}
