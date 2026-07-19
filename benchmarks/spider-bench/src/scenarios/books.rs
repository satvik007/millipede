//! `books` scenario — 5,100-page catalog with identical scraper extraction on
//! both engines (PLAN.md §4 row 8).
//!
//! Site shape (all paths nonce-prefixed):
//! - 100 listing pages `/list/{1..100}`, exactly 16,384 bytes each, carrying
//!   50 unique detail links (ids partitioned so the 100 listings cover exactly
//!   5,000 distinct books), a next-page link (`/list/{n+1}`, absent on the
//!   last listing), one duplicate-root link, and one off-host trap link.
//! - 5,000 detail pages `/book/{1..5000}`, exactly 4,096 bytes each, with a
//!   deterministic `<h1 class="title">` + `<p class="price">` record, no
//!   outgoing local links except the duplicate-root link (plus the off-host
//!   trap, which is not local).
//!
//! Work: [`PageWork::Extract`] with ONE shared extraction fn used verbatim by
//! both engines. Millipede runs it against the already-parsed DOM; spider's
//! drain re-parses the body bytes first — that asymmetry is by design
//! (PLAN.md §7) and is not compensated for here. The expected digest is
//! computed in this constructor by hashing the same records the generator
//! emits — never a hardcoded magic number.
//!
//! Owned by its scenario task; the scaffold never edits this file again.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::OnceLock;

use bytes::Bytes;
use scraper::{Html, Selector};

use crate::scenario::{Digest, PageWork, ScenarioSpec, SiteSpec, expected_from_site};

/// Number of listing pages.
const LISTINGS: u32 = 100;
/// Detail links per listing; listings partition ids 1..=5,000 exactly.
const BOOKS_PER_LISTING: u32 = 50;
/// Total detail pages (= extraction records).
const BOOKS: u32 = LISTINGS * BOOKS_PER_LISTING;
/// Exact byte size of every listing page.
const LISTING_SIZE: usize = 16_384;
/// Exact byte size of every detail page.
const DETAIL_SIZE: usize = 4_096;

// ---------------------------------------------------------------------------
// shared extraction (identical code path for BOTH engines)
// ---------------------------------------------------------------------------

/// `h1.title` — compiled once, same selector string for both engines.
fn title_selector() -> &'static Selector {
    static SEL: OnceLock<Selector> = OnceLock::new();
    SEL.get_or_init(|| Selector::parse("h1.title").expect("valid selector `h1.title`"))
}

/// `p.price` — compiled once, same selector string for both engines.
fn price_selector() -> &'static Selector {
    static SEL: OnceLock<Selector> = OnceLock::new();
    SEL.get_or_init(|| Selector::parse("p.price").expect("valid selector `p.price`"))
}

/// Folds one `(id, title, price)` record into the digest. Used verbatim by
/// the DOM extraction below AND by the expected-digest precomputation in
/// [`build`], so ground truth and extraction share one record encoding.
fn fold_record(digest: &mut Digest, id: u64, title: &str, price: &str) {
    let mut record = String::with_capacity(id_len_hint(title, price));
    // US (unit separator) delimiters: cannot appear in the generated fields.
    let _ = write!(record, "{id}\u{1f}{title}\u{1f}{price}");
    digest.record(record.as_bytes());
}

fn id_len_hint(title: &str, price: &str) -> usize {
    title.len() + price.len() + 22
}

/// The shared extraction fn ([`PageWork::Extract`]): on detail pages it
/// selects `h1.title` + `p.price`, recovers the id from the title, and folds
/// the record into the commutative digest. Listing pages match neither
/// selector and contribute nothing here (they count via page accounting).
pub(crate) fn extract(html: &Html, digest: &mut Digest) {
    let Some(title_el) = html.select(title_selector()).next() else {
        return; // listing page: no h1.title
    };
    let title: String = title_el.text().collect();
    let Some(price_el) = html.select(price_selector()).next() else {
        return;
    };
    let price: String = price_el.text().collect();
    let Some(id) = title
        .strip_prefix("Book ")
        .and_then(|rest| rest.parse::<u64>().ok())
    else {
        return;
    };
    fold_record(digest, id, &title, &price);
}

// ---------------------------------------------------------------------------
// deterministic generation
// ---------------------------------------------------------------------------

fn title_for(id: u32) -> String {
    format!("Book {id}")
}

/// Deterministic price in £1.99..£49.99 derived from the id (seeded, stable).
fn price_for(id: u32) -> String {
    let pence = 199 + (u64::from(id).wrapping_mul(7919) % 4_801);
    format!("£{}.{:02}", pence / 100, pence % 100)
}

/// Pads an HTML document ending in `</body></html>` to exactly `target` bytes
/// by inserting one inert comment before the closing tags.
fn pad_to(mut html: String, target: usize, what: &str) -> anyhow::Result<Bytes> {
    const SUFFIX: &str = "</body></html>";
    const OVERHEAD: usize = 7; // "<!--" + "-->"
    anyhow::ensure!(
        html.ends_with(SUFFIX),
        "{what}: generated body must end with {SUFFIX}"
    );
    anyhow::ensure!(
        html.len() + OVERHEAD <= target,
        "{what}: body is {} bytes; cannot pad to {target}",
        html.len()
    );
    let filler = target - html.len() - OVERHEAD;
    let pad = format!("<!--{}-->", "x".repeat(filler));
    let insert_at = html.len() - SUFFIX.len();
    html.insert_str(insert_at, &pad);
    anyhow::ensure!(
        html.len() == target,
        "{what}: padded to {} bytes, wanted {target}",
        html.len()
    );
    Ok(Bytes::from(html))
}

/// One listing page: 50 detail links + optional next-page + duplicate-root +
/// off-host trap (PLAN.md §4 common controls).
fn listing_page(nonce: &str, n: u32) -> anyhow::Result<Bytes> {
    let first_id = (n - 1) * BOOKS_PER_LISTING + 1;
    let mut html = String::with_capacity(LISTING_SIZE);
    let _ = write!(
        html,
        "<html><head><title>Catalog page {n}</title></head><body>\
         <h1>Catalog page {n}</h1><ul>"
    );
    for id in first_id..first_id + BOOKS_PER_LISTING {
        let _ = write!(
            html,
            "<li><a href=\"/{nonce}/book/{id}\">Book {id}</a></li>"
        );
    }
    html.push_str("</ul>");
    if n < LISTINGS {
        let _ = write!(
            html,
            "<a class=\"next\" href=\"/{nonce}/list/{}\">next</a>",
            n + 1
        );
    }
    // Duplicate-root link (dedup check) + off-host trap (`localhost` host
    // while seeds use `127.0.0.1`; a filtering failure stays local and shows
    // up as an off-host hit / failed request, never as traffic off-machine).
    let _ = write!(
        html,
        "<a href=\"/{nonce}/list/1\">home</a>\
         <a href=\"http://localhost/{nonce}/offsite\">mirror</a>\
         </body></html>"
    );
    pad_to(html, LISTING_SIZE, "listing")
}

/// One detail page: the extraction record + duplicate-root + off-host trap.
/// No other outgoing local links.
fn detail_page(nonce: &str, id: u32) -> anyhow::Result<Bytes> {
    let title = title_for(id);
    let price = price_for(id);
    let mut html = String::with_capacity(DETAIL_SIZE);
    let _ = write!(
        html,
        "<html><head><title>{title}</title></head><body>\
         <h1 class=\"title\">{title}</h1>\
         <p class=\"price\">{price}</p>\
         <a href=\"/{nonce}/list/1\">home</a>\
         <a href=\"http://localhost/{nonce}/offsite\">mirror</a>\
         </body></html>"
    );
    pad_to(html, DETAIL_SIZE, "detail")
}

// ---------------------------------------------------------------------------
// constructor
// ---------------------------------------------------------------------------

/// Builds the `books` scenario for the given run-nonce. The scenario is not
/// depth-scalable; a `--depth` override is rejected (registry contract).
pub fn build(nonce: &str, depth: Option<u32>) -> anyhow::Result<ScenarioSpec> {
    anyhow::ensure!(
        depth.is_none(),
        "scenario `books` is not depth-scalable; do not pass --depth"
    );

    let mut pages = BTreeMap::new();
    for n in 1..=LISTINGS {
        pages.insert(format!("/{nonce}/list/{n}"), listing_page(nonce, n)?);
    }
    for id in 1..=BOOKS {
        pages.insert(format!("/{nonce}/book/{id}"), detail_page(nonce, id)?);
    }
    anyhow::ensure!(
        pages.len() as u64 == u64::from(LISTINGS + BOOKS),
        "path collision: {} pages generated, expected {}",
        pages.len(),
        LISTINGS + BOOKS
    );

    // Ground-truth digest: fold the SAME records the generator embeds, via the
    // SAME fold_record used by the extraction fn. Computed here, per-run.
    let mut digest = Digest::default();
    for id in 1..=BOOKS {
        fold_record(&mut digest, u64::from(id), &title_for(id), &price_for(id));
    }

    // Sanity: run the real extraction fn over two generated detail pages and
    // one listing page; catches selector/format drift at build time (before
    // `ready`, unmeasured).
    for probe_id in [1u32, BOOKS] {
        let body = detail_page(nonce, probe_id)?;
        let dom = Html::parse_document(std::str::from_utf8(&body)?);
        let mut probe = Digest::default();
        extract(&dom, &mut probe);
        let mut want = Digest::default();
        fold_record(
            &mut want,
            u64::from(probe_id),
            &title_for(probe_id),
            &price_for(probe_id),
        );
        anyhow::ensure!(
            probe == want,
            "extraction self-check failed for /book/{probe_id}"
        );
    }
    {
        let body = listing_page(nonce, 1)?;
        let dom = Html::parse_document(std::str::from_utf8(&body)?);
        let mut probe = Digest::default();
        extract(&dom, &mut probe);
        anyhow::ensure!(
            probe == Digest::default(),
            "listing pages must yield no extraction records"
        );
    }

    let site = SiteSpec {
        pages,
        latency: None,
        redirects: BTreeMap::new(),
        gzip: false,
    };
    let expected = expected_from_site(&site, Some(u64::from(BOOKS)), Some(digest.value()));
    Ok(ScenarioSpec {
        name: "books",
        root_path: format!("/{nonce}/list/1"),
        site,
        expected,
        work: PageWork::Extract(extract),
    })
}
