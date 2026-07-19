//! `hn` scenario — PLAN.md §4 row 9: link-dense large pages with story +
//! comment extraction (markup shape inspired by `millipede/examples/hackernews.rs`,
//! fully local, no politeness delay).
//!
//! Site shape:
//! - 40 front pages `/<nonce>/news/{1..40}`, each exactly 49,152 bytes, each
//!   listing 25 stories. Every story row carries BOTH a title anchor and a
//!   comments anchor pointing at the SAME `/<nonce>/item/{id}` URL (deliberate
//!   duplicate anchors — within-page dedup stress). The 40 fronts partition
//!   ids 1..=1000 deterministically (front `p` owns `(p-1)*25+1 ..= p*25`),
//!   so exactly 1,000 distinct items are covered. Each front links to the next
//!   front (last has none) plus the duplicate root link and an off-host link.
//! - 1,000 item pages `/<nonce>/item/{id}`, exactly 32,768 bytes, each with a
//!   deterministic story header (`span.titleline` title + `span.score`) and
//!   exactly 40 `div.comment` blocks. No outgoing local links except the
//!   duplicate root (plus the off-host trap link).
//!
//! Total pages = 1,040. Extraction (PageWork::Extract, ONE shared fn for both
//! engines): item pages yield 1 story record + 40 comment records; front pages
//! additionally hash their 25 story titles into the digest VALUE (sum/xor
//! only, count untouched) to force real selector work on the large pages
//! while keeping `records == 41,000` (1,000 stories + 40,000 comments).
//! The expected digest is computed at generation time by running the same
//! hashing over the generated data — never hardcoded.
//!
//! Off-host trap: pages are pre-rendered before the server binds its port, so
//! the trap link is `http://localhost/<nonce>/offsite` (no port). PLAN.md §4
//! sketches `http://localhost:<port>/…`; the port is unknowable at build time.
//! The link still exercises host filtering (localhost != 127.0.0.1) and any
//! leak stays local; the server-side `Host: localhost` gate remains in force.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::LazyLock;

use bytes::Bytes;
use scraper::{ElementRef, Html, Selector};

use crate::scenario::{Digest, PageWork, ScenarioSpec, SiteSpec, expected_from_site};

const FRONTS: u64 = 40;
const STORIES_PER_FRONT: u64 = 25;
const ITEMS: u64 = FRONTS * STORIES_PER_FRONT; // 1,000
const COMMENTS_PER_ITEM: u64 = 40;
const EXPECTED_RECORDS: u64 = ITEMS + ITEMS * COMMENTS_PER_ITEM; // 41,000
const FRONT_SIZE: usize = 49_152; // 48 KiB exactly
const ITEM_SIZE: usize = 32_768; // 32 KiB exactly

// ---------------------------------------------------------------------------
// selectors — compiled ONCE; the single shared extract fn (and therefore the
// identical selector strings and compiled objects) serves both engines.
// ---------------------------------------------------------------------------

fn sel(s: &str) -> Selector {
    Selector::parse(s).expect("static selector must parse")
}

static ITEM_MARKER_SELECTOR: LazyLock<Selector> = LazyLock::new(|| sel("div.item-page"));
static STORY_TITLE_SELECTOR: LazyLock<Selector> = LazyLock::new(|| sel("span.titleline > a"));
static STORY_SCORE_SELECTOR: LazyLock<Selector> = LazyLock::new(|| sel("span.score"));
static COMMENT_SELECTOR: LazyLock<Selector> = LazyLock::new(|| sel("div.comment"));
static FRONT_TITLE_SELECTOR: LazyLock<Selector> =
    LazyLock::new(|| sel("tr.athing span.titleline > a"));

// ---------------------------------------------------------------------------
// deterministic content + record serialization (shared by generation-time
// digest computation and the runtime extractor; parity by construction)
// ---------------------------------------------------------------------------

fn story_title(id: u64) -> String {
    let h = seahash::hash(format!("hn-title-{id}").as_bytes());
    format!("Story {id}: benchmarking synthetic frontier {h:016x}")
}

fn story_score_text(id: u64) -> String {
    let points = seahash::hash(format!("hn-score-{id}").as_bytes()) % 4096 + 1;
    format!("{points} points")
}

fn comment_text(id: u64, k: u64) -> String {
    let h = seahash::hash(format!("hn-comment-{id}-{k}").as_bytes());
    format!("Comment {k} on story {id}: deterministic loopback payload {h:016x}")
}

fn story_record(title: &str, score_text: &str) -> Vec<u8> {
    format!("story\x1f{title}\x1f{score_text}").into_bytes()
}

fn comment_record(text: &str) -> Vec<u8> {
    format!("comment\x1f{text}").into_bytes()
}

/// Folds a front-page story title into the digest VALUE without counting a
/// record: `records` stays 1,000 stories + 40,000 comments while front pages
/// still require real selector + hashing work. Commutative (sum/xor), and used
/// identically at generation time and inside the extractor.
fn front_title_fold(digest: &mut Digest, title: &str) {
    let h = seahash::hash(format!("front\x1f{title}").as_bytes());
    digest.sum = digest.sum.wrapping_add(h);
    digest.xor ^= h;
}

// ---------------------------------------------------------------------------
// shared extractor (PageWork::Extract) — runs against millipede's
// already-parsed DOM and against spider's scraper re-parse (PLAN.md §7)
// ---------------------------------------------------------------------------

fn element_text(el: ElementRef<'_>) -> String {
    el.text().collect()
}

pub(crate) fn extract(html: &Html, digest: &mut Digest) {
    if html.select(&ITEM_MARKER_SELECTOR).next().is_some() {
        // Item page: one story record (title + score) ...
        let title = html
            .select(&STORY_TITLE_SELECTOR)
            .next()
            .map(element_text)
            .unwrap_or_default();
        let score = html
            .select(&STORY_SCORE_SELECTOR)
            .next()
            .map(element_text)
            .unwrap_or_default();
        digest.record(&story_record(&title, &score));
        // ... plus ALL div.comment nodes (exactly 40 per item; any deviation
        // shows up in the records/digest gates).
        for comment in html.select(&COMMENT_SELECTOR) {
            digest.record(&comment_record(&element_text(comment)));
        }
    } else {
        // Front page: extract the 25 story titles and hash them into the
        // digest value (uncounted) — real selector work on the 48 KiB pages.
        for anchor in html.select(&FRONT_TITLE_SELECTOR) {
            front_title_fold(digest, &element_text(anchor));
        }
    }
}

// ---------------------------------------------------------------------------
// page rendering (padded with one inert comment to the exact byte size)
// ---------------------------------------------------------------------------

fn front_path(nonce: &str, p: u64) -> String {
    format!("/{nonce}/news/{p}")
}

fn item_path(nonce: &str, id: u64) -> String {
    format!("/{nonce}/item/{id}")
}

/// Ids owned by front page `p` (1-based): `(p-1)*25+1 ..= p*25`.
fn front_ids(p: u64) -> std::ops::RangeInclusive<u64> {
    ((p - 1) * STORIES_PER_FRONT + 1)..=(p * STORIES_PER_FRONT)
}

/// Pads `html` (which must end with `</body></html>`) to exactly `target`
/// bytes by inserting one inert HTML comment before the closing tags.
fn pad_to(mut html: String, target: usize, what: &str) -> anyhow::Result<Bytes> {
    const CLOSE: &str = "</body></html>";
    const OVERHEAD: usize = 7; // "<!--" + "-->"
    anyhow::ensure!(html.ends_with(CLOSE), "{what}: body must end with {CLOSE}");
    anyhow::ensure!(
        html.len() + OVERHEAD <= target,
        "{what}: content {} bytes leaves no room to pad to {target}",
        html.len()
    );
    let fill = target - html.len() - OVERHEAD;
    let insert_at = html.len() - CLOSE.len();
    let mut pad = String::with_capacity(fill + OVERHEAD);
    pad.push_str("<!--");
    pad.extend(std::iter::repeat_n('p', fill));
    pad.push_str("-->");
    html.insert_str(insert_at, &pad);
    anyhow::ensure!(html.len() == target, "{what}: padded to {} != {target}", html.len());
    Ok(Bytes::from(html))
}

fn render_front(nonce: &str, p: u64) -> anyhow::Result<Bytes> {
    let mut h = String::with_capacity(FRONT_SIZE);
    let root = front_path(nonce, 1);
    write!(
        h,
        "<html><head><title>HN front {p}</title></head><body class=\"front\">\
         <a href=\"{root}\">home</a> \
         <a href=\"http://localhost/{nonce}/offsite\">offsite</a>\
         <table>"
    )?;
    for id in front_ids(p) {
        let item = item_path(nonce, id);
        let title = story_title(id);
        let score = story_score_text(id);
        // BOTH anchors target the SAME item URL — deliberate duplicate
        // anchors for within-page dedup stress.
        write!(
            h,
            "<tr class=\"athing\" id=\"story-{id}\"><td>\
             <span class=\"titleline\"><a href=\"{item}\">{title}</a></span> \
             <span class=\"subline\"><span class=\"score\">{score}</span> \
             <a class=\"comments\" href=\"{item}\">{COMMENTS_PER_ITEM}&nbsp;comments</a></span>\
             </td></tr>"
        )?;
    }
    h.push_str("</table>");
    if p < FRONTS {
        let next = front_path(nonce, p + 1);
        write!(h, "<a class=\"morelink\" href=\"{next}\">More</a>")?;
    }
    h.push_str("</body></html>");
    pad_to(h, FRONT_SIZE, "front page")
}

fn render_item(nonce: &str, id: u64) -> anyhow::Result<Bytes> {
    let mut h = String::with_capacity(ITEM_SIZE);
    let root = front_path(nonce, 1);
    let title = story_title(id);
    let score = story_score_text(id);
    // No outgoing local links except the duplicate root (both the `home`
    // anchor and the title anchor point at it), plus the off-host trap.
    write!(
        h,
        "<html><head><title>Item {id}</title></head><body>\
         <div class=\"item-page\">\
         <a href=\"{root}\">home</a> \
         <a href=\"http://localhost/{nonce}/offsite\">offsite</a>\
         <span class=\"titleline\"><a href=\"{root}\">{title}</a></span> \
         <span class=\"score\">{score}</span>"
    )?;
    for k in 0..COMMENTS_PER_ITEM {
        let text = comment_text(id, k);
        write!(h, "<div class=\"comment\">{text}</div>")?;
    }
    h.push_str("</div></body></html>");
    pad_to(h, ITEM_SIZE, "item page")
}

// ---------------------------------------------------------------------------
// scenario constructor
// ---------------------------------------------------------------------------

/// Builds the `hn` scenario for the given run-nonce. The scenario has a fixed
/// size (PLAN.md §4 row 9); a `--depth` override is rejected.
pub fn build(nonce: &str, depth: Option<u32>) -> anyhow::Result<ScenarioSpec> {
    anyhow::ensure!(
        depth.is_none(),
        "scenario `hn` has a fixed size (1,040 pages); --depth is not supported"
    );

    let mut pages: BTreeMap<String, Bytes> = BTreeMap::new();
    let mut digest = Digest::default();

    // Item pages: 1 story record + 40 comment records each, hashed at
    // generation time with the same serialization the extractor uses.
    for id in 1..=ITEMS {
        let title = story_title(id);
        let score = story_score_text(id);
        digest.record(&story_record(&title, &score));
        for k in 0..COMMENTS_PER_ITEM {
            digest.record(&comment_record(&comment_text(id, k)));
        }
        pages.insert(item_path(nonce, id), render_item(nonce, id)?);
    }

    // Front pages: 25 titles each folded into the digest value (uncounted).
    for p in 1..=FRONTS {
        for id in front_ids(p) {
            front_title_fold(&mut digest, &story_title(id));
        }
        pages.insert(front_path(nonce, p), render_front(nonce, p)?);
    }

    anyhow::ensure!(
        digest.count == EXPECTED_RECORDS,
        "hn generation produced {} records, expected {EXPECTED_RECORDS}",
        digest.count
    );
    anyhow::ensure!(
        pages.len() as u64 == FRONTS + ITEMS,
        "hn generation produced {} pages, expected {}",
        pages.len(),
        FRONTS + ITEMS
    );

    let site = SiteSpec {
        pages,
        latency: None,
        redirects: BTreeMap::new(),
        gzip: false,
    };
    let expected = expected_from_site(&site, Some(digest.count), Some(digest.value()));
    Ok(ScenarioSpec {
        name: "hn",
        root_path: front_path(nonce, 1),
        site,
        expected,
        work: PageWork::Extract(extract),
    })
}
