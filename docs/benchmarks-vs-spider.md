# Millipede vs spider — head-to-head benchmark report

**Date:** 2026-07-19 · **spider:** `=2.52.9` (features: `sync` only) · **millipede:** workspace `0.1.x`
**Machine:** Apple M2 Pro (aarch64, 32 GiB), macOS (Darwin 24.6.0), rustc 1.96.1, release profile
**Harness:** `benchmarks/spider-bench` (methodology in `benchmarks/PLAN.md`) · **Run:** commit `eaf507d` + uncommitted scenario implementations, results `1784470197-*`
**Suite runtime:** ~46 s (5 measured trials × 3 engines × 9 scenarios, interleaved, fresh child process per trial)

## Scope of claims

This suite measures **success-path HTTP/1.1 crawl throughput and peak RSS on loopback** against a synthetic axum site: identical page sets, fixed concurrency (C = 32), zero client-side delays, no retries, robots disabled on both engines, every trial validated by exact page counts, byte totals, and content digests. It does **not** characterize TLS, DNS, error/retry paths, anti-bot behavior, JS rendering, or politeness compliance. Live-site examples (`hackernews`, `scrape_books`) were never benchmarked directly; their workload shapes were replicated locally. A raw-`reqwest` baseline runs alongside both engines as the server-ceiling check. Deltas under 5 % are noise; never compare RSS across OSes.

## Results (median of 5 valid trials, C = 32, 4 runtime workers)

| scenario | workload | millipede | spider | ratio (mp/sp) | peak RSS mp / sp |
|---|---|---:|---:|---:|---|
| `hn` | 1,040 link-dense pages + selector extraction | 7,339 p/s (141 ms) | 2,299 p/s (452 ms) | **3.19× faster** | 20 / 54 MiB |
| `books` | 5,100-page catalog + selector extraction | 32,592 p/s (156 ms) | 15,819 p/s (322 ms) | **2.06× faster** | 18 / 45 MiB |
| `latency` | 2,047 pages, 10 ms server-side delay | 2,309 p/s | 2,284 p/s | ~tie (1.01×)¹ | 18 / 28 MiB |
| `wide` | 5,001 pages, 5,000-leaf fan-out | 47,305 p/s (105 ms) | 60,603 p/s (82 ms) | 0.78× | 29 / 33 MiB |
| `tree` | 8,191-page binary tree | 38,148 p/s (214 ms) | 56,002 p/s (146 ms) | 0.68× | 22 / 63 MiB |
| `mesh` | 8,192 pages, ~90 k link candidates (dedup stress) | 33,750 p/s (242 ms) | 56,048 p/s (146 ms) | 0.60× | 22 / 63 MiB |
| `compressed` | 4,095 gzip-encoded pages | 4,330 p/s (945 ms) | 20,092 p/s (203 ms) | 0.22× | 29 / 151 MiB |
| `payload` | 1,023 × 256 KiB pages | 1,053 p/s (971 ms) | 11,921 p/s (85 ms) | **0.09×** | 60 / 291 MiB |
| `redirects` | 2,047 pages behind 301s | 30,485 p/s (67 ms) | N/A² | — | 16 / — MiB |

¹ `latency` is flagged **server-bound** (both engines ≥ 85 % of the baseline ceiling; the 10 ms delay dominates at this page count) — treat as a tie, not publishable until scaled up.
² spider 2.52.9's SSRF guard blocks redirects to loopback addresses in every redirect policy; the loopback bench server cannot exercise its redirect path. The row measures millipede vs baseline only.

Full per-trial samples, IQR, wall min/max, CPU, connection counts, and wire-bytes columns: `benchmarks/spider-bench/target/results/1784470197-{summary.md,samples.jsonl,metadata.json}`.

## Analysis

**Millipede wins when extraction matters (the realistic scraping workloads).** On `books` and `hn`, millipede is 2–3× faster because `HtmlKind` parses each page exactly once and shares that DOM between link discovery and user extraction. Spider architecturally cannot do this: its internal lol_html pass extracts links only and exposes no DOM, so extraction requires a second, full parse of every page.

**The extraction re-parse charged to spider is exactly what spider's own APIs cost — verified.** A natural objection is that the harness should use "spider's native extraction API" instead of re-parsing with `scraper` in the subscriber. We checked: spider's native path is `spider_utils::css_query_select_map_streamed` (2.52.9), which is built on `spider_scraper` — described upstream as "a css scraper using html5ever", i.e. the same full-DOM parser family as the upstream `scraper` crate the harness uses. There is no spider API that reuses the internal link-parse for user extraction, so every spider extraction user pays this second parse. The harness's subscriber re-parse is equivalent work to the idiomatic spider path, not an artificial handicap. `scrape()` only changes page retention, not parsing.

**Spider wins raw fetch-and-discard throughput on small pages.** On `tree`/`mesh`/`wide` (no extraction), spider's streaming lol_html link discovery beats millipede's full-DOM `scraper` parse by 1.3–1.7×. The baseline shows the server ceiling at ~112 k p/s, so spider (~56 k) is at roughly half the ceiling — these ratios are, if anything, compressed by the server, not inflated.

**`payload` (0.09×) and `compressed` (0.22×) expose millipede's main optimization target.** With 256 KiB bodies, millipede burns ~3.9 s CPU per trial vs spider's ~0.3 s — the cost of building full `scraper` DOMs on large pages, plus gzip decompression sharing the same hot path in `compressed`. A streaming or lighter-weight link-extraction path for large bodies (lol_html-style, falling back to full DOM only when a handler needs it) is the single highest-leverage improvement suggested by this suite.

**Memory consistently favors millipede** — up to 5× lower peak RSS. Disclosed caveat: part of spider's RSS is the architectural cost of its `subscribe` broadcast channel, which (sized drop-free at `pages + 64` per the fairness plan) retains every page until crawl end because `subscribe` keeps an internal receiver that never reads. That is a real cost of consuming pages from spider, but it is a subscription-model cost, not crawl-core cost.

## Fairness controls (summary; details in `benchmarks/PLAN.md`)

- Identical page sets from a deterministic seeded generator; per-run nonce paths; identical concurrency (32), zero delays, no retries, robots disabled on both engines.
- HTTP clients pre-built outside the timed region on both sides; proxy env scrubbed (`NO_PROXY` pinned) so macOS system proxies cannot skew reqwest 0.13 (spider) vs 0.12 (millipede).
- Every page carries dedup-duplicate and off-host tripwire links; `/robots.txt` must receive zero hits; any invalid trial fails the suite.
- Each trial runs in a fresh child process; site corpus is shipped over a handshake so RSS excludes site-generation memory; trials interleaved M,S,B to spread thermal drift.
- Server-bound rows (engine ≥ 70 % of baseline ceiling) are marked and block publication.
- spider pinned at `=2.52.9` with minimal portable features (`sync`); a `spider-upstream-defaults` sensitivity feature exists for Linux runs with spider's default feature bundle.

## Reproducing

```text
cargo bench-spider orchestrate              # full suite, ~46 s + compile
cargo bench-spider orchestrate --quick      # 1 iter, no warmup, not publishable
cargo bench-spider orchestrate --scenario tree --depth 9 --quick
```

Known limitation of this run: `latency` needs a larger page count before its row is publishable, and the optional browser/Chromium suite (PLAN.md §10) is not yet implemented.
