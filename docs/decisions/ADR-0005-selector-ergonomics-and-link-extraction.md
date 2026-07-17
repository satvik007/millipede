# ADR-0005: Selector ergonomics and link extraction

## Status

Accepted.

## Context

INTERFACE.md §22 left four candidates open for Phase 5 measurement:

- “A `selectors!("a.detail", "h1 > span", …)` macro that expands to `static` items behind `OnceLock<Selector>`.”
- “A `Selectors` registry on `HtmlContext` (`ctx.selectors.parse_or_get("a.detail")`) for handlers that compute selector strings dynamically.”
- “Status quo: users call `Selector::parse(…).unwrap()` inline.”
- “A streaming `lol_html` extractor used only by `EnqueueLinker` for the engine hot path, while handlers still see `scraper::Html`.”

The CTO rule is to ship the user-facing ergonomics whose cost lands closest to inline-parse time without committing Millipede to unnecessary runtime API surface. `lol_html` may be used internally for `EnqueueLinker` only on a material win. A material win is defined, before examining the results, as at least 25% lower Criterion slope point estimate or at least 30% lower peak allocation than extraction over the already-parsed document, with no regression greater than 10% in the other metric. Criterion's median estimate is a secondary sanity check for the timing direction.

The comparison point for engine extraction is the already-parsed document. `HtmlContext` pays for the scraper parse regardless and stores the result in `Arc<millipede_html::SynchronizedHtml>`, a synchronized wrapper around `scraper::Html` that exposes synchronous query helpers; parsing the same response again with a streaming extractor is additional marginal work.

The wrapper is a soundness requirement, not an ergonomic preference. With scraper 0.24's
`atomic` feature, `scraper::Html` is `Send` but not `Sync`: element `id` and `classes` caches are
`std::cell::OnceCell` values populated through `&self`, and tendril's atomic representation has a
`Send` implementation but no `Sync` implementation. Since `Arc<T>: Send` requires `T: Send +
Sync`, `Arc<scraper::Html>` cannot be moved into the engine's `Send + 'static` handler future.
Marking it `Sync` or marking a context containing it `Send` would permit unsynchronized concurrent
cache mutation and would be unsound.

## Measurements

`cargo bench -p millipede-html --bench selector_bench` was run with rustc 1.96.1 (`aarch64-apple-darwin`) on a 12-core Apple M2 Pro MacBook Pro with 32 GB RAM. At each approximate size, the deterministic in-bench corpus crosses product and category page kinds with `<base href>` present and absent, producing four documents per batch. Every document contains exactly 200 relative, root-relative, same-site absolute, and external links. Repeated nested `div` blocks scale the DOM with document size; only a final sub-block remainder is text-filled. The 1 MB group uses 20 Criterion samples. Extraction values are per four-document batch (800 links). Values below are Criterion's slope point estimates with the reported confidence interval in parentheses, except the flat-sampled 100 KB and 1 MB d1 cells, for which Criterion does not produce a slope and the mean estimate is reported. Median estimates were checked as a secondary sanity figure.

| Selector operation | Time per lookup |
|---|---:|
| Inline `Selector::parse` | 250.16 ns (249.60–250.77 ns) |
| `selectors!` / cached `OnceLock` | 0.451 ns (0.424–0.479 ns) |
| `HashMap<String, Selector>` lookup-or-parse, warm entry | 23.682 ns (23.176–24.387 ns) |

| Corpus batch (four documents; total bytes) | scraper full parse + select (d1) | scraper pre-parsed select (d2) | `lol_html` streaming (d3) |
|---|---:|---:|---:|
| ~10 KB each (49,214) | 993.82 µs (991.31–996.87 µs) | 45.656 µs (45.589–45.726 µs) | 274.82 µs (274.54–275.15 µs) |
| ~100 KB each (409,600) | 7.9821 ms (7.9743–7.9909 ms) | 136.25 µs (131.90–141.64 µs) | 1.1003 ms (1.0912–1.1132 ms) |
| ~1 MB each (4,194,304) | 84.520 ms (84.184–84.828 ms) | 1.0190 ms (1.0170–1.0207 ms) | 9.5815 ms (9.5726–9.5945 ms) |

Criterion does not instrument peak allocations, so this run makes no allocation-reduction claim. The streaming path nevertheless cannot meet the predeclared material-win rule: relative to d2 it regresses the slope point estimate by approximately 502%, 708%, and 840%, respectively, already exceeding the allowed 10% regression in the other metric. The richer DOM makes selector traversal scale with page complexity while holding link count fixed, and the decision remains unchanged.

The Phase 5 dependency commit also made two deliberate deviations from the Phase 0 locked dependency table. `psl` was added for correct eTLD+1 same-domain matching, including suffixes such as `.co.uk`; `criterion` was added to execute the selector/link-extraction decision required by INTERFACE.md §22. Both passed the Rust 1.85 MSRV gate in that commit.

## Decision

Ship the `selectors!` macro in `millipede-html`. It creates `OnceLock`-backed selector accessor functions, costs effectively only the cached lookup after first use, and adds no registry or other runtime API surface to support.

Do not ship `ctx.selectors` or a `Selectors` registry. Dynamic selector strings are rare, and the warm registry lookup is over 40 times slower than the macro path while adding state and a permanent public API. Users with dynamic selectors can parse inline or maintain an application-local cache.

Keep `EnqueueLinker` extraction on the pre-parsed scraper document held by `HtmlContext` as `Arc<millipede_html::SynchronizedHtml>`. The wrapper owns the `scraper::Html` behind a mutex, exposes owned synchronous query helpers, and provides `lock()` for complete scraper API compatibility through `MutexGuard`'s `Deref<Target = scraper::Html>`. The guard must be dropped before `.await`. That is the real marginal cost after `HtmlContext` has paid for parsing; running `lol_html` over the bytes as well is a net loss. `lol_html` could matter for a hypothetical parse-free HTTP-side extraction path, but Phase 5 has no such path and does not change `HtmlContext`.

## Consequences

Handlers can declare selectors once with a small macro and use ordinary `scraper::Selector` references. Selector syntax remains validated lazily on first access, and an invalid literal panics with the selector and parser error. `scraper` is publicly re-exported from `millipede-html` so macro expansion through `$crate` works in downstream crates and handler code can use the same scraper types.

Compile-time coverage asserts `Arc<SynchronizedHtml>: Send + Sync`, while a compile-fail guard
asserts that `Arc<scraper::Html>: Send + Sync` remains rejected. Callers retain access to the full
scraper document API through the synchronized dereferencing guard without inventing an unsafe
auto-trait implementation.

There is no Millipede-owned dynamic selector cache to configure, synchronize, document, or preserve for compatibility. Applications that genuinely need dynamic strings own that policy themselves.

Engine link extraction reuses the existing DOM and does not add `lol_html` to the production dependency graph. A future parse-free HTTP extraction design would require a new benchmark and ADR because its cost model differs from `HtmlContext`.
