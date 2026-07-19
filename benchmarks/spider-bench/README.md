# Cross-language crawler benchmark harness

Head-to-head benchmark harness: **millipede**, [spider](https://crates.io/crates/spider)
2.52.9, [Colly](https://github.com/gocolly/colly) 2.3.0, and
[Crawlee](https://crawlee.dev/) 3.17.0 (plus a raw-reqwest "speed of light"
baseline), extending the approved methodology in
[`../PLAN.md`](../PLAN.md).

This is a standalone package (own `[workspace]` table, excluded from the root
workspace), so root CI never compiles spider. It builds with the repo-pinned
toolchain (`rust-toolchain.toml`, currently 1.96) and has no MSRV gate.

## Scope of claims

This suite measures success-path HTTP/1.1 (plus one redirect and one
compression scenario) crawl throughput and peak RSS against a synthetic axum
site on loopback, with identical page sets, fixed concurrency, zero
client-side delays, no retries, and robots disabled on all crawler engines. It does
**not** characterize TLS, DNS, error/retry paths, anti-bot behavior, JS
rendering, or politeness compliance. Live-network examples are never
benchmarked directly; their workload shapes are replicated locally. Never
publish a ratio without the absolute numbers and raw samples alongside.
Published numbers must name machine, OS, versions, feature sets, and the exact
command used. Never compare RSS across OSes; deltas under 5 % are noise.

## Running

From the repo root (aliases live in `.cargo/config.toml`):

```sh
cargo bench-spider-check                                     # compile check
cargo bench-spider orchestrate --iters 5 --concurrency 32    # full suite (release)
cargo bench-spider orchestrate --scenario tree --quick       # 1 trial, validation only, not publishable
cargo bench-spider report path/to/<ts>-samples.jsonl         # regenerate summary.md
```

`orchestrate` prepares the external runners before measurement: it builds
`../gocolly-bench` with Go and installs the lockfile-pinned `../crawlee-bench`
dependencies with `npm ci`. Go, Node.js, and npm must therefore be available.

Results land in `benchmarks/spider-bench/target/results/<timestamp>-{samples.jsonl,metadata.json,summary.md}`
(override with `--out`). A row is publishable only from a release-profile run
on an idle, AC-powered machine with ≥ 5 valid trials per engine; every trial
must pass the exact-count/byte/checksum validation gates (PLAN.md §8) or the
suite fails loudly.

Useful flags: `--concurrency` (default 32), `--runtime-workers` (default 4;
sets Rust's Tokio workers and Go's `GOMAXPROCS`; Crawlee runs JavaScript on
Node's single event-loop thread), `--depth` (depth-scalable scenarios),
`--sensitivity` (reserved for the clearly-labelled off-headline rows).

The `spider-upstream-defaults` cargo feature builds spider with its default
feature bundle for separately-labelled sensitivity rows (Linux); it is never
part of the headline table.

## Compliance

`deny.toml` here mirrors the root policy (bench-local additions are documented
inline); the manual `bench-compare` workflow runs
`cargo deny --manifest-path benchmarks/spider-bench/Cargo.toml check` plus a
smoke run. `Cargo.lock` is committed to pin spider's transitive graph.
