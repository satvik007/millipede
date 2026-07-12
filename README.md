# Millipede

An idiomatic Rust web-crawling library inspired by Crawlee.

> **Status: pre-alpha.** Phase 0 (the project skeleton) is complete, but the API does not exist yet. See the [roadmap](docs/ROADMAP.md) for the implementation plan and the [interface design](docs/INTERFACE.md) for the target API. The CI-green exit criterion is deferred until the workflows are exercised on the first push to GitHub.

## Workspace layout

| Crate | Purpose |
|---|---|
| `millipede` | User-facing umbrella crate that re-exports a curated public API. |
| `millipede-core` | Engine, requests, queues, autoscaling, sessions, proxies, storage traits, routing, events, errors, and statistics. |
| `millipede-storage-memory` | Default in-memory storage client. |
| `millipede-storage-fs` | File-system storage client with on-disk parity with memory storage. |
| `millipede-http` | Reqwest-based HTTP crawler and fetcher. |
| `millipede-html` | HTML crawler with `scraper`-based parsing. |
| `millipede-browser` | Browser crawler core, browser provider trait, and browser pool. |
| `millipede-browser-chromiumoxide` | Chromium CDP driver using `chromiumoxide`. |
| `millipede-fingerprint` | Browser-like header generation and TLS fingerprinting hooks. |

`millipede-cli` is deferred until post-MVP.

## Development

Millipede requires stable Rust 1.85 or newer. [`rust-toolchain.toml`](rust-toolchain.toml) pins the exact toolchain used for development.

Every change must pass:

```console
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps
```

Run the Phase 0 example with:

```console
cargo run -p millipede --example phase0_hello
```

See [CONTRIBUTING.md](docs/CONTRIBUTING.md) for the contribution workflow.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
