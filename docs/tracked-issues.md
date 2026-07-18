# Tracked Issues Staging List

This file stages issues for the maintainer to file manually on GitHub after the phase closes.

## TRACKED-1: Define a runtime-mutable configuration subset

Labels: `enhancement`, `configuration`, `post-1.0`

Body: Define which settings on a built crawler may change safely at runtime, starting with log level, proxy strategy selection, and the autoscaler ceiling. The design should preserve the immutable `Configuration` contract described in `docs/INTERFACE.md` §14 while introducing synchronization only for the approved subset. The implementation would primarily land in `crates/millipede-core/src/config.rs` and the crawler engine code that reads those values.

## TRACKED-2: Add a `millipede-cli` scaffolder

Labels: `enhancement`, `cli`, `post-1.0`

Body: Build a post-1.0 `millipede-cli` scaffolder for commands such as `millipede new`, using the `templates/basic-http`, `templates/basic-html`, and `templates/basic-browser` starters shipped in Phase 8 as its initial project catalog. This extends the resolved scaffolding decision recorded in `docs/INTERFACE.md` §22 without changing the 0.1.0 API. The work would land in a new `crates/millipede-cli/` crate and consume the templates maintained under `templates/`.

## TRACKED-3: Implement a Redis-backed `RequestQueue`

Labels: `enhancement`, `storage`, `distributed`, `post-1.0`

Body: Add a Redis-backed implementation of the lease-based `RequestQueue` contract described in `docs/INTERFACE.md` §8.1 and tracked in §22. It must preserve deduplication and correctly implement lease expiry, renewal, reclaim, abandon, and handled transitions across multiple processes. The implementation would land in a dedicated crate such as `crates/millipede-storage-redis/`, against the traits in `crates/millipede-core/src/storage/queue.rs`.

## TRACKED-4: Introduce typed `CrawlError` source chains before 1.0

Labels: `enhancement`, `errors`, `before-1.0`

Body: Revisit the `anyhow::Error` sources retained for 0.1.0 and define typed source chains where structured matching provides practical value. The result must preserve the retry classifications and conversion behavior specified in `docs/INTERFACE.md` §16, with the follow-up decision recorded in §22. The primary implementation location is `crates/millipede-core/src/errors.rs`, with conversions updated in the HTTP, HTML, browser, and storage crates as needed.

## TRACKED-5: Provide a `UserData` derive macro

Labels: `enhancement`, `macros`, `post-1.0`

Body: Explore a `#[derive(UserData)]` macro that generates typed accessors while retaining the map-backed interoperability of `UserData` described in `docs/INTERFACE.md` §3 and deferred in §22. The design should specify serialization, missing-field, and schema-evolution behavior before stabilizing generated APIs. The macro would land in a new proc-macro crate, with its runtime-facing types remaining in `crates/millipede-core/src/request.rs`.

## TRACKED-6: Add a `millipede-storage-apify` platform client

Labels: `enhancement`, `storage`, `apify`, `post-1.0`

Body: Implement the Apify platform storage backend anticipated in `docs/INTERFACE.md` §8.2 and the deployment follow-up in §22. It should provide the existing dataset, key-value store, and request queue contracts over the platform API, including real queue lease expiry. The work would land in a new `crates/millipede-storage-apify/` crate built against the storage traits in `crates/millipede-core/src/storage/`.

## TRACKED-7: Create the `millipede-extras` crate

Labels: `enhancement`, `extras`, `community`, `post-1.0`

Body: Create the independently versioned `millipede-extras` crate according to `docs/guide/extras.md` and the ecosystem decision in `docs/INTERFACE.md` §22. Seed it only with helpers that compose public APIs, include tests and documentation examples, and do not require core internals. The implementation would land in a new `crates/millipede-extras/` crate with CI matching core's gates except for public API diffing.

## TRACKED-8: Add TLS-level fingerprinting through an alternate `HttpClient`

Labels: `enhancement`, `http`, `fingerprinting`, `post-1.0`

Body: Evaluate and implement an alternate `HttpClient` backend that provides TLS-level fingerprint impersonation rather than only header and browser-context consistency. The backend must honor the stable abstraction and response semantics in `docs/INTERFACE.md` §9 and the explicit limitation in §21. The implementation should land in a dedicated backend crate, integrating with `crates/millipede-core/src/http_client.rs` rather than weakening the default client or custom TLS verification.

## TRACKED-9: Add WebDriver BiDi and Playwright-style browser providers

Labels: `enhancement`, `browser`, `providers`, `post-1.0`

Body: Reassess WebDriver BiDi support and a Playwright-style provider after their Rust ecosystems and deployment tradeoffs mature. Any provider must implement the capability-aware, potentially lossy behavior described in `docs/INTERFACE.md` §12 without exposing provider generics to handlers. The work would land in dedicated provider crates alongside `crates/millipede-browser-chromiumoxide/`, using the traits and adapters in `crates/millipede-browser/src/provider.rs` and `crates/millipede-browser/src/page.rs`.
