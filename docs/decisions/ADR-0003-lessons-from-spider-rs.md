# ADR-0003: Lessons from spider-rs

## Status

Accepted (Phase 1)

## Context

`spider-rs/spider` is the closest prior art for a Rust crawler and was reviewed while designing Millipede. Its operational experience validates useful crawler patterns, while its architecture also highlights choices that do not fit Millipede. This ADR pins what we borrow and what we reject so that the rationale survives implementation.

## Decision

Millipede borrows these patterns, with each assigned to an explicit landing point:

- Real-time result streaming lands in Phase 2 as `Crawler::results()` returning a `ResultStream`.
- A small builder provides the happy path without hiding advanced configuration through `ConfigurationBuilder` in Phase 1 and the `Crawler<Kind>` builders from Phase 2 onward.
- All-atomic additive-increase/multiplicative-decrease is the first deterministic autoscaling mode, implemented by `AimdController` in Phase 4.
- Borrowed-context retry and proxy strategy hooks land in Phase 3 as `RetryStrategy` with `AttemptOutcome<'_>` and `ProxyStrategy` with `ProxyRouteContext<'_>`.
- Domain-round-robin frontier ordering has already landed in Phase 1 as `MemoryQueuePolicy::DomainRoundRobin` in `millipede-storage-memory`. The policy is pure and unit-tested; FIFO remains the default.
- In-flight request coalescing sits above `HttpClient` and lands in Phase 3, independently of queue deduplication.
- Streaming link extraction with `lol_html` is reserved for the engine hot path and will be benchmarked in Phase 5.
- Browser page cleanup uses a `PageHandle` RAII guard backed by a background close worker in Phase 6, while explicit asynchronous close remains preferred.

Millipede rejects these anti-patterns and records the corresponding counter-decisions:

- A single all-purpose crawler struct is replaced by `Crawler<Kind>` and the multi-crate workspace split.
- Synthetic HTTP status codes standing in for failures are replaced by the typed `CrawlError` taxonomy that landed in Phase 1.
- Global, environment-driven semaphores are replaced by builder-owned explicit configuration. Environment variables may only provide inputs to `ConfigurationBuilder::build`.
- Shared tuple task contexts accessed by numeric index are replaced by named `TaskCtx` structs, as required by the risk register.
- Unsafe client-build shortcuts are replaced by typed client-build errors, with workspace-wide `unsafe_code = "deny"` enforcement.
- A large default feature matrix in the user-facing crate is replaced by minimal umbrella defaults: `http`, `html`, and `storage-memory`.

## Consequences

The borrowed items are tracked in their assigned phase scopes, and any deviation from this list requires updating this ADR. `cargo-semver-checks` is deliberately not added to CI yet: every crate is `publish = false`, so there is no crates.io release to compare against. It is deferred until the first publish in Phase 8, consistent with the intent of the ROADMAP Release Discipline section.
