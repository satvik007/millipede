# ADR-0001: MSRV and async-fn-in-trait policy

## Status

Accepted (Phase 0).

## Context

`docs/ROADMAP.md` originally set the MSRV to Rust 1.75 because it stabilized async functions in traits. The workspace uses edition 2024, which requires rustc 1.85 or newer, so an MSRV of 1.75 is unsatisfiable. The roadmap's Decision Log Hooks section schedules ADR-0001, “MSRV and async-fn-in-trait policy,” for the close of Phase 0.

## Decision

The MSRV is stable Rust 1.85. It is declared as `rust-version` in `[workspace.package]` and enforced by a dedicated CI job running `cargo +1.85 check`. Raising the MSRV is a minor (`0.x.0`) change and requires updating this ADR.

Use native `async fn` in traits for internal or otherwise non-object-safe traits. Use `#[async_trait]` from the `async-trait` crate where the public API requires dyn object safety, including the storage traits in `INTERFACE.md` section 8.1. Re-evaluate this policy in each phase as language support for dyn-compatible async traits evolves.

## Consequences

Edition 2024 idioms are available throughout the workspace. Contributors using Rust older than 1.85 are unsupported. We accept the small `async-trait` dependency at object-safe boundaries.
