# Contributing to Millipede

## Toolchain

Development uses stable Rust through `rust-toolchain.toml`, currently pinned to 1.96. The minimum supported Rust version (MSRV) is 1.85 and CI checks it with `cargo +1.85 check`; the explicit `+1.85` bypasses the repository toolchain pin.

## Quality gate

Every commit must pass:

```console
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps
```

Write tests alongside code, not after it. Every roadmap phase ends with a runnable example under `examples/`.

## Workflow

Keep commits small and focused. Use conventional-commit style subjects such as `feat:`, `fix:`, `chore:`, `docs:`, `ci:`, and `test:`. Open pull requests against `main`; CI must be green before merge.

## Design ground rules

- Reference Crawlee, but do not transliterate it. Prefer what a Rust user would expect.
- The public API may break until 1.0: `0.x.0` may break compatibility; `0.x.y` may not.
- New dependencies must come from the locked [Dependency Choices](ROADMAP.md#dependency-choices-locked-at-phase-0) table or be justified by an ADR under `docs/decisions/`.

## Licensing

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed under Apache-2.0 or MIT at your option, without any additional terms or conditions.
