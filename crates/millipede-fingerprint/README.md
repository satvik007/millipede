# millipede-fingerprint

Browser-like header generation and fingerprint hooks for the Millipede web crawler.

[![crates.io](https://img.shields.io/crates/v/millipede-fingerprint.svg)](https://crates.io/crates/millipede-fingerprint) [![docs.rs](https://docs.rs/millipede-fingerprint/badge.svg)](https://docs.rs/millipede-fingerprint) [![license](https://img.shields.io/crates/l/millipede-fingerprint.svg)](https://github.com/satvik007/millipede#license)

This crate gives [Millipede](https://github.com/satvik007/millipede) deterministic browser-like user-agent and companion-header profiles for HTTP sessions and browser hooks, using a curated dataset bundled with the crate.

## Installation

```toml
[dependencies]
millipede-fingerprint = "0.1"
```

Most users should depend on the umbrella [`millipede`](https://crates.io/crates/millipede) crate instead.

## Example

```rust
use millipede_fingerprint::HeaderGenerator;

let first = HeaderGenerator::new().generate("session-42");
let second = HeaderGenerator::new().generate("session-42");

assert_eq!(first, second);
println!("{}", first.user_agent);
```

The generated profiles align HTTP headers and browser context settings; they do not claim JavaScript-visible or TLS fingerprint spoofing.

## Part of Millipede

See the [Millipede guide](https://github.com/satvik007/millipede/tree/main/docs/guide) for fingerprint consistency, supported integration points, and limitations.

## License

Licensed under either **MIT OR Apache-2.0** at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this crate is dual-licensed as above, without any additional terms or conditions.
