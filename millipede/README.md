# millipede

An idiomatic Rust web-crawling library inspired by Crawlee. This is the user-facing umbrella crate: it re-exports a curated API from the Millipede workspace crates behind feature flags, so projects that only need HTTP crawling never pull in browser/CDP dependencies.

| Feature | Crate | Default |
|---|---|---|
| `http` | `millipede-http` | Yes |
| `html` | `millipede-html` | Yes |
| `storage-memory` | `millipede-storage-memory` | Yes |
| `storage-fs` | `millipede-storage-fs` | No |
| `browser` | `millipede-browser` | No |
| `browser-chromiumoxide` | `millipede-browser-chromiumoxide` (also enables `browser`) | No |
| `fingerprint` | `millipede-fingerprint` | No |

`millipede-core` is always enabled.

**Status: pre-alpha skeleton.** This crate is part of the [Millipede](https://github.com/satvik007/millipede) workspace and does not yet expose a usable API. Real types land in later phases per `docs/ROADMAP.md`.
