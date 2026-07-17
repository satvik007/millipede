# ADR-0006: Chromiumoxide lifecycle and browser adapter surface

## Status

Accepted (Phase 6).

## Context

Phase 6 introduces the first browser provider and must establish that its native page and browser handles meet Millipede's asynchronous pool requirements before the adapter API is locked. The provider also needs deterministic browser discovery and lifecycle behavior without downloading executables at runtime. INTERFACE §12.1 sketches both provider-level page operations and the provider-erased `BrowserPage` interface in §12.2, even though every provider is required to adapt its native page type to `BrowserPage`.

The checked-in `millipede-browser` crate is still a skeleton: it does not define `BrowserHooks` or `post_page_create` yet. INTERFACE §12.1 currently sketches a generic `BrowserHooks<P>`. Before that API is implemented, its intended shape must be settled so provider generics do not leak into user-facing hooks.

## Decision

Millipede pins `chromiumoxide =0.9.1` exactly. Its declared Rust version is 1.85, matching the workspace MSRV gate; it uses edition 2024 and is Tokio-native. Default features are disabled and only `bytes` is enabled. The optional `fetcher` feature, which auto-downloads Chromium, is deliberately off. Browser executables are discovered through `MILLIPEDE_CHROME`, then `CHROME`, then a probe of well-known macOS and Linux paths.

The lifecycle spike establishes that `chromiumoxide::Page` is `Send + Sync + Clone`, `chromiumoxide::Browser` is `Send + Sync`, and a future holding a page across an `.await` remains `Send`. The `Handler` stream must be continuously driven by a spawned task. Normal shutdown calls `close().await` followed by `wait().await`, which reaps the child process. Chromiumoxide's `kill_on_drop` behavior remains the last-resort cleanup path.

Page-level operations live only on the object-safe `BrowserPage` trait. `BrowserProvider::Page: BrowserPage + Clone` is the erasure mechanism. This deliberately collapses INTERFACE §12.1's duplicated provider-level `goto`, `get_cookies`, and `set_cookies` methods: §12.2 already requires every provider to adapt its page type to `BrowserPage`, so retaining equivalent provider methods would create dead surface. This follows the standing direction to reference Crawlee without transliterating its API and instead expose what Rust users expect.

When `BrowserHooks` is introduced, it will be non-generic. `post_page_create` will be part of that initial surface for the Phase 7 fingerprint-injection path described by INTERFACE §12; only `post_launch` and browser-parameterized `pre_page_create` from the current INTERFACE sketch are deferred to Phase 7.

## Consequences

The Chromium adapter has a dependency and runtime baseline fixed to a version covered by the workspace's Rust 1.85 gate. It never downloads a browser implicitly, so local development and CI must provide a Chrome or Chromium executable through discovery.

The provider must own a handler-driving task for every launched browser and must prefer explicit close-and-wait shutdown. Dropping the browser is only a fallback. Page behavior has one object-safe home, avoiding parallel provider-level and erased APIs, while the deferred lifecycle hooks can be reconsidered if a concrete use case requires them.
