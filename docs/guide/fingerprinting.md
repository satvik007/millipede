# Fingerprinting in v0.1

Millipede v0.1 fingerprinting means deterministic header and browser-context consistency. A
session token (or another stable seed) selects one curated browser profile, including its user agent
and ordered `Accept-*` and `Sec-Ch-Ua-*` headers. Reusing the seed keeps those values consistent
across requests and freshly created generator instances.

## Enabling fingerprinting

Use `HttpKindBuilder::header_generator(true)` for HTTP requests. The builder installs its default
Millipede user agent before generated headers are applied, and the generator only fills headers
that are absent. Generated companion headers can therefore be selected deterministically, but the
generated user agent does not replace the builder's default user agent.

For browser pages, install a `BrowserFingerprintGenerator` on `BrowserHooks::defaults()` so the
standard bidirectional session-cookie synchronization hooks remain enabled, then pass those hooks
to the browser kind builder. Enable the umbrella crate's `fingerprint` feature to use its
`BrowserFingerprintGenerator` re-export:

```rust
use std::sync::Arc;
use millipede::{
    BrowserFingerprintGenerator, BrowserHooks, BrowserKind, ChromiumoxideProvider,
};

let hooks = BrowserHooks::defaults()
    .with_fingerprint(Arc::new(BrowserFingerprintGenerator::new()));
let kind = BrowserKind::builder(ChromiumoxideProvider)
    .hooks(hooks)
    .build()?;
```

## Limitations

This is header/context consistency, not complete browser impersonation. v0.1 does not spoof
JavaScript-visible navigator, canvas, or WebGL properties and does not provide JA3, JA4, or other
TLS fingerprinting. TLS impersonation requires a future alternate `HttpClient` backend, remains
out of scope for v0.1, and is tracked in
[TRACKED-8](../tracked-issues.md#tracked-8-add-tls-level-fingerprinting-through-an-alternate-httpclient).
