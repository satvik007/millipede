# Fingerprinting in v0.1

Millipede v0.1 fingerprinting means deterministic header and browser-context consistency. A
session token (or another stable seed) selects one curated browser profile, including its user agent
and ordered `Accept-*` and `Sec-Ch-Ua-*` headers. Reusing the seed keeps those values consistent
across requests and freshly created generator instances.

## Enabling fingerprinting

Later Phase 7 integration steps will expose `HttpKindBuilder::header_generator(true)` for HTTP
requests and `BrowserHooks::with_fingerprint` for browser pages. Those integration APIs are not
wired by this step; this crate currently supplies the deterministic profiles and generators they
will use.

## Limitations

This is header/context consistency, not complete browser impersonation. v0.1 does not spoof
JavaScript-visible navigator, canvas, or WebGL properties and does not provide JA3, JA4, or other
TLS fingerprinting. TLS impersonation requires a future alternate `HttpClient` backend and remains
out of scope for v0.1.
