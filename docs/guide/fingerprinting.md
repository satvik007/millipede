# Fingerprinting in v0.1

Millipede v0.1 fingerprinting means deterministic header and browser-context consistency. A
session token (or another stable seed) selects one curated browser profile, including its user agent
and ordered `Accept-*` and `Sec-Ch-Ua-*` headers. Reusing the seed keeps those values consistent
across requests and freshly created generator instances.

## Enabling fingerprinting

Use `HttpKindBuilder::header_generator(true)` for HTTP requests. For browser pages, install a
`BrowserFingerprintGenerator` with `BrowserHooks::with_fingerprint`; both paths select from the
same committed deterministic profiles.

## Limitations

This is header/context consistency, not complete browser impersonation. v0.1 does not spoof
JavaScript-visible navigator, canvas, or WebGL properties and does not provide JA3, JA4, or other
TLS fingerprinting. TLS impersonation requires a future alternate `HttpClient` backend and remains
out of scope for v0.1.
