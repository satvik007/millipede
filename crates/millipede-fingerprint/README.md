# millipede-fingerprint

Browser-like header generation and fingerprint hooks for the Millipede web crawler.

The crate ships a small, curated, committed set of realistic browser header profiles. It performs no
network fetch at build time or runtime. `HeaderGenerator` deterministically selects the same profile
for the same session seed, while `BrowserFingerprintGenerator` provides the header profile intended
for a browser `post_page_create` hook.

## v0.1 limitation

Fingerprinting in v0.1 provides header and browser-context consistency only. It does not spoof
JavaScript-visible navigator, canvas, or WebGL properties, and it makes no TLS, JA3, or JA4
fingerprinting claim. TLS impersonation would require a future alternate `HttpClient` backend and is
out of scope for v0.1.
