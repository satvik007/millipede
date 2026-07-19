# Millipede 0.1.0

Millipede 0.1.0 is ready for maintainer publication: a Crawlee-inspired, Tokio-based web-crawling library for Rust. It provides HTTP, HTML, and browser crawlers, including smart HTTP-first crawling that promotes pages to a browser when needed. Its core includes lease-based queues, sessions and proxies, autoscaling, Crawlee-compatible on-disk storage, and typed crawler errors.

The release also includes deterministic fingerprinting for browser-like headers and browser-context consistency. In 0.1, fingerprinting is header-level only and does not claim TLS-level JA3/JA4 impersonation. Millipede supports Rust 1.85 and is dual-licensed under MIT or Apache-2.0. Explore the [repository](https://github.com/satvik007/millipede), read the [guide](https://github.com/satvik007/millipede/tree/main/docs/guide), see [migrating from Crawlee](https://github.com/satvik007/millipede/blob/main/docs/guide/migrating-from-crawlee.md), or start from the [templates](https://github.com/satvik007/millipede/tree/main/templates).

Publishing to crates.io is still pending maintainer action. Feedback, bug reports, and use cases are welcome in the [issue tracker](https://github.com/satvik007/millipede/issues); proposals for community utilities should follow the [extras policy](https://github.com/satvik007/millipede/blob/main/docs/guide/extras.md).
