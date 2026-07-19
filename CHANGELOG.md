# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## millipede-core [0.1.1] - 2026-07-19

### Fixed

- Migrated the sitemap parser to `quick-xml` 0.41, resolving the
  RUSTSEC-2026-0194 and RUSTSEC-2026-0195 advisories against the 0.37
  line. Entity references in sitemap text (for example `&amp;` inside
  `<loc>` URLs) continue to resolve under the new event model.

## [0.1.0] - 2026-07-18

### Added

- Phase 1: a typed request model and lease-based request queues.
- Phase 2: the crawler engine loop with routing, retries, and lifecycle events.
- Phase 3: `HttpCrawler`, session pooling, and proxy configuration.
- Phase 4: autoscaled concurrency and rate limiting.
- Phase 5: `HtmlCrawler`, routing, `enqueue_links`, sitemap support, and file-system storage compatible with Crawlee's on-disk layout.
- Phase 6: `BrowserCrawler`, the chromiumoxide provider, and smart HTTP-first promotion to browser crawling.
- Phase 7: deterministic header fingerprinting, anti-bot detection, and error snapshots.
- Phase 8: the public API audit and release collateral, including documentation, examples, benchmarks, and project templates. The `PageOpts` to `PageOptions` and `SessionPool::get_session` to `SessionPool::session` breaks are accepted 0.x API changes.

[Unreleased]: https://github.com/satvik007/millipede/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/satvik007/millipede/releases/tag/v0.1.0
