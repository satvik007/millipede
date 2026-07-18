# Millipede guide

Millipede is an idiomatic Rust web-crawling and scraping library inspired by Crawlee. It combines
a generic crawl engine with HTTP, parsed-HTML, browser, and smart crawler kinds, typed retry
semantics, pluggable storage, sessions, proxies, and operational controls. Millipede's minimum
supported Rust version (MSRV) is 1.85.

Most applications depend on the `millipede` umbrella crate. It re-exports the public APIs from the
smaller workspace crates so a project can use one dependency while feature flags keep optional
browser and file-system storage dependencies out of builds that do not need them. The default
features are `http`, `html`, and `storage-memory`. The default `http` feature pulls in
`millipede-fingerprint`, while the separate `fingerprint` feature exposes its generator types
directly from the umbrella crate.

## Guide contents

Read the chapters in this order when learning the library:

1. [Getting started](./getting-started.md) — install the umbrella crate and run HTTP, HTML,
   browser, and smart crawlers.
2. [Requests and storage](./request-storage.md) — understand queue leases, deduplication,
   datasets, key-value stores, saved state, and storage backends.
3. [Sessions and proxies](./sessions-and-proxies.md) — manage cookies and crawling identities,
   then select and report proxy health.
4. [Error handling](./error-handling.md) — classify failures, control retries and session
   rotation, capture snapshots, and inspect error statistics.
5. [Autoscaler](./autoscaler.md) — tune fixed concurrency, AIMD, load signals, rate limits, and
   per-domain politeness.
6. [Fingerprinting](./fingerprinting.md) — enable deterministic browser-like HTTP headers and
   browser-context consistency while understanding the v0.1 limits.
7. [Crawlee storage migration](./crawlee-storage-migration.md) — open a Crawlee-compatible
   `./storage` directory safely with the file-system backend.
8. [Migrating from Crawlee](./migrating-from-crawlee.md) — translate common Crawlee routing,
   enqueue, error, and persistence patterns into Rust.
9. [Extras policy](./extras.md) — learn which community helpers belong in `millipede-extras` and
   how a helper can graduate into core.

See the [benchmark baselines](../benchmarks.md) for queue, extraction, and per-request performance.
The runnable
[umbrella-crate examples](../../millipede/examples/) are the source of truth for complete programs;
guide snippets intentionally stay shorter.

## Choose a crawler kind

| Need | Kind and context | Feature |
|---|---|---|
| Fetch response bytes and headers | `HttpKind` and `HttpContext` | `http` |
| Parse server-rendered HTML | `HtmlKind` and `HtmlContext` | `html` |
| Drive Chromium | `BrowserKind<ChromiumoxideProvider>` and `BrowserContext` | `browser-chromiumoxide` |
| Start with HTTP and promote selected pages to Chromium | `SmartKind<ChromiumoxideProvider>` and `SmartContext` | `browser-chromiumoxide` plus `html` |

Every kind plugs into `Crawler<K>` through `CrawlerKind`. `CrawlerBuilder<K>` owns engine-wide
settings such as concurrency, retries, storage, the request handler, and crawl policy. Kind
builders own transport-specific settings such as HTTP sessions and proxies or browser launch
options.

## Feature map

| Feature | What it enables | Default |
|---|---|---|
| `http` | `millipede-http`, `HttpKind`, and `HttpCrawler` | Yes |
| `html` | `millipede-html`, `HtmlKind`, and `HtmlCrawler` | Yes |
| `storage-memory` | `MemoryStorageClient` | Yes |
| `storage-fs` | `FsStorageClient` and Crawlee-compatible disk layout | No |
| `browser` | Browser abstractions and pooling | No |
| `browser-chromiumoxide` | `ChromiumoxideProvider`; also enables `browser` | No |
| `fingerprint` | Header and browser fingerprint generators | No |

For example, an HTML crawler that persists results to disk can use:

```console
cargo add millipede --features storage-fs
```

A Chromium-backed crawler can use:

```console
cargo add millipede --features browser-chromiumoxide
```

## The recurring shape

Complete crawls follow the same lifecycle:

1. Build a concrete kind.
2. Pass it to `Crawler::builder`.
3. Supply a `StorageClient` and `request_handler`.
4. Add concurrency, retry, session, proxy, or policy settings.
5. Await `build()`.
6. Pass start URLs or requests to `run()`.
7. Read the returned `FinalStatistics`.

That shared engine shape is why moving from HTTP to HTML or browser crawling does not require
learning a separate scheduler. Continue with [Getting started](./getting-started.md) for runnable
examples, then use the topic chapters as configuration references.

## Reading conventions

Guide code blocks focus on the relevant API calls and may omit surrounding application setup.
Follow the linked example files for complete imports, local mock servers, feature flags, and error
handling. Public identifiers in these chapters follow the post-audit release-candidate surface.

The crawler performs network requests concurrently. Respect target-site policies, identify your
crawler where appropriate, set conservative concurrency and domain delays, and only crawl content
you are authorized to access.
