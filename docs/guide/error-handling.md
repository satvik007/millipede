# Error handling and retries

Handlers return `Result<(), CrawlError>`. Without a custom `RetryStrategy`, the variant supplies the
engine's default retry and session policy. A configured strategy has full authority over
`RetryDirective::should_retry` for non-critical failures and can override that default, including
retrying a normally non-retryable error. Critical errors and requests marked `no_retry` do not reach
the strategy.

## The typed error taxonomy

`CrawlError` is non-exhaustive and currently exposes these variants. The retry descriptions below
are the defaults when no custom `RetryStrategy` is configured:

- `Retry(error)` retries and counts against `max_request_retries`.
- `Session(error)` retries with a session rotation and counts against
  `max_session_rotations`, not the ordinary request retry limit.
- `ForceRetry(error)` retries while ignoring `max_request_retries`.
- `NonRetryable(error)` does not retry and proceeds to the failed-request handler.
- `Critical(error)` aborts the crawler.
- `MissingRoute { label, method }` reports that no router entry matched both request label and
  HTTP method.
- `AntiBotDetected { tech, source }` reports a recognized anti-bot or web application firewall
  response and rotates the session on retry.

Constructor helpers keep handler code concise: `CrawlError::retry`, `session`, `force_retry`,
`non_retryable`, and `critical` wrap any value convertible into `anyhow::Error`.

```rust
use millipede::CrawlError;

if ctx.response.status.as_u16() == 503 {
    return Err(CrawlError::retry(anyhow::anyhow!("temporary 503")));
}
if ctx.response.status.as_u16() == 403 {
    return Err(CrawlError::session(anyhow::anyhow!("blocked identity")));
}
Ok(())
```

`is_retryable`, `rotates_session`, `ignores_max_retries`, `is_critical`, and
`counts_against_retries` expose the same classification for custom policy code.

## Retry machinery

`CrawlerBuilder::max_request_retries` controls ordinary retries. Installing a custom
`RetryStrategy` with `retry_strategy(strategy)` gives the strategy an `AttemptOutcome` and lets it
return a `RetryDirective`; `RetryDirective::should_retry` decides eligibility, and the strategy's
`max_retries()` supplies its ordinary retry limit. This decision can retry `NonRetryable` or stop a
normally retryable variant; it cannot override `Critical` or a request's `no_retry` flag.

`HttpKindBuilder::retry_status_codes` replaces the explicit retryable status set.
`retry_server_errors(bool)` separately controls retries for server-error responses. A
`ForceRetry` is special because `ignores_max_retries()` is true.

```rust
let kind = millipede::HttpKind::builder()
    .retry_status_codes([408, 429])
    .retry_server_errors(true)
    .build()?;

let crawler = millipede::Crawler::builder(kind)
    .max_request_retries(3)
    .retry_strategy(strategy)
    .storage_client(storage)
    .request_handler(handler)
    .build()
    .await?;
```

The engine stores error messages on the request as attempts fail. Exhausted requests, and failures
for which the active default or custom retry policy stops retrying, transition to the
permanent-failure path.

## Failed request handler

Register `failed_request_handler` on `CrawlerBuilder`. It receives a `FailedRequestContext` with
the terminal `request` and `error` and returns `Result<(), CrawlError>`.

```rust
.failed_request_handler(|ctx: millipede::FailedRequestContext| async move {
    eprintln!("failed to crawl {}: {}", ctx.request.url, ctx.error);
    Ok(())
})
```

The callback is the right place for terminal logging, dead-letter storage, or alerts. A `Critical`
error from request processing aborts the run instead of treating the request as an isolated
failure; errors returned by the failed-request handler itself are logged.

## Blocked responses and session rotation

HTTP sessions are enabled by default. `HttpKindBuilder::session_status_codes` defines response
codes classified as session errors. Those responses retire or rotate the current identity and are
bounded by `CrawlerBuilder::max_session_rotations`.

This budget is deliberately separate from `max_request_retries`: a blocked cookie/IP identity can
be replaced without consuming the transient network retry budget. `Session` and
`AntiBotDetected` both report `rotates_session()` as true.

## Anti-bot detection

`AntiBotDetector` inspects `AntiBotSignals` and returns an optional `AntiBotTech`.
`DefaultAntiBotDetector` uses bounded static response markers for Cloudflare, DataDome,
PerimeterX, Kasada, Imperva, and Akamai; it can also emit `Custom(String)` or `Unknown`.

Enable the default detector with `HttpKindBuilder::detect_anti_bot_default()`, or install an
`Arc<dyn AntiBotDetector>` with `detect_anti_bot(detector)`. The default detector can be tuned with
`with_inspection_limit` and `with_custom_marker` before installation.

```rust
let kind = millipede::HttpKind::builder()
    .detect_anti_bot_default()
    .build()?;
```

A detected challenge becomes `CrawlError::AntiBotDetected { tech, source }`, which is retryable
and rotates the session.

## Error snapshots

`ErrorSnapshotter` stores failure artifacts in a `KeyValueStore`. HTTP kinds enable handler-failure
body capture with `snapshot_errors_on_failure(true)`. Execute-time failures that occur before a
handler context exists have no response body to capture.

The deterministic base key is `ERROR_SNAPSHOT_` followed by a 16-digit lowercase hexadecimal hash
of `request.unique_key`. `capture` appends `.{suffix}`; the HTTP handler-failure path uses the
`body` suffix, producing `ERROR_SNAPSHOT_{hash}.body`.

```rust
let base = millipede::ErrorSnapshotter::base_key(&request);
let body_key = format!("{base}.body");
let snapshot = millipede::ErrorSnapshotter::new(kvs)
    .load(&body_key)
    .await?;
```

## Statistics and skipped URLs

`StatisticsSnapshot` and `FinalStatistics` expose `errors` for terminal failures and
`retry_errors` for retry attempts. Both are `BTreeMap<String, u64>` groups. Millipede normalizes
the first error line by masking URLs and UUIDs, collapsing digit runs, and limiting key length, so
request-specific values do not fragment counts unnecessarily.

Link admission failures are observable before navigation. `EnqueueResult.skipped` contains
`SkippedUrl { url, reason }`. `SkipReason` currently distinguishes `MaxDepthExceeded`,
`MaxRequestsReached`, `StrategyExcluded`, `GlobExcluded`, `RegexExcluded`, `TransformRejected`,
`DuplicateUniqueKey`, and `InvalidUrl`. A `CrawlPolicy` can register `on_skipped` to observe each
rejection as it happens.

See the complete [error handling example](../../millipede/examples/error_handling.rs). The
[fingerprint crawl example](../../millipede/examples/fingerprint_crawl.rs) additionally exercises
anti-bot recovery, snapshot loading, and normalized error groups against an offline mock client.
