# ADR-0007: Public API conventions for 0.1.0

## 1. `PageOptions` naming

Rename `PageOpts` to `PageOptions` because it was the workspace's only abbreviated `*Opts` type and 0.1.0 is the last free rename point.

## 2. Passive browser option setters

Use `PageOptions::with_session`, `PageOptions::with_extra_headers`, `GotoOptions::with_timeout`, and `GotoOptions::with_wait_until` because passive option values follow the workspace-dominant `with_*` convention; `ScreenshotOptions` remains field-only because it has no consuming setters.

## 3. Session pool checkout naming and return type

Rename `SessionPool::get_session` to `SessionPool::session` and retain `Arc<Session>` because checkout is intentionally infallible; the `Result<Arc<Session>>` shown in `INTERFACE.md` section 10 is stale and is reconciled later.

## 4. Typed `get_*` pairs

Keep `KeyValueStore::get_bytes` and `UserData::get_typed` because their suffixes disambiguate legitimate typed access pairs rather than adding redundant getter noise.

## 5. Router fallback spelling

Keep `Router::<C>::default(self, handler) -> Self` because it preserves Crawlee `addDefaultHandler` parity, matches `INTERFACE.md`, and cannot collide with an intentionally absent `std::default::Default` implementation.

## 6. Proxy construction helpers

Keep async `ProxyConfiguration::new_url` and `ProxyConfiguration::new_proxy_info` because their names and signatures already match `INTERFACE.md` section 11 exactly.

## 7. Two-tier consuming-setter rule

Use bare verbs for fluent build/send-terminated builders (`CrawlerBuilder`, `HttpKindBuilder`, `HtmlKindBuilder`, `ConfigurationBuilder`, `RequestBuilder`, and the enqueue pipeline) and `with_*` for passive values (`SessionConfig`, `SessionPoolOptions`, `ReqwestClientOptions`, `ProxyBuckets`, `DefaultAntiBotDetector`, `DefaultPromotionDetector`, `ChromiumLaunchOptions`, `PageOptions`, and `GotoOptions`) because the terminal operation distinguishes actions from configuration.

## 8. `EnqueueLinksOptions` accepted deviation

Keep the `EnqueueLinksOptions` name and its bare consuming setters because it behaves as the Crawlee-parity `enqueueLinks(options)` pipeline builder despite its options-shaped name.

## 9. Extensible enums

Mark `CrawlError`, `AntiBotTech`, `ScaleDecision`, `AutoscaleMode`, `ConfigError`, `CookieJarError`, `CrawlerBuildError`, `SkipReason`, `CrawlerEvent`, `RequestFinalState`, `HttpClientError`, `UrlPattern`, `LinkPatternError`, `TransformResult`, `MethodFilter`, `ProxyKind`, `RotationStrategy`, `RequestBuildError`, `SessionRetryAction`, `RequestSource`, `StorageError`, `HtmlError`, `BrowserError`, `PromotionReason`, `WaitUntil`, `SmartContext`, and `MemoryQueuePolicy` non-exhaustive because their taxonomies may grow without a major release.

## 10. Closed enums

Keep `EnqueueStrategy`, `LogLevel`, `RequestState`, `RequestBody`, `RequestOutcome`, and `SameSite` exhaustive because they are deliberately closed strategy, level, lifecycle, protocol, or standards-defined sets where complete matching is useful.

## 11. Extensible options

Mark every public options struct non-exhaustive, including autoscaler signal/status/pool options and `EnqueueLinksOptions`, because adding configuration fields must remain semver-compatible.

## 12. Must-use values

Mark builders, passive consuming configuration values, enqueue/queue completion results, batch handles, and final crawler statistics `#[must_use]` because silently dropping them almost always indicates an incomplete operation.

## 13. Umbrella completeness

Re-export every user-facing core result/error, storage implementation, browser hook, synchronized HTML, anti-bot, error-snapshot, and fingerprint type through `millipede` under its owning feature because the umbrella crate is the canonical ergonomic import surface.

## 14. Documentation lint policy

Rely on the workspace `missing_docs = "warn"` lint plus warning-denying verification instead of adding redundant per-crate lint attributes because documentation coverage is already structurally enforced.

## 15. `BrowserHooks` registration spelling

Keep `BrowserHooks::push_*` for additive hook-registration methods because they append callbacks rather than replace passive configuration values and therefore are not setters governed by the `with_*` tier.
