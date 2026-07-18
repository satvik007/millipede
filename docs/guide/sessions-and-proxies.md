# Sessions and proxies

Sessions give requests a reusable crawling identity: cookies, user data, an error score, a usage
count, and a stable `SessionId`. Proxy configuration independently decides how a request reaches
its target. `HttpKind` integrates both and exposes the selected values as `ctx.session` and
`ctx.proxy_info`.

## Session lifecycle

Create a `SessionPool` with `SessionPoolOptions`. Passive option types use `with_*` setters:

```rust
use millipede::{SessionConfig, SessionPool, SessionPoolOptions};

let options = SessionPoolOptions::default()
    .with_max_pool_size(200)
    .with_session_config(
        SessionConfig::default()
            .with_max_usage_count(40)
            .with_max_error_score_scaled(3_000),
    )
    .with_persist_state_key("MY_SESSION_POOL");
let pool = SessionPool::new(options);
```

`pool.session(None).await` checks out a usable session. Passing `Some(&session_id)` requests a
sticky session; if it is unavailable or unusable, the pool selects or creates another. Checkout
records one use. `pool.retire_session(&session_id).await` explicitly retires an entry, while
`session.retire().await` retires the session directly.

A session becomes unusable when it is retired, expires, reaches `max_usage_count`, or reaches its
error-score threshold. `mark_bad()` adds one error point, `mark_good()` subtracts the configured
`error_score_decrement_scaled`, and `is_blocked()` reports whether the threshold has been reached.
The pool removes unusable entries as it checks sessions out.

## Session persistence

Pools are created before crawler storage is necessarily open, so persistence is attached in a
separate step:

```rust
pool.attach_persistence(kvs);
pool.restore().await?;

// At an application durability point:
pool.persist().await?;
```

`attach_persistence` installs the `KeyValueStore`; `restore` replaces the pool from its saved
state; `persist` saves IDs, cookies, scores, usage counts, retirement state, and original expiry.
Without an attached store, both persistence operations return successfully without changing
storage. The default persistence key is `SESSION_POOL_PERSIST_KEY`.

## HTTP builder integration

`HttpKindBuilder` enables sessions by default. Choose one of these configurations:

- `session_pool(SessionPoolOptions)` asks the kind to own a configured pool.
- `shared_session_pool(Arc<SessionPool>)` reuses a pool across crawlers.
- `disable_sessions()` removes session checkout and cookie reuse.
- `session_status_codes(...)` replaces the status-code set that triggers session errors.

`CrawlerBuilder::max_session_rotations` limits session rotations separately from ordinary request
retries.

```rust
let kind = millipede::HttpKind::builder()
    .session_pool(
        millipede::SessionPoolOptions::default()
            .with_max_pool_size(100),
    )
    .session_status_codes([401, 403, 429])
    .build()?;

let crawler = millipede::Crawler::builder(kind)
    .max_session_rotations(5)
    .storage_client(storage)
    .request_handler(handler)
    .build()
    .await?;
```

Blocked responses classified as session errors rotate the identity and count against session
rotations rather than ordinary `max_request_retries`.

## Cookies

Each `Session` owns an authoritative shared `CookieJar`. `Cookie` represents name, value, domain,
path, expiry, secure, HTTP-only, host-only, and optional `SameSite` state. `SameSite` has `Strict`,
`Lax`, and `None` variants.

The jar can build `cookie_header_for` a URL, `store_response_cookies`, `export_cookies`, report
`cookie_count`, serialize with `to_json`, restore with `from_json`, or `clear` its contents.
`Session::set_cookies_from_response` stores response cookies in the session jar.

[ADR-0002](../decisions/ADR-0002-cookie-jar.md) explains why Millipede exposes its own synchronous,
persistable `CookieJar` instead of a reqwest-specific store or an async lock.

## Proxy configuration modes

`ProxyConfiguration` supports four selection shapes:

- `round_robin(urls)` cycles through a static list.
- `rotating(urls, RotationStrategy)` selects round-robin or random rotation.
- `tiered(tiers)` starts at a lower tier and escalates a target domain after blocking.
- `custom(resolver)` delegates selection to an async `ProxyResolver`.

`new_url(context).await` returns the selected URL, or `None` for a direct request.
`new_proxy_info(context).await` returns parsed `ProxyInfo` with tier and session metadata.
`ProxyResolveContext::new()` can be enriched with `request`, `session_id`, and `attempt`.

```rust
use millipede::{ProxyConfiguration, ProxyResolveContext};

let proxies = ProxyConfiguration::round_robin([
    "http://proxy-a.example:8000".parse()?,
    "http://proxy-b.example:8000".parse()?,
]);
let info = proxies
    .new_proxy_info(ProxyResolveContext::new().attempt(0))
    .await?;
```

For tiered configurations, `report_blocked(&target_url)` escalates the domain unless a recovery
probe failed. `report_success(&target_url)` accepts a successful lower-tier recovery probe. These
methods are harmless no-ops for non-tiered configurations.

## Proxy buckets and retry strategy

`ProxyBuckets` associates `ProxyKind` values with configurations. Build it with `new`, then add a
fallback with `with_default`, a media route with `with_media`, and named routes with
`with_custom(name, configuration)`.

```rust
let buckets = millipede::ProxyBuckets::new()
    .with_default(default_proxies)
    .with_media(media_proxies)
    .with_custom("api", api_proxies);
```

`HttpKindBuilder::proxy` installs one `ProxyConfiguration`.
`HttpKindBuilder::proxy_buckets` installs buckets, and `HttpKindBuilder::proxy_strategy` installs a
`ProxyStrategy` that selects a `ProxyKind` from each `ProxyRouteContext`. The resulting
`ProxyInfo`, when present, is available to handlers as `ctx.proxy_info`.

Sessions and proxies also provide the stable seed and route metadata needed for coherent browser
profiles. Continue with [Fingerprinting](./fingerprinting.md) for the supported v0.1 consistency
layer.
