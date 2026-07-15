# ADR-0002: Cookie jar concretion — custom CookieJar over cookie_store behind std::sync::Mutex

## Status

Accepted (Phase 3).

## Context

Sessions need one persistable cookie representation that can be shared by the HTTP crawler now and adapted to chromiumoxide in Phase 6. The store belongs to a request/session (`HttpRequest::cookie_jar`), while a reqwest cookie provider belongs to a `Client`. `HttpResponse::redirect_chain` also requires `ReqwestClient` to disable reqwest's automatic redirects and follow each hop manually: reqwest exposes neither intermediate hop URLs nor every hop's `Set-Cookie` headers after automatic redirect handling.

The ROADMAP dependency table suggested `reqwest_cookie_store`, and INTERFACE §22 Q1 listed `reqwest_cookie_store::CookieStoreMutex` and `Arc<tokio::sync::RwLock<cookie_store::CookieStore>>` as candidates. During the Phase 3 audit, the initial custom jar's default `cookie_store` serialization was found to drop nonpersistent session cookies, contradicting its persistence contract and failing the session-cookie round-trip test.

## Decision

The public `millipede_core::cookies::CookieJar` newtype wraps `std::sync::Mutex<cookie_store::CookieStore>`. JSON persistence includes session and expired cookies using `cookie_store::serde::json::{save_incl_expired_and_nonpersistent, load_all}`. The audit fixed the serialization bug by replacing the default `Serialize`/`Deserialize` path with that explicit pairing and added a regression test for a session cookie without `Expires` or `Max-Age`.

`reqwest_cookie_store::CookieStoreMutex` is rejected. Version 0.8 hard-depends on reqwest because it exists to implement `reqwest::cookie::CookieStore`; pulling it into `millipede-core` would violate INTERFACE §2's crate boundary, where core is backend-agnostic and only `millipede-http` may depend on reqwest. Future `wreq`/`impit`-style backends are the reason for the `HttpClient` trait. Manual redirect handling is independently required for `redirect_chain` and per-hop `Set-Cookie`, so reqwest's automatic cookie layer would be unused. Per-request jars also cannot ride a per-client provider. Exposing a third-party lock type behind the newtype would add no benefit.

`Arc<tokio::sync::RwLock<cookie_store::CookieStore>>` is rejected. Every jar operation is a microsecond-scale synchronous critical section, and future reqwest and chromiumoxide adapter surfaces are synchronous. An async lock would buy nothing and invite held-across-await bugs.

This decision maps to §22 Q1's criteria as follows: redirect-chain cookie round trips are proven by `wiremock` tests in `millipede-http` this phase; the chromiumoxide `set_cookies`/`get_cookies` cycle remains Phase 6 work isolated behind the newtype, matching the Risk Register requirement for a thin adapter owned by `Session` that never exposes the raw cookie store; and no lock is held across `.await` because all `CookieJar` methods are synchronous. The last property is reinforced by the workspace `clippy::await_holding_lock = "deny"` lint and the tokio-console-assisted lock-progress regression test landing in `millipede-http` this phase.

Under the CTO's Phase 3 delegation, this ADR supersedes the ROADMAP dependency-table Cookies row. `ROADMAP.md` is updated in the same commit.

## Consequences

`millipede-core` stays independent of reqwest, and HTTP backends receive an explicit per-request jar through the stable newtype. `ReqwestClient` must inject matching cookies and store response cookies at every manually followed redirect hop. Browser integration must translate through narrow adapter methods in Phase 6 rather than exposing `cookie_store` types publicly.

Cookie operations use a poisoning-tolerant synchronous mutex and must remain short and non-async. Persistence intentionally retains session and expired cookies; consumers decide applicability through `cookie_store` when constructing request headers.
