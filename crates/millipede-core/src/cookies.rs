//! Synchronous session cookie storage and JSON persistence.

use std::{fmt, sync::Mutex};

use http::{HeaderMap, HeaderValue, header::SET_COOKIE};
use url::Url;

/// A transport-neutral HTTP cookie shared by HTTP and browser crawler contexts.
///
/// `domain` is stored as a bare host without a leading dot. A cookie with
/// `host_only` set is sent only to that exact host, while `expires: None`
/// represents a session cookie.
///
/// # Examples
///
/// ```
/// use millipede_core::cookies::Cookie;
///
/// let cookie = Cookie::new("session", "abc", "example.com");
/// assert_eq!(cookie.path, "/");
/// assert!(!cookie.host_only);
/// ```
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct Cookie {
    /// The cookie name.
    pub name: String,
    /// The cookie value.
    pub value: String,
    /// The bare host, without a leading dot.
    pub domain: String,
    /// Whether the cookie is restricted to the exact host in `domain`.
    pub host_only: bool,
    /// The URL path scope, normally `/`.
    pub path: String,
    /// The persistent expiry instant, or `None` for a session cookie.
    #[serde(with = "time::serde::rfc3339::option")]
    pub expires: Option<time::OffsetDateTime>,
    /// Whether the cookie is sent only over secure transports.
    pub secure: bool,
    /// Whether browser scripts are prevented from reading the cookie.
    pub http_only: bool,
    /// The cookie's cross-site request policy, when explicitly set.
    pub same_site: Option<SameSite>,
}

impl Cookie {
    /// Creates a domain cookie with `/` path and no optional attributes.
    pub fn new(
        name: impl Into<String>,
        value: impl Into<String>,
        domain: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            domain: domain.into(),
            host_only: false,
            path: "/".to_owned(),
            expires: None,
            secure: false,
            http_only: false,
            same_site: None,
        }
    }
}

/// A cookie's cross-site request policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SameSite {
    /// Send the cookie only in same-site requests.
    Strict,
    /// Also send the cookie for safe top-level cross-site navigations.
    Lax,
    /// Permit cross-site requests; secure transport is normally required.
    None,
}

impl SameSite {
    fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "Strict",
            Self::Lax => "Lax",
            Self::None => "None",
        }
    }
}

/// A synchronized cookie store shared by crawler sessions.
///
/// The inner representation is fixed by ADR-0002; the newtype boundary keeps
/// backend adapters from exposing the underlying cookie store or lock.
///
/// # Examples
///
/// ```
/// use millipede_core::cookies::CookieJar;
///
/// let jar = CookieJar::new();
/// assert_eq!(jar.cookie_count(), 0);
/// ```
pub struct CookieJar {
    store: Mutex<cookie_store::CookieStore>,
}

impl CookieJar {
    /// Creates an empty cookie jar.
    pub fn new() -> Self {
        Self {
            store: Mutex::new(cookie_store::CookieStore::default()),
        }
    }

    /// Parses and stores every `Set-Cookie` response header.
    pub fn store_response_cookies(&self, url: &Url, headers: &HeaderMap) {
        let mut store = self.store.lock().unwrap_or_else(|error| error.into_inner());
        for value in headers.get_all(SET_COOKIE) {
            let Ok(value) = value.to_str() else {
                tracing::debug!(?value, "ignoring non-text Set-Cookie header");
                continue;
            };
            if let Err(error) = store.parse(value, url) {
                tracing::debug!(%error, %url, cookie = value, "ignoring unparseable Set-Cookie header");
            }
        }
    }

    /// Builds the `Cookie` request header for `url`, if matching cookies exist.
    pub fn cookie_header_for(&self, url: &Url) -> Option<HeaderValue> {
        let store = self.store.lock().unwrap_or_else(|error| error.into_inner());
        let value = store
            .get_request_values(url)
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("; ");
        if value.is_empty() {
            None
        } else {
            HeaderValue::from_str(&value).ok()
        }
    }

    /// Exports all currently unexpired cookies in the transport-neutral representation.
    pub fn export_cookies(&self) -> Vec<Cookie> {
        let store = self.store.lock().unwrap_or_else(|error| error.into_inner());
        store
            .iter_unexpired()
            .filter_map(|stored| {
                let (domain, host_only) = match &stored.domain {
                    cookie_store::CookieDomain::HostOnly(host) => (host.clone(), true),
                    cookie_store::CookieDomain::Suffix(suffix) => {
                        (suffix.trim_start_matches('.').to_owned(), false)
                    }
                    cookie_store::CookieDomain::NotPresent | cookie_store::CookieDomain::Empty => {
                        tracing::debug!(
                            name = stored.name(),
                            "skipping exported cookie without a usable domain"
                        );
                        return None;
                    }
                };
                let path = if stored.path.is_empty() {
                    "/".to_owned()
                } else {
                    stored.path.to_string()
                };
                let expires = match &stored.expires {
                    cookie_store::CookieExpiration::AtUtc(expires) => Some(*expires),
                    cookie_store::CookieExpiration::SessionEnd => None,
                };
                let same_site = stored.same_site().map(|same_site| {
                    if same_site.is_strict() {
                        SameSite::Strict
                    } else if same_site.is_lax() {
                        SameSite::Lax
                    } else {
                        SameSite::None
                    }
                });

                Some(Cookie {
                    name: stored.name().to_owned(),
                    value: stored.value().to_owned(),
                    domain,
                    host_only,
                    path,
                    expires,
                    secure: stored.secure().unwrap_or(false),
                    http_only: stored.http_only().unwrap_or(false),
                    same_site,
                })
            })
            .collect()
    }

    /// Imports cookies, merging or overwriting entries by `(name, domain, path)`.
    ///
    /// Invalid entries are skipped. The returned count includes only cookies that
    /// the underlying store accepted.
    pub fn import_cookies(&self, cookies: &[Cookie]) -> usize {
        let mut store = self.store.lock().unwrap_or_else(|error| error.into_inner());
        let mut stored_count = 0;

        for cookie in cookies {
            if cookie.name.is_empty() || cookie.domain.is_empty() {
                tracing::debug!(
                    name = cookie.name,
                    domain = cookie.domain,
                    "skipping imported cookie with an empty name or domain"
                );
                continue;
            }

            let path = if cookie.path.is_empty() {
                "/"
            } else {
                cookie.path.as_str()
            };
            let mut builder =
                cookie_store::RawCookie::build((cookie.name.clone(), cookie.value.clone()))
                    .path(path.to_owned())
                    .secure(cookie.secure)
                    .http_only(cookie.http_only);
            if !cookie.host_only {
                builder = builder.domain(cookie.domain.clone());
            }
            if let Some(expires) = cookie.expires {
                builder = builder.expires(expires);
            }
            let mut raw = builder.build();
            if let Some(same_site) = cookie.same_site {
                let marker = format!("millipede=marker; SameSite={}", same_site.as_str());
                if let Ok(parsed) = cookie_store::RawCookie::parse(marker) {
                    if let Some(parsed_same_site) = parsed.same_site() {
                        raw.set_same_site(parsed_same_site);
                    }
                }
            }

            let request_url = format!(
                "{}://{}{}",
                if cookie.secure { "https" } else { "http" },
                cookie.domain,
                path
            );
            let request_url = match Url::parse(&request_url) {
                Ok(url) => url,
                Err(error) => {
                    tracing::debug!(%error, url = request_url, "skipping cookie with an invalid request URL");
                    continue;
                }
            };
            match store.insert_raw(&raw, &request_url) {
                Ok(_) => stored_count += 1,
                Err(error) => {
                    tracing::debug!(%error, url = %request_url, name = cookie.name, "skipping cookie rejected by the store");
                }
            }
        }

        stored_count
    }

    /// Serializes all cookies, including session and expired cookies, to JSON.
    pub fn to_json(&self) -> Result<String, CookieJarError> {
        let store = self.store.lock().unwrap_or_else(|error| error.into_inner());
        let mut buffer = Vec::new();
        cookie_store::serde::json::save_incl_expired_and_nonpersistent(&store, &mut buffer)
            .map_err(|error| {
                let error = match error.downcast::<serde_json::Error>() {
                    Ok(error) => anyhow::Error::new(*error),
                    Err(error) => anyhow::anyhow!("{error}"),
                };
                CookieJarError::Serialize(error)
            })?;
        String::from_utf8(buffer)
            .map_err(|error| CookieJarError::Serialize(anyhow::Error::new(error)))
    }

    /// Deserializes a cookie jar from its JSON representation.
    pub fn from_json(json: &str) -> Result<Self, CookieJarError> {
        let store = cookie_store::serde::json::load_all(json.as_bytes()).map_err(|error| {
            let error = match error.downcast::<serde_json::Error>() {
                Ok(error) => anyhow::Error::new(*error),
                Err(error) => anyhow::anyhow!("{error}"),
            };
            CookieJarError::Deserialize(error)
        })?;
        Ok(Self {
            store: Mutex::new(store),
        })
    }

    /// Removes every cookie from the jar.
    pub fn clear(&self) {
        self.store
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
    }

    /// Returns the number of unexpired cookies.
    pub fn cookie_count(&self) -> usize {
        self.store
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter_unexpired()
            .count()
    }
}

impl Default for CookieJar {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for CookieJar {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CookieJar")
            .field("cookie_count", &self.cookie_count())
            .finish()
    }
}

/// An error serializing or deserializing a cookie jar.
///
/// # Examples
///
/// ```
/// use millipede_core::cookies::{CookieJar, CookieJarError};
///
/// let result: Result<CookieJar, CookieJarError> = CookieJar::from_json("not json");
/// assert!(result.is_err());
/// ```
#[derive(Debug, thiserror::Error)]
pub enum CookieJarError {
    /// Cookie JSON serialization failed.
    #[error("failed to serialize cookie jar: {0}")]
    Serialize(#[source] anyhow::Error),
    /// Cookie JSON deserialization failed.
    #[error("failed to deserialize cookie jar: {0}")]
    Deserialize(#[source] anyhow::Error),
}
