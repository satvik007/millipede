//! Synchronous session cookie storage and JSON persistence.

use std::{fmt, sync::Mutex};

use http::{HeaderMap, HeaderValue, header::SET_COOKIE};
use url::Url;

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
