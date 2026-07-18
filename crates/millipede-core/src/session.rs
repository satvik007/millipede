//! Sessions and the session pool.
//!
//! Expiry uses a persisted wall-clock timestamp instead of an `Instant`, so restarting a crawler
//! cannot silently renew sessions. Persistence is explicit rather than `AutoSaved`: the live cookie
//! jar remains authoritative instead of being duplicated behind another lock. Pools are created
//! before crawler storage is available, so
//! [`SessionPool::attach_persistence`](crate::session::SessionPool::attach_persistence) bridges that
//! lifecycle gap instead of requiring storage in
//! [`SessionPool::new`](crate::session::SessionPool::new).

use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{
    cookies::CookieJar,
    errors::CrawlError,
    request::UserData,
    storage::{KeyValueStore, KeyValueStoreExt},
};

/// Stable identifier for a crawler session.
///
/// ```
/// use millipede_core::session::SessionId;
/// let id = SessionId::generate();
/// assert!(id.as_str().starts_with("session-"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(String);

impl SessionId {
    /// Generates a process-local random identifier.
    pub fn generate() -> Self {
        Self(format!("session-{:016x}", crate::util::rand_u64()))
    }
    /// Returns the identifier as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<String> for SessionId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Stable token used to keep fingerprint generation consistent within a session.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionToken(String);

impl SessionToken {
    /// Creates a token from its textual representation.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the token as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SessionToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<String> for SessionToken {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for SessionToken {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<&SessionId> for SessionToken {
    fn from(id: &SessionId) -> Self {
        Self(id.as_str().to_owned())
    }
}

impl From<SessionId> for SessionToken {
    fn from(id: SessionId) -> Self {
        Self(id.as_str().to_owned())
    }
}

#[cfg(test)]
mod session_token_tests {
    use super::{SessionId, SessionToken};

    #[test]
    fn session_token_from_session_id_preserves_text() {
        let id = SessionId::from("session-stable".to_owned());
        assert_eq!(SessionToken::from(&id).as_str(), id.as_str());
        assert_eq!(SessionToken::from(id).as_str(), "session-stable");
    }
}

/// Limits and scoring behavior for one session.
///
/// Scores use fixed-point thousandths. `max_age` extends the three-field interface configuration
/// with Crawlee's 3,000-second session lifetime.
///
/// ```
/// use millipede_core::session::SessionConfig;
/// let config = SessionConfig::default().with_max_usage_count(10);
/// assert_eq!(config.max_usage_count, 10);
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SessionConfig {
    /// Blocking threshold, scaled by 1,000.
    pub max_error_score_scaled: u32,
    /// Score removed after a successful use, scaled by 1,000.
    pub error_score_decrement_scaled: u32,
    /// Maximum attempts served before retirement.
    pub max_usage_count: u32,
    /// Maximum wall-clock lifetime.
    pub max_age: Duration,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_error_score_scaled: 3_000,
            error_score_decrement_scaled: 500,
            max_usage_count: 50,
            max_age: Duration::from_secs(3_000),
        }
    }
}

impl SessionConfig {
    /// Sets the scaled blocking threshold.
    pub fn with_max_error_score_scaled(mut self, value: u32) -> Self {
        self.max_error_score_scaled = value;
        self
    }
    /// Sets the scaled successful-use decrement.
    pub fn with_error_score_decrement_scaled(mut self, value: u32) -> Self {
        self.error_score_decrement_scaled = value;
        self
    }
    /// Sets the maximum usage count.
    pub fn with_max_usage_count(mut self, value: u32) -> Self {
        self.max_usage_count = value;
        self
    }
    /// Sets the maximum session age.
    pub fn with_max_age(mut self, value: Duration) -> Self {
        self.max_age = value;
        self
    }
}

struct SessionState {
    user_data: UserData,
    error_score_scaled: u32,
    usage_count: u32,
    retired: bool,
}

/// Cookie, score, and user-data state associated with a crawling identity.
///
/// ```
/// use millipede_core::session::{Session, SessionConfig};
/// let session = Session::new(SessionConfig::default());
/// assert!(session.id().as_str().starts_with("session-"));
/// ```
pub struct Session {
    id: SessionId,
    cookies: Arc<CookieJar>,
    state: tokio::sync::Mutex<SessionState>,
    expires_at: OffsetDateTime,
    config: SessionConfig,
}

impl Session {
    /// Creates an empty session with a generated identifier.
    pub fn new(config: SessionConfig) -> Self {
        let expires_at = OffsetDateTime::now_utc() + config.max_age;
        Self {
            id: SessionId::generate(),
            cookies: Arc::new(CookieJar::new()),
            state: tokio::sync::Mutex::new(SessionState {
                user_data: UserData::default(),
                error_score_scaled: 0,
                usage_count: 0,
                retired: false,
            }),
            expires_at,
            config,
        }
    }

    fn restored(value: PersistedSession, config: SessionConfig) -> Result<Self, CrawlError> {
        let cookies = CookieJar::from_json(&value.cookies).map_err(CrawlError::non_retryable)?;
        Ok(Self {
            id: value.id.into(),
            cookies: Arc::new(cookies),
            state: tokio::sync::Mutex::new(SessionState {
                user_data: UserData::default(),
                error_score_scaled: value.error_score_scaled,
                usage_count: value.usage_count,
                retired: value.retired,
            }),
            expires_at: value.expires_at,
            config,
        })
    }

    /// Returns this session's identifier.
    pub fn id(&self) -> &SessionId {
        &self.id
    }
    /// Returns the authoritative shared cookie jar.
    pub fn cookie_jar(&self) -> &Arc<CookieJar> {
        &self.cookies
    }
    /// Reads user data through a synchronous closure, releasing the lock on return.
    pub async fn with_user_data<R>(&self, f: impl FnOnce(&UserData) -> R) -> R {
        f(&self.state.lock().await.user_data)
    }
    /// Mutates user data through a synchronous closure, releasing the lock on return.
    pub async fn update_user_data(&self, f: impl FnOnce(&mut UserData)) {
        f(&mut self.state.lock().await.user_data);
    }
    /// Returns the error score in unscaled units.
    pub async fn error_score(&self) -> f32 {
        self.state.lock().await.error_score_scaled as f32 / 1_000.0
    }
    /// Returns the number of attempts served.
    pub async fn usage_count(&self) -> u32 {
        self.state.lock().await.usage_count
    }
    /// Records one checkout attempt. Session pools call this when serving a session.
    pub async fn record_usage(&self) {
        let mut state = self.state.lock().await;
        state.usage_count = state.usage_count.saturating_add(1);
    }
    /// Returns whether the error threshold has been reached.
    pub async fn is_blocked(&self) -> bool {
        self.state.lock().await.error_score_scaled >= self.config.max_error_score_scaled
    }
    /// Returns whether the fixed creation-time expiry has passed.
    pub fn is_expired(&self) -> bool {
        OffsetDateTime::now_utc() >= self.expires_at
    }
    /// Returns whether the session was explicitly retired.
    pub async fn is_retired(&self) -> bool {
        self.state.lock().await.retired
    }
    /// Returns whether the session can serve another attempt.
    pub async fn is_usable(&self) -> bool {
        let state = self.state.lock().await;
        !state.retired
            && !self.is_expired()
            && state.error_score_scaled < self.config.max_error_score_scaled
            && state.usage_count < self.config.max_usage_count
    }
    /// Decreases the error score, flooring it at zero.
    pub async fn mark_good(&self) {
        let mut state = self.state.lock().await;
        state.error_score_scaled = state
            .error_score_scaled
            .saturating_sub(self.config.error_score_decrement_scaled);
    }
    /// Adds one scaled error point, saturating at `u32::MAX`.
    pub async fn mark_bad(&self) {
        let mut state = self.state.lock().await;
        state.error_score_scaled = state.error_score_scaled.saturating_add(1_000);
    }
    /// Permanently retires this session.
    pub async fn retire(&self) {
        self.state.lock().await.retired = true;
    }
    /// Stores `Set-Cookie` headers synchronously and infallibly.
    pub fn set_cookies_from_response(&self, response: &crate::http_client::HttpResponse) {
        self.cookies
            .store_response_cookies(&response.url, &response.headers);
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Session")
            .field("id", &self.id)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// Default key used to persist the session pool.
pub const SESSION_POOL_PERSIST_KEY: &str = "SDK_SESSION_POOL_STATE";

/// Session pool capacity, creation, and persistence settings.
///
/// ```
/// use millipede_core::session::SessionPoolOptions;
/// assert_eq!(SessionPoolOptions::default().max_pool_size, 1000);
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SessionPoolOptions {
    /// Maximum number of retained sessions.
    pub max_pool_size: usize,
    /// Configuration cloned into new and restored sessions.
    pub session_config: SessionConfig,
    /// KVS key used by explicit persistence.
    pub persist_state_key: String,
}

impl Default for SessionPoolOptions {
    fn default() -> Self {
        Self {
            max_pool_size: 1_000,
            session_config: SessionConfig::default(),
            persist_state_key: SESSION_POOL_PERSIST_KEY.into(),
        }
    }
}

impl SessionPoolOptions {
    /// Sets the maximum retained session count.
    pub fn with_max_pool_size(mut self, value: usize) -> Self {
        self.max_pool_size = value;
        self
    }
    /// Sets configuration for pool sessions.
    pub fn with_session_config(mut self, value: SessionConfig) -> Self {
        self.session_config = value;
        self
    }
    /// Sets the persistence key.
    pub fn with_persist_state_key(mut self, value: impl Into<String>) -> Self {
        self.persist_state_key = value.into();
        self
    }
}

/// A bounded collection of reusable crawler sessions.
///
/// ```
/// use millipede_core::session::{SessionPool, SessionPoolOptions};
/// let pool = SessionPool::new(SessionPoolOptions::default());
/// # let _ = pool;
/// ```
pub struct SessionPool {
    sessions: tokio::sync::Mutex<Vec<Arc<Session>>>,
    options: SessionPoolOptions,
    kvs: Mutex<Option<Arc<dyn KeyValueStore>>>,
}

impl SessionPool {
    /// Creates an empty pool. Persistence can be attached once storage opens.
    pub fn new(options: SessionPoolOptions) -> Self {
        Self {
            sessions: tokio::sync::Mutex::new(Vec::new()),
            options,
            kvs: Mutex::new(None),
        }
    }
    /// Attaches the key-value store used by [`Self::persist`] and [`Self::restore`].
    pub fn attach_persistence(&self, kvs: Arc<dyn KeyValueStore>) {
        *self.kvs.lock().unwrap_or_else(|e| e.into_inner()) = Some(kvs);
    }
    /// Checks out a sticky usable session, creates one while below capacity, or rotates at random.
    pub async fn get_session(&self, sticky: Option<&SessionId>) -> Arc<Session> {
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sticky
            .and_then(|id| sessions.iter().find(|session| session.id() == id))
            .cloned()
        {
            if session.is_usable().await {
                session.record_usage().await;
                return session;
            }
        }
        let mut usable = Vec::with_capacity(sessions.len());
        for session in sessions.iter() {
            usable.push(session.is_usable().await);
        }
        let mut index = 0;
        sessions.retain(|_| {
            let keep = usable[index];
            index += 1;
            keep
        });
        if sessions.len() < self.options.max_pool_size {
            let session = Arc::new(Session::new(self.options.session_config.clone()));
            session.record_usage().await;
            sessions.push(Arc::clone(&session));
            return session;
        }
        if sessions.is_empty() {
            let session = Arc::new(Session::new(self.options.session_config.clone()));
            session.record_usage().await;
            return session;
        }
        let index = crate::util::rand_u64() as usize % sessions.len();
        let session = Arc::clone(&sessions[index]);
        session.record_usage().await;
        session
    }
    /// Retires the session with `id` when it exists.
    pub async fn retire_session(&self, id: &SessionId) {
        if let Some(session) = self
            .sessions
            .lock()
            .await
            .iter()
            .find(|s| s.id() == id)
            .cloned()
        {
            session.retire().await;
        }
    }
    /// Returns the number of currently retained entries.
    pub async fn session_count(&self) -> usize {
        self.sessions.lock().await.len()
    }
    /// Persists IDs, cookies, scores, usage, retirement, and original expiry.
    pub async fn persist(&self) -> Result<(), CrawlError> {
        let kvs = self.kvs.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let Some(kvs) = kvs else {
            return Ok(());
        };
        let sessions = self.sessions.lock().await;
        let mut persisted = Vec::with_capacity(sessions.len());
        for session in sessions.iter() {
            let state = session.state.lock().await;
            persisted.push(PersistedSession {
                id: session.id.to_string(),
                cookies: session
                    .cookies
                    .to_json()
                    .map_err(CrawlError::non_retryable)?,
                error_score_scaled: state.error_score_scaled,
                usage_count: state.usage_count,
                retired: state.retired,
                expires_at: session.expires_at,
            });
        }
        drop(sessions);
        kvs.set(
            &self.options.persist_state_key,
            &SessionPoolState {
                sessions: persisted,
            },
        )
        .await
        .map_err(CrawlError::retry)
    }
    /// Replaces pool contents from persisted state, skipping entries with corrupt cookie JSON.
    pub async fn restore(&self) -> Result<(), CrawlError> {
        let kvs = self.kvs.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let Some(kvs) = kvs else {
            return Ok(());
        };
        let Some(state) = kvs
            .get::<SessionPoolState>(&self.options.persist_state_key)
            .await
            .map_err(CrawlError::retry)?
        else {
            return Ok(());
        };
        let mut restored = Vec::with_capacity(state.sessions.len());
        for persisted in state.sessions {
            match Session::restored(persisted, self.options.session_config.clone()) {
                Ok(session) => restored.push(Arc::new(session)),
                Err(error) => tracing::warn!(%error, "skipping corrupt persisted session"),
            }
        }
        *self.sessions.lock().await = restored;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct SessionPoolState {
    sessions: Vec<PersistedSession>,
}

#[derive(Serialize, Deserialize)]
struct PersistedSession {
    id: String,
    cookies: String,
    error_score_scaled: u32,
    usage_count: u32,
    retired: bool,
    #[serde(with = "time::serde::rfc3339")]
    expires_at: OffsetDateTime,
}
