//! Attempt-level retry strategy hooks.

use crate::{
    errors::{AntiBotTech, CrawlError},
    proxy::{ProxyInfo, ProxyKind},
    request::Request,
    session::SessionId,
};
use std::time::Duration;

/// Overrides carried to the next attempt of the same request.
///
/// Overrides live in engine memory keyed by request unique key and do not survive restarts.
///
/// # Examples
///
/// ```
/// use millipede_core::retry_strategy::AttemptOverrides;
/// use std::time::Duration;
///
/// let mut overrides = AttemptOverrides::default();
/// overrides.backoff = Some(Duration::from_millis(250));
/// ```
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct AttemptOverrides {
    /// Proxy bucket requested for the next attempt.
    pub proxy_kind: Option<ProxyKind>,
    /// User-agent profile requested for the next attempt.
    pub user_agent_profile: Option<String>,
    /// Delay before the next attempt begins.
    pub backoff: Option<Duration>,
}

/// Borrowed metadata describing a failed request attempt.
///
/// `session_id`, `proxy_info`, and `response_bytes` are present when the failed attempt produced a
/// handler context, and absent for fetch-level failures.
///
/// # Examples
///
/// ```
/// use millipede_core::retry_strategy::AttemptOutcome;
///
/// fn is_first_attempt(outcome: &AttemptOutcome<'_>) -> bool {
///     outcome.attempt == 0
/// }
/// ```
#[non_exhaustive]
pub struct AttemptOutcome<'a> {
    /// Request after preparation for this attempt.
    pub request: &'a Request,
    /// The request's retry count, independent of session rotations.
    pub attempt: u32,
    /// Observed or error-carried HTTP status.
    pub status: Option<http::StatusCode>,
    /// Attempt error.
    pub error: Option<&'a CrawlError>,
    /// Detected anti-bot technology.
    pub anti_bot: Option<AntiBotTech>,
    /// Proxy used by the attempt, when known.
    pub proxy_info: Option<&'a ProxyInfo>,
    /// Session used by the attempt, when known.
    pub session_id: Option<&'a SessionId>,
    /// Number of response body bytes, when known.
    pub response_bytes: Option<usize>,
}

/// Session disposition for a strategy-authorized retry.
///
/// # Examples
///
/// ```
/// use millipede_core::retry_strategy::SessionRetryAction;
///
/// let action = SessionRetryAction::Rotate;
/// assert_ne!(action, SessionRetryAction::Keep);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum SessionRetryAction {
    /// Keep the current session classification.
    #[default]
    Keep,
    /// Rotate the session for the next attempt.
    Rotate,
    /// Retire the session. In Phase 3 the engine accounts this like [`Self::Rotate`]; kind-level
    /// retirement relies on the kind's own classification and is revisited in Phase 4.
    Retire,
}

/// Owned instructions returned by a [`RetryStrategy`].
///
/// # Examples
///
/// ```
/// use millipede_core::retry_strategy::{RetryDirective, SessionRetryAction};
/// use std::time::Duration;
///
/// let directive = RetryDirective::retry()
///     .backoff(Duration::from_secs(1))
///     .session_action(SessionRetryAction::Rotate);
/// assert!(directive.should_retry);
/// ```
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct RetryDirective {
    /// Whether the attempt should be retried.
    pub should_retry: bool,
    /// Delay before the next attempt.
    pub backoff: Option<Duration>,
    /// Proxy bucket for the next attempt.
    pub proxy_kind: Option<ProxyKind>,
    /// User-agent profile for the next attempt.
    pub user_agent_profile: Option<String>,
    /// Session disposition for the retry.
    pub session_action: SessionRetryAction,
}

impl RetryDirective {
    /// Creates a directive authorizing a retry.
    pub fn retry() -> Self {
        Self {
            should_retry: true,
            ..Self::default()
        }
    }
    /// Creates a directive stopping retries.
    pub fn stop() -> Self {
        Self::default()
    }
    /// Sets the retry delay.
    pub fn backoff(mut self, backoff: Duration) -> Self {
        self.backoff = Some(backoff);
        self
    }
    /// Sets the proxy bucket for the next attempt.
    pub fn proxy_kind(mut self, proxy_kind: ProxyKind) -> Self {
        self.proxy_kind = Some(proxy_kind);
        self
    }
    /// Sets the user-agent profile for the next attempt.
    pub fn user_agent_profile(mut self, profile: impl Into<String>) -> Self {
        self.user_agent_profile = Some(profile.into());
        self
    }
    /// Sets the session disposition.
    pub fn session_action(mut self, action: SessionRetryAction) -> Self {
        self.session_action = action;
        self
    }
}

/// Controls retries and next-attempt overrides for non-critical failures.
///
/// A configured strategy has full authority over `should_retry` and may retry a non-retryable
/// error. Critical errors and requests marked `no_retry` never reach it.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use millipede_core::retry_strategy::{AttemptOutcome, RetryDirective, RetryStrategy};
/// struct Backoff;
/// impl RetryStrategy for Backoff {
///     fn max_retries(&self) -> u32 { 3 }
///     fn on_retry(&self, outcome: &AttemptOutcome<'_>) -> RetryDirective {
///         RetryDirective::retry().backoff(Duration::from_secs(1 << outcome.attempt))
///     }
/// }
/// ```
pub trait RetryStrategy: Send + Sync + 'static {
    /// Maximum ordinary retries when a request does not override the cap.
    fn max_retries(&self) -> u32;
    /// Returns instructions after a failed attempt.
    fn on_retry(&self, outcome: &AttemptOutcome<'_>) -> RetryDirective;
}
