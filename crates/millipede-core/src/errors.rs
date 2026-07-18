//! Crawl errors and their retry semantics.

use crate::request::{Method, RequestBuildError};
use std::time::Duration;

/// An error produced while processing a crawl request.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CrawlError {
    /// A retryable error that counts against the request retry limit.
    #[error("retryable: {0}")]
    Retry(#[source] anyhow::Error),
    /// A retryable error that rotates the current session.
    #[error("session: {0}")]
    Session(#[source] anyhow::Error),
    /// A retryable error that ignores the request retry limit.
    #[error("force-retry: {0}")]
    ForceRetry(#[source] anyhow::Error),
    /// A permanent request error that invokes the failure handler.
    #[error("non-retryable: {0}")]
    NonRetryable(#[source] anyhow::Error),
    /// A critical error that aborts the crawler.
    #[error("critical: {0}")]
    Critical(#[source] anyhow::Error),
    /// No registered route matched a request's label and method.
    #[error("missing route for label {label:?} ({method})")]
    MissingRoute {
        /// The request routing label.
        label: Option<String>,
        /// The request HTTP method.
        method: Method,
    },
    /// A known anti-bot or web application firewall response was detected.
    #[error("anti-bot detected: {tech:?}")]
    AntiBotDetected {
        /// The detected anti-bot technology.
        tech: AntiBotTech,
        /// The underlying detection error.
        #[source]
        source: anyhow::Error,
    },
}

/// A recognized anti-bot or web application firewall technology.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AntiBotTech {
    /// Cloudflare protection.
    Cloudflare,
    /// DataDome protection.
    DataDome,
    /// PerimeterX protection.
    PerimeterX,
    /// Kasada protection.
    Kasada,
    /// Imperva protection.
    Imperva,
    /// Akamai protection.
    Akamai,
    /// A user-defined technology name.
    Custom(String),
    /// An unidentified anti-bot technology.
    Unknown,
}

impl CrawlError {
    /// Returns an HTTP status carried anywhere in the underlying error chain.
    pub fn http_status(&self) -> Option<http::StatusCode> {
        let source = match self {
            Self::Retry(error)
            | Self::Session(error)
            | Self::ForceRetry(error)
            | Self::NonRetryable(error)
            | Self::Critical(error) => error,
            Self::AntiBotDetected { source, .. } => source,
            Self::MissingRoute { .. } => return None,
        };
        source.chain().find_map(|error| {
            error
                .downcast_ref::<crate::http_client::HttpStatusError>()
                .map(|status| status.status)
        })
    }

    /// Returns a Retry-After duration carried anywhere in the underlying error chain.
    pub fn retry_after(&self) -> Option<Duration> {
        let source = match self {
            Self::Retry(error)
            | Self::Session(error)
            | Self::ForceRetry(error)
            | Self::NonRetryable(error)
            | Self::Critical(error) => error,
            Self::AntiBotDetected { source, .. } => source,
            Self::MissingRoute { .. } => return None,
        };
        source.chain().find_map(|error| {
            error
                .downcast_ref::<crate::http_client::HttpStatusError>()
                .and_then(|status| status.retry_after)
        })
    }

    /// Creates a retryable error that counts against the retry limit.
    pub fn retry<E: Into<anyhow::Error>>(error: E) -> Self {
        Self::Retry(error.into())
    }

    /// Creates a retryable error that rotates the current session.
    pub fn session<E: Into<anyhow::Error>>(error: E) -> Self {
        Self::Session(error.into())
    }

    /// Creates a retryable error that ignores the retry limit.
    pub fn force_retry<E: Into<anyhow::Error>>(error: E) -> Self {
        Self::ForceRetry(error.into())
    }

    /// Creates a permanent, non-retryable request error.
    pub fn non_retryable<E: Into<anyhow::Error>>(error: E) -> Self {
        Self::NonRetryable(error.into())
    }

    /// Creates a critical error that aborts the crawler.
    pub fn critical<E: Into<anyhow::Error>>(error: E) -> Self {
        Self::Critical(error.into())
    }

    /// Creates a permanent error that is never retried and runs the failure handler.
    ///
    /// This is an alias for [`Self::non_retryable`].
    pub fn fatal<E: Into<anyhow::Error>>(error: E) -> Self {
        Self::non_retryable(error)
    }

    /// Returns whether the request should be attempted again.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Retry(_) | Self::Session(_) | Self::ForceRetry(_) | Self::AntiBotDetected { .. }
        )
    }

    /// Returns whether retrying should rotate the current session.
    pub fn rotates_session(&self) -> bool {
        matches!(self, Self::Session(_) | Self::AntiBotDetected { .. })
    }

    /// Returns whether the error ignores the request retry limit.
    pub fn ignores_max_retries(&self) -> bool {
        matches!(self, Self::ForceRetry(_))
    }

    /// Returns whether the error should abort the entire crawler.
    pub fn is_critical(&self) -> bool {
        matches!(self, Self::Critical(_))
    }

    /// Returns whether retrying counts against the request retry limit.
    pub fn counts_against_retries(&self) -> bool {
        matches!(self, Self::Retry(_))
    }
}

impl From<std::io::Error> for CrawlError {
    fn from(error: std::io::Error) -> Self {
        Self::retry(error)
    }
}

impl From<serde_json::Error> for CrawlError {
    fn from(error: serde_json::Error) -> Self {
        Self::non_retryable(error)
    }
}

impl From<RequestBuildError> for CrawlError {
    fn from(error: RequestBuildError) -> Self {
        Self::non_retryable(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;

    #[test]
    fn constructors_produce_expected_variants() {
        assert!(matches!(
            CrawlError::retry(anyhow::anyhow!("x")),
            CrawlError::Retry(_)
        ));
        assert!(matches!(
            CrawlError::session(anyhow::anyhow!("x")),
            CrawlError::Session(_)
        ));
        assert!(matches!(
            CrawlError::force_retry(anyhow::anyhow!("x")),
            CrawlError::ForceRetry(_)
        ));
        assert!(matches!(
            CrawlError::non_retryable(anyhow::anyhow!("x")),
            CrawlError::NonRetryable(_)
        ));
        assert!(matches!(
            CrawlError::fatal(anyhow::anyhow!("x")),
            CrawlError::NonRetryable(_)
        ));
        assert!(matches!(
            CrawlError::critical(anyhow::anyhow!("x")),
            CrawlError::Critical(_)
        ));
    }

    #[test]
    fn extracts_http_status_from_error_chain() {
        let direct = CrawlError::retry(crate::http_client::HttpStatusError::new(
            StatusCode::TOO_MANY_REQUESTS,
        ));
        assert_eq!(direct.http_status(), Some(StatusCode::TOO_MANY_REQUESTS));

        let wrapped = anyhow::Error::new(crate::http_client::HttpStatusError::new(
            StatusCode::BAD_GATEWAY,
        ))
        .context("fetch failed");
        assert_eq!(
            CrawlError::retry(wrapped).http_status(),
            Some(StatusCode::BAD_GATEWAY)
        );
    }

    #[test]
    fn retry_after_extracted_through_chain() {
        let retry_after = Duration::from_secs(2);
        let direct = CrawlError::retry(
            crate::http_client::HttpStatusError::new(StatusCode::TOO_MANY_REQUESTS)
                .with_retry_after(retry_after),
        );
        assert_eq!(direct.retry_after(), Some(retry_after));

        let plain = CrawlError::retry(crate::http_client::HttpStatusError::new(
            StatusCode::TOO_MANY_REQUESTS,
        ));
        assert_eq!(plain.retry_after(), None);

        let wrapped = anyhow::Error::new(
            crate::http_client::HttpStatusError::new(StatusCode::TOO_MANY_REQUESTS)
                .with_retry_after(retry_after),
        )
        .context("fetch failed");
        assert_eq!(CrawlError::retry(wrapped).retry_after(), Some(retry_after));
    }

    #[test]
    fn retry_after_is_none_for_missing_route_and_anti_bot() {
        let missing_route = CrawlError::MissingRoute {
            label: None,
            method: Method::GET,
        };
        let anti_bot = CrawlError::AntiBotDetected {
            tech: AntiBotTech::Cloudflare,
            source: anyhow::anyhow!("challenge"),
        };

        assert_eq!(missing_route.retry_after(), None);
        assert_eq!(anti_bot.retry_after(), None);
    }

    #[test]
    fn helpers_cover_the_full_classification_matrix() {
        let cases = [
            (
                CrawlError::retry(anyhow::anyhow!("x")),
                [true, false, false, false, true],
            ),
            (
                CrawlError::session(anyhow::anyhow!("x")),
                [true, true, false, false, false],
            ),
            (
                CrawlError::force_retry(anyhow::anyhow!("x")),
                [true, false, true, false, false],
            ),
            (CrawlError::non_retryable(anyhow::anyhow!("x")), [false; 5]),
            (
                CrawlError::critical(anyhow::anyhow!("x")),
                [false, false, false, true, false],
            ),
            (
                CrawlError::MissingRoute {
                    label: Some("detail".into()),
                    method: Method::GET,
                },
                [false; 5],
            ),
            (
                CrawlError::AntiBotDetected {
                    tech: AntiBotTech::Cloudflare,
                    source: anyhow::anyhow!("x"),
                },
                [true, true, false, false, false],
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.is_retryable(), expected[0]);
            assert_eq!(error.rotates_session(), expected[1]);
            assert_eq!(error.ignores_max_retries(), expected[2]);
            assert_eq!(error.is_critical(), expected[3]);
            assert_eq!(error.counts_against_retries(), expected[4]);
        }
    }

    #[test]
    fn standard_errors_have_default_classifications() {
        let io_error: CrawlError = std::io::Error::other("io").into();
        assert!(matches!(io_error, CrawlError::Retry(_)));

        let json_error: CrawlError = serde_json::from_str::<serde_json::Value>("{")
            .expect_err("invalid JSON")
            .into();
        assert!(matches!(json_error, CrawlError::NonRetryable(_)));

        let request_error: CrawlError = RequestBuildError::MissingUrl.into();
        assert!(matches!(request_error, CrawlError::NonRetryable(_)));
    }

    #[test]
    fn display_strings_include_classification_prefixes() {
        assert!(
            CrawlError::retry(anyhow::anyhow!("x"))
                .to_string()
                .contains("retryable:")
        );
        assert!(
            CrawlError::session(anyhow::anyhow!("x"))
                .to_string()
                .contains("session:")
        );
        assert!(
            CrawlError::force_retry(anyhow::anyhow!("x"))
                .to_string()
                .contains("force-retry:")
        );
        assert!(
            CrawlError::non_retryable(anyhow::anyhow!("x"))
                .to_string()
                .contains("non-retryable:")
        );
        assert!(
            CrawlError::critical(anyhow::anyhow!("x"))
                .to_string()
                .contains("critical:")
        );
        assert!(
            CrawlError::AntiBotDetected {
                tech: AntiBotTech::Cloudflare,
                source: anyhow::anyhow!("x"),
            }
            .to_string()
            .contains("anti-bot detected:")
        );
    }

    #[test]
    fn missing_route_display_contains_label_and_method() {
        let error = CrawlError::MissingRoute {
            label: Some("detail".into()),
            method: Method::POST,
        };
        let display = error.to_string();
        assert!(display.contains("detail"));
        assert!(display.contains("POST"));
    }
}
