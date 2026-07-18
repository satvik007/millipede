//! Conservative HTTP-response detection for smart browser promotion.

use std::fmt;

use millipede_core::{errors::AntiBotTech, request::Request};
use millipede_html::{SynchronizedHtml, scraper::Selector};

/// A borrowed snapshot of one successful HTTP/HTML attempt.
#[non_exhaustive]
pub struct HttpAttemptSnapshot<'a> {
    /// Request that produced the response.
    pub request: &'a Request,
    /// Final response status.
    pub status: http::StatusCode,
    /// Final response headers.
    pub headers: &'a http::HeaderMap,
    /// Buffered response body.
    pub body: &'a [u8],
    /// Final response URL after redirects.
    pub final_url: &'a url::Url,
    /// Parsed HTML, when the response was parsed as HTML.
    pub html: Option<&'a SynchronizedHtml>,
}

impl<'a> HttpAttemptSnapshot<'a> {
    pub(crate) fn new(
        request: &'a Request,
        status: http::StatusCode,
        headers: &'a http::HeaderMap,
        body: &'a [u8],
        final_url: &'a url::Url,
        html: Option<&'a SynchronizedHtml>,
    ) -> Self {
        Self {
            request,
            status,
            headers,
            body,
            final_url,
            html,
        }
    }
}

/// Why an HTTP attempt should be repeated through a browser.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum PromotionReason {
    /// A successful HTML document has little visible text but contains JavaScript.
    EmptyBodyLikelyJs,
    /// A known anti-bot interstitial was detected.
    KnownAntiBot(AntiBotTech),
    /// A caller-required selector was absent from the parsed document.
    SelectorMissing {
        /// Selector that did not match any element.
        selector: String,
    },
    /// An HTTP error status was configured for browser promotion.
    StatusPromoted {
        /// Numeric HTTP status code.
        status: u16,
    },
    /// A custom detector-specific explanation.
    Custom(String),
}

impl fmt::Display for PromotionReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyBodyLikelyJs => {
                formatter.write_str("successful page is likely a JavaScript shell")
            }
            Self::KnownAntiBot(tech) => write!(formatter, "known anti-bot interstitial: {tech:?}"),
            Self::SelectorMissing { selector } => {
                write!(formatter, "required selector is missing: {selector}")
            }
            Self::StatusPromoted { status } => {
                write!(formatter, "configured HTTP status promoted: {status}")
            }
            Self::Custom(reason) => formatter.write_str(reason),
        }
    }
}

/// Decides whether a successful HTTP/HTML attempt needs browser execution.
pub trait BrowserPromotionDetector: Send + Sync + 'static {
    /// Returns the promotion reason, or `None` when the HTTP result should be kept.
    fn should_promote(&self, attempt: &HttpAttemptSnapshot<'_>) -> Option<PromotionReason>;
}

/// Conservative built-in promotion heuristics.
///
/// Anti-bot classification is delegated to the configured core detector.
#[derive(Debug, Clone)]
pub struct DefaultPromotionDetector {
    required_selector: Option<String>,
    min_visible_text: usize,
    anti_bot: std::sync::Arc<dyn millipede_core::antibot::AntiBotDetector>,
}

impl DefaultPromotionDetector {
    /// Creates a detector with conservative defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Promotes parsed pages that do not contain `selector`.
    pub fn with_required_selector(mut self, selector: impl Into<String>) -> Self {
        self.required_selector = Some(selector.into());
        self
    }

    /// Sets the minimum visible body-text length for JavaScript-shell detection.
    pub fn with_min_visible_text(mut self, minimum: usize) -> Self {
        self.min_visible_text = minimum;
        self
    }

    /// Sets the anti-bot detector used to classify response signals.
    pub fn with_anti_bot_detector(
        mut self,
        detector: std::sync::Arc<dyn millipede_core::antibot::AntiBotDetector>,
    ) -> Self {
        self.anti_bot = detector;
        self
    }

    fn inspect_html(
        &self,
        status: http::StatusCode,
        document: &millipede_html::scraper::Html,
    ) -> Option<PromotionReason> {
        if let Some(selector_text) = &self.required_selector {
            if let Ok(selector) = Selector::parse(selector_text) {
                if document.select(&selector).next().is_none() {
                    return Some(PromotionReason::SelectorMissing {
                        selector: selector_text.clone(),
                    });
                }
            }
        }

        if status.is_success() {
            let body_selector = Selector::parse("body").expect("static body selector is valid");
            let script_selector =
                Selector::parse("script").expect("static script selector is valid");
            let visible_text_len = document
                .select(&body_selector)
                .next()
                .map(|body| body.text().collect::<Vec<_>>().join(" ").trim().len())
                .unwrap_or(0);
            let has_script = document.select(&script_selector).next().is_some();
            if visible_text_len < self.min_visible_text && has_script {
                return Some(PromotionReason::EmptyBodyLikelyJs);
            }
        }

        None
    }
}

impl Default for DefaultPromotionDetector {
    fn default() -> Self {
        Self {
            required_selector: None,
            min_visible_text: 40,
            anti_bot: std::sync::Arc::new(millipede_core::antibot::DefaultAntiBotDetector::new()),
        }
    }
}

impl BrowserPromotionDetector for DefaultPromotionDetector {
    fn should_promote(&self, attempt: &HttpAttemptSnapshot<'_>) -> Option<PromotionReason> {
        let signals = millipede_core::antibot::AntiBotSignals::new(
            attempt.status,
            attempt.headers,
            attempt.body,
            attempt.final_url,
        );
        if let Some(tech) = self.anti_bot.detect(&signals) {
            return Some(PromotionReason::KnownAntiBot(tech));
        }

        if let Some(html) = attempt.html {
            if let Some(reason) =
                html.with_html(|document| self.inspect_html(attempt.status, document))
            {
                return Some(reason);
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use millipede_core::request::Request;

    fn snapshot<'a>(
        request: &'a Request,
        headers: &'a http::HeaderMap,
        body: &'a [u8],
        status: http::StatusCode,
    ) -> HttpAttemptSnapshot<'a> {
        HttpAttemptSnapshot::new(request, status, headers, body, &request.url, None)
    }

    fn request() -> Request {
        Request::get("https://example.com/")
            .build()
            .expect("valid test request")
    }

    fn html(source: &str) -> millipede_html::scraper::Html {
        millipede_html::scraper::Html::parse_document(source)
    }

    #[test]
    fn cloudflare_marker_is_promoted() {
        let request = request();
        let headers = http::HeaderMap::new();
        assert_eq!(
            DefaultPromotionDetector::new().should_promote(&snapshot(
                &request,
                &headers,
                b"<html><body>Just a moment...</body></html>",
                http::StatusCode::OK,
            )),
            Some(PromotionReason::KnownAntiBot(AntiBotTech::Cloudflare))
        );
    }

    #[test]
    fn contentful_page_stays_http() {
        let document = html(
            "<html><body>A long, genuinely contentful page with enough visible text for the conservative detector.</body></html>",
        );
        assert_eq!(
            DefaultPromotionDetector::new().inspect_html(http::StatusCode::OK, &document),
            None
        );
    }

    #[test]
    fn javascript_shell_is_promoted() {
        let document = html("<html><body><script src=\"app.js\"></script></body></html>");
        assert_eq!(
            DefaultPromotionDetector::new().inspect_html(http::StatusCode::OK, &document),
            Some(PromotionReason::EmptyBodyLikelyJs)
        );
    }

    #[test]
    fn required_selector_only_promotes_when_missing() {
        let missing = html("<html><body><main>content</main></body></html>");
        let present = html("<html><body><main id=\"app\">content</main></body></html>");
        let detector = DefaultPromotionDetector::new().with_required_selector("#app");
        assert_eq!(
            detector.inspect_html(http::StatusCode::OK, &missing),
            Some(PromotionReason::SelectorMissing {
                selector: "#app".to_owned(),
            })
        );
        assert_eq!(detector.inspect_html(http::StatusCode::OK, &present), None);
    }

    #[test]
    fn markers_after_inspection_cap_are_ignored() {
        let request = request();
        let headers = http::HeaderMap::new();
        // Mirrors the core detector's default inspection window.
        let mut body =
            vec![b'x'; millipede_core::antibot::DefaultAntiBotDetector::DEFAULT_INSPECTION_LIMIT];
        body.extend_from_slice(b"just a moment");
        assert_eq!(
            DefaultPromotionDetector::new().should_promote(&snapshot(
                &request,
                &headers,
                &body,
                http::StatusCode::OK,
            )),
            None
        );
    }

    #[test]
    fn perimeterx_fixture_is_promoted() {
        let body = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../millipede-core/tests/fixtures/antibot/perimeterx.html"
        ));
        let request = request();
        let headers = http::HeaderMap::new();
        assert_eq!(
            DefaultPromotionDetector::new().should_promote(&snapshot(
                &request,
                &headers,
                body.as_bytes(),
                http::StatusCode::OK,
            )),
            Some(PromotionReason::KnownAntiBot(AntiBotTech::PerimeterX))
        );
    }

    #[test]
    fn imperva_fixture_is_promoted() {
        let body = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../millipede-core/tests/fixtures/antibot/imperva.html"
        ));
        let request = request();
        let headers = http::HeaderMap::new();
        assert_eq!(
            DefaultPromotionDetector::new().should_promote(&snapshot(
                &request,
                &headers,
                body.as_bytes(),
                http::StatusCode::OK,
            )),
            Some(PromotionReason::KnownAntiBot(AntiBotTech::Imperva))
        );
    }

    #[test]
    fn cloudflare_fixture_is_promoted() {
        let body = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../millipede-core/tests/fixtures/antibot/cloudflare.html"
        ));
        let request = request();
        let headers = http::HeaderMap::new();
        assert_eq!(
            DefaultPromotionDetector::new().should_promote(&snapshot(
                &request,
                &headers,
                body.as_bytes(),
                http::StatusCode::OK,
            )),
            Some(PromotionReason::KnownAntiBot(AntiBotTech::Cloudflare))
        );
    }

    #[test]
    fn benign_contentful_fixture_stays_http() {
        let body = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../millipede-core/tests/fixtures/antibot/benign_contentful.html"
        ));
        let request = request();
        let headers = http::HeaderMap::new();
        assert_eq!(
            DefaultPromotionDetector::new().should_promote(&snapshot(
                &request,
                &headers,
                body.as_bytes(),
                http::StatusCode::OK,
            )),
            None
        );
    }

    #[test]
    fn contentful_forbidden_page_does_not_trip_body_detector() {
        let document = html(
            "<html><body>A long forbidden response with meaningful content and no JavaScript shell markers.</body></html>",
        );
        assert_eq!(
            DefaultPromotionDetector::new().inspect_html(http::StatusCode::FORBIDDEN, &document),
            None
        );
    }
}
