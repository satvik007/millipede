//! Content-based anti-bot and web application firewall detection.

use crate::errors::AntiBotTech;
use http::{HeaderMap, StatusCode};
use url::Url;

/// Response signals inspected for evidence of an anti-bot challenge.
#[non_exhaustive]
pub struct AntiBotSignals<'a> {
    /// The response status code.
    pub status: StatusCode,
    /// The response headers.
    pub headers: &'a HeaderMap,
    /// The response body bytes.
    pub body: &'a [u8],
    /// The final response URL after redirects.
    pub final_url: &'a Url,
}

impl<'a> AntiBotSignals<'a> {
    /// Creates a collection of response signals for detection.
    pub fn new(
        status: StatusCode,
        headers: &'a HeaderMap,
        body: &'a [u8],
        final_url: &'a Url,
    ) -> Self {
        Self {
            status,
            headers,
            body,
            final_url,
        }
    }
}

/// Detects anti-bot or web application firewall responses from response signals.
pub trait AntiBotDetector: Send + Sync + std::fmt::Debug + 'static {
    /// Returns the detected technology, or `None` when evidence is insufficient.
    fn detect(&self, signals: &AntiBotSignals<'_>) -> Option<AntiBotTech>;
}

/// A conservative detector using bounded, vendor-specific static markers.
#[derive(Debug, Clone)]
#[must_use = "detector configuration does nothing unless the detector is installed"]
pub struct DefaultAntiBotDetector {
    inspection_limit: usize,
    custom_markers: Vec<(String, AntiBotTech)>,
}

impl DefaultAntiBotDetector {
    /// The maximum number of response-body bytes inspected by default.
    pub const DEFAULT_INSPECTION_LIMIT: usize = 64 * 1024;

    /// Creates a detector with the default body inspection limit.
    pub fn new() -> Self {
        Self {
            inspection_limit: Self::DEFAULT_INSPECTION_LIMIT,
            custom_markers: Vec::new(),
        }
    }

    /// Sets the maximum number of response-body bytes to inspect.
    pub fn with_inspection_limit(mut self, inspection_limit: usize) -> Self {
        self.inspection_limit = inspection_limit;
        self
    }

    /// Adds a case-insensitive body marker with a custom technology label.
    pub fn with_custom_marker(
        mut self,
        marker: impl Into<String>,
        label: impl Into<String>,
    ) -> Self {
        self.custom_markers.push((
            marker.into().to_lowercase(),
            AntiBotTech::Custom(label.into()),
        ));
        self
    }
}

impl Default for DefaultAntiBotDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl AntiBotDetector for DefaultAntiBotDetector {
    fn detect(&self, signals: &AntiBotSignals<'_>) -> Option<AntiBotTech> {
        let inspected_len = signals.body.len().min(self.inspection_limit);
        let body = String::from_utf8_lossy(&signals.body[..inspected_len]).to_ascii_lowercase();

        let mut cf_header = false;
        let mut cloudflare_server = false;
        let mut cloudflare_cookie = false;
        let mut datadome_header = false;
        let mut datadome_cookie = false;
        let mut perimeterx_cookie = false;
        let mut kasada_header = false;
        let mut imperva_header = false;
        let mut imperva_cookie = false;
        let mut akamai_header = false;
        let mut akamai_cookie = false;

        for (name, value) in signals.headers.iter() {
            let name = name.as_str().to_ascii_lowercase();
            if name == "cf-ray" || name == "cf-mitigated" {
                cf_header = true;
            }
            if name == "x-datadome" || name == "x-dd-b" {
                datadome_header = true;
            }
            if name == "x-kpsdk-ct" || name == "x-kpsdk-cd" {
                kasada_header = true;
            }
            if name == "x-iinfo" {
                imperva_header = true;
            }
            if name.starts_with("x-akamai") {
                akamai_header = true;
            }

            let Ok(value) = value.to_str() else {
                continue;
            };
            let value = value.to_ascii_lowercase();

            if name == "server" && value.contains("cloudflare") {
                cloudflare_server = true;
            }
            if name == "set-cookie" {
                cloudflare_cookie |= value.contains("__cf_bm") || value.contains("cf_clearance");
                datadome_cookie |= value.contains("datadome");
                perimeterx_cookie |= value.contains("_px") || value.contains("_pxhd");
                imperva_cookie |= value.contains("visid_incap") || value.contains("incap_ses");
                akamai_cookie |= value.contains("_abck") || value.contains("ak_bmsc");
            }
        }

        if cf_header
            || cloudflare_server
            || cloudflare_cookie
            || contains_any(
                &body,
                &[
                    "just a moment",
                    "cf-chl",
                    "challenges.cloudflare.com",
                    "checking your browser",
                ],
            )
        {
            return Some(AntiBotTech::Cloudflare);
        }

        if datadome_header
            || datadome_cookie
            || contains_any(&body, &["datadome", "geo.captcha-delivery.com"])
        {
            return Some(AntiBotTech::DataDome);
        }

        if perimeterx_cookie || contains_any(&body, &["px-captcha", "perimeterx", "/_px"]) {
            return Some(AntiBotTech::PerimeterX);
        }

        if kasada_header || contains_any(&body, &["kpsdk", "kasada"]) {
            return Some(AntiBotTech::Kasada);
        }

        if imperva_header
            || imperva_cookie
            || contains_any(&body, &["incapsula", "_incap_", "incident id"])
        {
            return Some(AntiBotTech::Imperva);
        }

        if akamai_cookie
            || akamai_header
            || contains_any(&body, &["akamai bot manager", "akamaighost"])
        {
            return Some(AntiBotTech::Akamai);
        }

        for (marker, technology) in &self.custom_markers {
            if body.contains(marker) {
                return Some(technology.clone());
            }
        }

        if body.contains("captcha")
            && contains_any(
                &body,
                &["access denied", "verify you are human", "are you a human"],
            )
        {
            return Some(AntiBotTech::Unknown);
        }

        None
    }
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}
