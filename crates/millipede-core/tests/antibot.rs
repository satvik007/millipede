//! Integration tests for conservative anti-bot response detection.

use http::{HeaderMap, HeaderValue, StatusCode, header::SET_COOKIE};
use millipede_core::antibot::{AntiBotDetector, AntiBotSignals, DefaultAntiBotDetector};
use millipede_core::errors::AntiBotTech;

fn url() -> url::Url {
    url::Url::parse("https://example.test/final").expect("fixture URL should be valid")
}

fn detect_fixture(body: &str, status: StatusCode) -> Option<AntiBotTech> {
    let headers = HeaderMap::new();
    let final_url = url();
    let signals = AntiBotSignals::new(status, &headers, body.as_bytes(), &final_url);
    DefaultAntiBotDetector::new().detect(&signals)
}

macro_rules! vendor_fixture_test {
    ($name:ident, $fixture:literal, $expected:expr) => {
        #[test]
        fn $name() {
            let fixture = include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/antibot/",
                $fixture
            ));
            assert_eq!(detect_fixture(fixture, StatusCode::OK), Some($expected));
        }
    };
}

vendor_fixture_test!(
    cloudflare_fixture,
    "cloudflare.html",
    AntiBotTech::Cloudflare
);
vendor_fixture_test!(datadome_fixture, "datadome.html", AntiBotTech::DataDome);
vendor_fixture_test!(
    perimeterx_fixture,
    "perimeterx.html",
    AntiBotTech::PerimeterX
);
vendor_fixture_test!(kasada_fixture, "kasada.html", AntiBotTech::Kasada);
vendor_fixture_test!(imperva_fixture, "imperva.html", AntiBotTech::Imperva);
vendor_fixture_test!(akamai_fixture, "akamai.html", AntiBotTech::Akamai);

#[test]
fn benign_contentful_page_is_not_detected() {
    let fixture = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/antibot/benign_contentful.html"
    ));
    assert_eq!(detect_fixture(fixture, StatusCode::OK), None);
}

#[test]
fn plain_forbidden_page_is_not_detected() {
    let fixture = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/antibot/benign_forbidden.html"
    ));
    assert_eq!(detect_fixture(fixture, StatusCode::FORBIDDEN), None);
}

#[test]
fn detects_cloudflare_from_header_only() {
    let mut headers = HeaderMap::new();
    headers.insert("cf-ray", HeaderValue::from_static("abc"));
    assert_eq!(
        detect(&DefaultAntiBotDetector::new(), &headers, b""),
        Some(AntiBotTech::Cloudflare)
    );
}

#[test]
fn detects_kasada_from_header_only() {
    let mut headers = HeaderMap::new();
    headers.insert("x-kpsdk-ct", HeaderValue::from_static("fixture"));
    assert_eq!(
        detect(&DefaultAntiBotDetector::new(), &headers, b""),
        Some(AntiBotTech::Kasada)
    );
}

#[test]
fn detects_akamai_from_set_cookie() {
    let mut headers = HeaderMap::new();
    headers.insert(
        SET_COOKIE,
        HeaderValue::from_static("_abck=fixture; Path=/"),
    );
    assert_eq!(
        detect(&DefaultAntiBotDetector::new(), &headers, b""),
        Some(AntiBotTech::Akamai)
    );
}

#[test]
fn detects_custom_marker() {
    let detector = DefaultAntiBotDetector::new().with_custom_marker("acme-shield", "Acme");
    assert_eq!(
        detect(&detector, &HeaderMap::new(), b"Protected by ACME-SHIELD"),
        Some(AntiBotTech::Custom("Acme".to_owned()))
    );
}

#[test]
fn ignores_markers_past_inspection_limit() {
    let mut body = vec![b'x'; DefaultAntiBotDetector::DEFAULT_INSPECTION_LIMIT];
    body.extend_from_slice(b"Just a moment...");
    assert_eq!(
        detect(&DefaultAntiBotDetector::new(), &HeaderMap::new(), &body),
        None
    );
}

fn detect(
    detector: &DefaultAntiBotDetector,
    headers: &HeaderMap,
    body: &[u8],
) -> Option<AntiBotTech> {
    let final_url = url();
    let signals = AntiBotSignals::new(StatusCode::OK, headers, body, &final_url);
    detector.detect(&signals)
}
