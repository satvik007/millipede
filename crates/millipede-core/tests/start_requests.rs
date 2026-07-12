//! Public start-request conversion tests.

use millipede_core::{
    crawler::{IntoStartRequest as _, IntoStartRequests as _},
    errors::CrawlError,
    request::Request,
};

#[test]
fn conversion_matrix() -> Result<(), CrawlError> {
    assert_eq!("https://example.com".into_start_requests()?.len(), 1);
    assert_eq!(
        String::from("https://example.com")
            .into_start_requests()?
            .len(),
        1
    );
    assert_eq!(
        url::Url::parse("https://example.com")
            .unwrap()
            .into_start_requests()?
            .len(),
        1
    );
    assert_eq!(
        Request::get("https://example.com")
            .build()?
            .into_start_requests()?
            .len(),
        1
    );
    assert_eq!(
        vec!["https://example.com/a", "https://example.com/b"]
            .into_start_requests()?
            .len(),
        2
    );
    assert_eq!(
        ["https://example.com/a", "https://example.com/b"]
            .into_start_requests()?
            .len(),
        2
    );
    let requests = [
        Request::get("https://example.com/a").build()?,
        Request::get("https://example.com/b").build()?,
    ];
    assert_eq!((&requests[..]).into_start_requests()?.len(), 2);
    assert!(matches!(
        "not a url".into_start_request(),
        Err(CrawlError::NonRetryable(_))
    ));
    Ok(())
}
