use chromiumoxide::cdp::browser_protocol::network::{self, CookieSameSite, TimeSinceEpoch};
use millipede_browser::BrowserError;
use millipede_core::cookies::{Cookie, SameSite};
use time::OffsetDateTime;

pub(crate) fn to_cdp(cookie: &Cookie) -> Result<network::CookieParam, BrowserError> {
    if cookie.name.is_empty() {
        return Err(BrowserError::CookieConversion(anyhow::anyhow!(
            "cookie name must not be empty"
        )));
    }
    if cookie.domain.is_empty() {
        return Err(BrowserError::CookieConversion(anyhow::anyhow!(
            "cookie domain must not be empty"
        )));
    }

    let mut builder = network::CookieParam::builder()
        .name(cookie.name.clone())
        .value(cookie.value.clone())
        .path(cookie.path.clone())
        .secure(cookie.secure)
        .http_only(cookie.http_only);
    if cookie.host_only {
        let scheme = if cookie.secure { "https" } else { "http" };
        builder = builder.url(format!("{scheme}://{}{}", cookie.domain, cookie.path));
    } else {
        builder = builder.domain(cookie.domain.clone());
    }
    if let Some(same_site) = cookie.same_site {
        builder = builder.same_site(match same_site {
            SameSite::Strict => CookieSameSite::Strict,
            SameSite::Lax => CookieSameSite::Lax,
            SameSite::None => CookieSameSite::None,
        });
    }
    if let Some(expires) = cookie.expires {
        builder = builder.expires(TimeSinceEpoch::new(expires.unix_timestamp() as f64));
    }
    builder
        .build()
        .map_err(|error| BrowserError::CookieConversion(anyhow::anyhow!(error)))
}

pub(crate) fn from_cdp(cookie: &network::Cookie) -> Cookie {
    let expires = if cookie.session || cookie.expires <= 0.0 {
        None
    } else {
        OffsetDateTime::from_unix_timestamp(cookie.expires as i64).ok()
    };
    let mut converted = Cookie::new(
        cookie.name.clone(),
        cookie.value.clone(),
        cookie.domain.trim_start_matches('.'),
    );
    converted.host_only = !cookie.domain.starts_with('.');
    converted.path = cookie.path.clone();
    converted.expires = expires;
    converted.secure = cookie.secure;
    converted.http_only = cookie.http_only;
    converted.same_site = cookie.same_site.as_ref().map(|same_site| match same_site {
        CookieSameSite::Strict => SameSite::Strict,
        CookieSameSite::Lax => SameSite::Lax,
        CookieSameSite::None => SameSite::None,
    });
    converted
}

#[cfg(test)]
mod tests {
    use chromiumoxide::cdp::browser_protocol::network::{
        Cookie as CdpCookie, CookiePriority, CookieSameSite, CookieSourceScheme,
    };
    use millipede_browser::BrowserError;
    use millipede_core::cookies::{Cookie, SameSite};
    use time::OffsetDateTime;

    use super::{from_cdp, to_cdp};

    fn core_cookie() -> Cookie {
        let mut cookie = Cookie::new("sid", "value", "example.com");
        cookie.path = "/account".to_owned();
        cookie.expires = Some(OffsetDateTime::from_unix_timestamp(2_000_000_000).unwrap());
        cookie.secure = true;
        cookie.http_only = true;
        cookie
    }

    fn cdp_cookie(domain: &str, session: bool, expires: f64) -> CdpCookie {
        CdpCookie::builder()
            .name("sid")
            .value("value")
            .domain(domain)
            .path("/")
            .expires(expires)
            .size(8_i64)
            .http_only(true)
            .secure(false)
            .session(session)
            .priority(CookiePriority::Medium)
            .source_scheme(CookieSourceScheme::NonSecure)
            .source_port(80_i64)
            .build()
            .unwrap()
    }

    #[test]
    fn host_only_cookie_uses_url_without_domain() {
        let mut cookie = core_cookie();
        cookie.host_only = true;
        let converted = to_cdp(&cookie).unwrap();
        assert_eq!(
            converted.url.as_deref(),
            Some("https://example.com/account")
        );
        assert_eq!(converted.domain, None);
    }

    #[test]
    fn domain_cookie_roundtrip_normalizes_leading_dot() {
        let core = from_cdp(&cdp_cookie(".example.com", false, 2_000_000_000.0));
        assert_eq!(core.domain, "example.com");
        assert!(!core.host_only);
        let converted = to_cdp(&core).unwrap();
        assert_eq!(converted.domain.as_deref(), Some("example.com"));
        assert_eq!(converted.url, None);
    }

    #[test]
    fn session_expiry_is_unset_in_both_directions() {
        let mut core = core_cookie();
        core.expires = None;
        assert_eq!(to_cdp(&core).unwrap().expires, None);
        assert_eq!(
            from_cdp(&cdp_cookie("example.com", true, 2_000_000_000.0)).expires,
            None
        );
        assert_eq!(
            from_cdp(&cdp_cookie("example.com", false, -1.0)).expires,
            None
        );
    }

    #[test]
    fn maps_all_same_site_variants() {
        for (core_value, cdp_value) in [
            (SameSite::Strict, CookieSameSite::Strict),
            (SameSite::Lax, CookieSameSite::Lax),
            (SameSite::None, CookieSameSite::None),
        ] {
            let mut core = core_cookie();
            core.same_site = Some(core_value);
            assert_eq!(to_cdp(&core).unwrap().same_site, Some(cdp_value.clone()));

            let mut cdp = cdp_cookie("example.com", false, 2_000_000_000.0);
            cdp.same_site = Some(cdp_value);
            assert_eq!(from_cdp(&cdp).same_site, Some(core_value));
        }
    }

    #[test]
    fn rejects_empty_name() {
        let mut cookie = core_cookie();
        cookie.name.clear();
        assert!(matches!(
            to_cdp(&cookie),
            Err(BrowserError::CookieConversion(_))
        ));
    }
}
