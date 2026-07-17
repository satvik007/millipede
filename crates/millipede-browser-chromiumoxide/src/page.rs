//! Chromiumoxide page adapter.

use std::time::Duration;

use async_trait::async_trait;
use chromiumoxide::{
    Page,
    cdp::browser_protocol::{
        network::{Headers, SetExtraHttpHeadersParams},
        page::CaptureScreenshotFormat,
    },
    page::ScreenshotParams,
};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use millipede_browser::{
    BrowserError, BrowserPage, BrowserResponse, GotoOptions, ScreenshotOptions, WaitUntil,
};
use millipede_core::cookies::Cookie;
use serde_json::{Map, Value};
use url::Url;

/// A cloneable adapter over a chromiumoxide page.
#[derive(Clone)]
pub struct ChromiumPage {
    page: Page,
}

impl ChromiumPage {
    pub(crate) fn new(page: Page) -> Self {
        Self { page }
    }

    pub(crate) fn into_inner(self) -> Page {
        self.page
    }
}

fn response_headers(headers: &Headers) -> HeaderMap {
    let mut converted = HeaderMap::new();
    let Some(entries) = headers.inner().as_object() else {
        tracing::debug!("ignoring CDP response headers that are not an object");
        return converted;
    };
    for (name, value) in entries {
        let Some(value) = value.as_str() else {
            tracing::debug!(
                header = name,
                ?value,
                "ignoring non-string CDP response header"
            );
            continue;
        };
        let Ok(name) = HeaderName::from_bytes(name.as_bytes()) else {
            tracing::debug!(header = name, "ignoring invalid CDP response header name");
            continue;
        };
        let Ok(value) = HeaderValue::from_str(value) else {
            tracing::debug!(?value, "ignoring invalid CDP response header value");
            continue;
        };
        converted.append(name, value);
    }
    converted
}

#[async_trait]
/// Adapts chromiumoxide operations to Millipede's provider-erased page interface.
///
/// In this first provider version, both [`WaitUntil::DomContentLoaded`] and [`WaitUntil::Load`]
/// use chromiumoxide's same navigation-response wait as a best-effort lifecycle approximation.
impl BrowserPage for ChromiumPage {
    async fn goto(
        &self,
        url: &Url,
        opts: GotoOptions,
    ) -> Result<Option<BrowserResponse>, BrowserError> {
        let timeout = opts.timeout;
        let target = url.clone();
        let navigation = async {
            self.page
                .goto(url.as_str())
                .await
                .map_err(|error| BrowserError::Navigation {
                    url: url.clone(),
                    source: anyhow::Error::new(error),
                })?;
            let request = match opts.wait_until {
                WaitUntil::DomContentLoaded | WaitUntil::Load => self
                    .page
                    .wait_for_navigation_response()
                    .await
                    .ok()
                    .flatten(),
                _ => self
                    .page
                    .wait_for_navigation_response()
                    .await
                    .ok()
                    .flatten(),
            };
            let Some(request) = request else {
                return Ok(None);
            };
            let Some(response) = request.response.as_ref() else {
                return Ok(None);
            };
            let mut converted = BrowserResponse::default();
            converted.status = StatusCode::from_u16(response.status as u16).ok();
            converted.headers = response_headers(&response.headers);
            converted.url = Url::parse(&response.url).ok();
            Ok(Some(converted))
        };
        tokio::time::timeout(timeout, navigation)
            .await
            .map_err(|_| BrowserError::NavigationTimeout {
                url: target,
                timeout,
            })?
    }

    async fn content(&self) -> Result<String, BrowserError> {
        self.page
            .content()
            .await
            .map_err(|error| BrowserError::Protocol(anyhow::Error::new(error)))
    }

    async fn evaluate_js(&self, script: &str) -> Result<Value, BrowserError> {
        let evaluation = self
            .page
            .evaluate(script)
            .await
            .map_err(|error| BrowserError::Evaluation(anyhow::Error::new(error)))?;
        Ok(evaluation.value().cloned().unwrap_or(Value::Null))
    }

    async fn evaluate_anchors(&self, selector: Option<&str>) -> Result<Vec<Url>, BrowserError> {
        let selector = selector.unwrap_or("a[href]");
        let selector_json = serde_json::to_string(selector)
            .map_err(|error| BrowserError::Evaluation(anyhow::Error::new(error)))?;
        let script = format!(
            "Array.from(document.querySelectorAll({selector_json})).map(a => a.href).filter(h => typeof h === 'string' && h.length > 0)"
        );
        let value = self.evaluate_js(&script).await?;
        let anchors: Vec<String> = serde_json::from_value(value)
            .map_err(|error| BrowserError::Evaluation(anyhow::Error::new(error)))?;
        Ok(anchors
            .into_iter()
            .filter_map(|anchor| Url::parse(&anchor).ok())
            .filter(|url| matches!(url.scheme(), "http" | "https"))
            .collect())
    }

    async fn cookies(&self) -> Result<Vec<Cookie>, BrowserError> {
        let cookies = self
            .page
            .get_cookies()
            .await
            .map_err(|error| BrowserError::Protocol(anyhow::Error::new(error)))?;
        Ok(cookies.iter().map(crate::cookie::from_cdp).collect())
    }

    async fn set_cookies(&self, cookies: &[Cookie]) -> Result<(), BrowserError> {
        let converted = cookies
            .iter()
            .map(crate::cookie::to_cdp)
            .collect::<Result<Vec<_>, _>>()?;
        self.page
            .set_cookies(converted)
            .await
            .map_err(|error| BrowserError::Protocol(anyhow::Error::new(error)))?;
        Ok(())
    }

    async fn set_extra_headers(&self, headers: &HeaderMap) -> Result<(), BrowserError> {
        let mut values = Map::new();
        for (name, value) in headers {
            let Ok(value) = value.to_str() else {
                tracing::debug!(header = %name, "ignoring non-text extra header");
                continue;
            };
            values.insert(name.as_str().to_owned(), Value::String(value.to_owned()));
        }
        let params = SetExtraHttpHeadersParams::new(Headers::new(Value::Object(values)));
        self.page
            .execute(params)
            .await
            .map_err(|error| BrowserError::Protocol(anyhow::Error::new(error)))?;
        Ok(())
    }

    async fn wait_for_selector(
        &self,
        selector: &str,
        timeout: Duration,
    ) -> Result<(), BrowserError> {
        let selector_json = serde_json::to_string(selector)
            .map_err(|error| BrowserError::Evaluation(anyhow::Error::new(error)))?;
        let script = format!("document.querySelector({selector_json}) !== null");
        let poll = async {
            loop {
                if self.evaluate_js(&script).await?.as_bool() == Some(true) {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        tokio::time::timeout(timeout, poll)
            .await
            .map_err(|_| BrowserError::WaitTimeout {
                what: format!("selector {selector}"),
                timeout,
            })?
    }

    async fn click(&self, selector: &str) -> Result<(), BrowserError> {
        let element = self
            .page
            .find_element(selector)
            .await
            .map_err(|error| BrowserError::Evaluation(anyhow::Error::new(error)))?;
        element
            .click()
            .await
            .map_err(|error| BrowserError::Protocol(anyhow::Error::new(error)))?;
        Ok(())
    }

    async fn screenshot(&self, opts: ScreenshotOptions) -> Result<bytes::Bytes, BrowserError> {
        let params = ScreenshotParams::builder()
            .format(CaptureScreenshotFormat::Png)
            .full_page(opts.full_page)
            .build();
        let bytes = self
            .page
            .screenshot(params)
            .await
            .map_err(|error| BrowserError::Protocol(anyhow::Error::new(error)))?;
        Ok(bytes::Bytes::from(bytes))
    }
}
