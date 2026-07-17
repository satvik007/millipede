use std::sync::Arc;

use millipede_core::{
    errors::CrawlError,
    link_extraction::{ExtractedLink, LinkExtractor},
};
use scraper::Selector;
use url::Url;

use crate::SynchronizedHtml;

/// Extracts raw link targets from an already-parsed HTML document.
pub struct HtmlLinkExtractor {
    html: Arc<SynchronizedHtml>,
    document_url: Url,
}

impl HtmlLinkExtractor {
    /// Creates an extractor for `html`, resolving document bases against `document_url`.
    pub fn new(html: Arc<scraper::Html>, document_url: Url) -> Self {
        Self::from_synchronized(
            Arc::new(SynchronizedHtml::from_html(Arc::unwrap_or_clone(html))),
            document_url,
        )
    }

    pub(crate) fn from_synchronized(html: Arc<SynchronizedHtml>, document_url: Url) -> Self {
        Self { html, document_url }
    }
}

impl LinkExtractor for HtmlLinkExtractor {
    fn extract(&self, selector: Option<&str>) -> Result<Vec<ExtractedLink>, CrawlError> {
        let selector_text = selector.unwrap_or("a[href]");
        let selector = Selector::parse(selector_text).map_err(|error| {
            CrawlError::non_retryable(anyhow::anyhow!(
                "invalid link selector {selector_text:?}: {error}"
            ))
        })?;
        let base_selector = Selector::parse("base[href]")
            .expect("the built-in base[href] selector must always parse");

        Ok(self.html.with_html(|html| {
            let effective_base = html
                .select(&base_selector)
                .next()
                .and_then(|element| element.value().attr("href"))
                .and_then(|href| self.document_url.join(href).ok())
                .unwrap_or_else(|| self.document_url.clone());

            html.select(&selector)
                .filter_map(|element| element.value().attr("href"))
                .map(|href| ExtractedLink {
                    url: href.to_owned(),
                    base: Some(effective_base.clone()),
                })
                .collect()
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn extractor(document: &str) -> HtmlLinkExtractor {
        HtmlLinkExtractor::new(
            #[allow(clippy::arc_with_non_send_sync)]
            Arc::new(scraper::Html::parse_document(document)),
            Url::parse("https://example.test/landing/page").expect("document URL must parse"),
        )
    }

    #[test]
    fn extracts_raw_hrefs_against_the_first_valid_base() {
        let links = extractor(
            r#"<base href="/catalog/"><base href="/ignored/">
               <a href="item">default</a><a class="product" href="../other">other</a>"#,
        )
        .extract(None)
        .expect("default selector must extract");

        assert_eq!(links.len(), 2);
        assert_eq!(links[0].url, "item");
        assert_eq!(
            links[0].base.as_ref().map(Url::as_str),
            Some("https://example.test/catalog/")
        );
    }

    #[test]
    fn missing_base_uses_document_url_for_every_link() {
        let links = extractor(r#"<a href="child">child</a>"#)
            .extract(None)
            .expect("default selector must extract");

        assert_eq!(links.len(), 1);
        assert_eq!(
            links[0].base.as_ref().map(Url::as_str),
            Some("https://example.test/landing/page")
        );
    }

    #[test]
    fn invalid_first_base_falls_back_to_document_url_and_ignores_later_bases() {
        let links = extractor(
            r#"<base href="http://["><base href="/later-valid/">
               <a href="child">child</a>"#,
        )
        .extract(None)
        .expect("default selector must extract");

        assert_eq!(links.len(), 1);
        assert_eq!(
            links[0].base.as_ref().map(Url::as_str),
            Some("https://example.test/landing/page")
        );
    }

    #[test]
    fn custom_selector_still_skips_elements_without_href() {
        let links =
            extractor(r#"<a class="product">missing</a><a class="product" href="kept">kept</a>"#)
                .extract(Some("a.product"))
                .expect("custom selector must extract");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].url, "kept");
    }

    #[test]
    fn invalid_selector_reports_source_text() {
        let error = extractor("")
            .extract(Some("a["))
            .expect_err("invalid selector must fail");
        assert!(error.to_string().contains("a["));
    }
}
