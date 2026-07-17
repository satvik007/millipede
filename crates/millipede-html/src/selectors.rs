/// Declares lazily parsed, process-wide CSS selector accessors.
///
/// Each generated function parses its CSS literal at most once and returns the same
/// `&'static` selector thereafter. Invalid selectors panic on first use.
///
/// # Example
///
/// ```
/// millipede_html::selectors! {
///     pub title_sel = "title";
///     product = "article.product_pod h3 a";
/// }
///
/// struct HandlerContext {
///     html: millipede_html::scraper::Html,
/// }
///
/// fn handler(ctx: &HandlerContext) {
///     for title in ctx.html.select(title_sel()) {
///         println!("{}", title.text().collect::<String>());
///     }
///     let _products = ctx.html.select(product()).count();
/// }
///
/// let ctx = HandlerContext {
///     html: millipede_html::scraper::Html::parse_document(
///         "<title>Millipede</title><article class='product_pod'><h3><a>Item</a></h3></article>",
///     ),
/// };
/// handler(&ctx);
/// ```
#[macro_export]
macro_rules! selectors {
    ($( $vis:vis $name:ident = $css:literal; )+) => { $(
        $vis fn $name() -> &'static $crate::scraper::Selector {
            static SELECTOR: ::std::sync::OnceLock<$crate::scraper::Selector> = ::std::sync::OnceLock::new();
            SELECTOR.get_or_init(|| {
                $crate::scraper::Selector::parse($css)
                    .unwrap_or_else(|error| panic!("invalid CSS selector {:?}: {error:?}", $css))
            })
        }
    )+ };
}

#[cfg(test)]
mod tests {
    crate::selectors! {
        test_selector = "a.detail[href]";
        invalid_selector = "a[";
    }

    #[test]
    fn returns_same_static_selector() {
        assert!(std::ptr::eq(test_selector(), test_selector()));
    }

    #[test]
    #[should_panic(expected = "invalid CSS selector")]
    fn invalid_selector_panics_on_first_use() {
        let _ = invalid_selector();
    }
}
