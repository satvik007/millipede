#![doc = include_str!("../README.md")]

mod extract;
mod kind;
mod selectors;

pub use extract::HtmlLinkExtractor;
pub use kind::{HtmlContext, HtmlCrawler, HtmlError, HtmlKind, HtmlKindBuilder, SynchronizedHtml};
pub use scraper;

/// Commonly used items from this crate.
pub mod prelude {}
