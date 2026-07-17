#![doc = include_str!("../README.md")]

mod kind;
mod selectors;

pub use kind::{HtmlContext, HtmlCrawler, HtmlError, HtmlKind, HtmlKindBuilder, SynchronizedHtml};
pub use scraper;

/// Commonly used items from this crate.
pub mod prelude {}
