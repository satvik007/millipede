#![doc = include_str!("../README.md")]

mod kind;

pub use kind::{HtmlContext, HtmlCrawler, HtmlError, HtmlKind, HtmlKindBuilder, SynchronizedHtml};

/// Commonly used items from this crate.
pub mod prelude {}
