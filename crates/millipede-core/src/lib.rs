#![doc = include_str!("../README.md")]

/// Crawler configuration and environment resolution.
pub mod config;
/// Crawl error taxonomy and retry classification.
pub mod errors;
/// Crawler lifecycle events and broadcast support.
pub mod events;
/// Request data types and construction helpers.
pub mod request;

/// Commonly used items from this crate.
pub mod prelude {
    pub use crate::config::{Configuration, ConfigurationBuilder, LogLevel};
    pub use crate::errors::{AntiBotTech, CrawlError};
    pub use crate::events::{
        CrawlerEvent, EventBus, EventStream, HandledRequest, RequestFinalState,
    };
    pub use crate::request::{
        HeaderMap, IntoUrl, Method, Request, RequestBody, RequestBuildError, RequestBuilder,
        RequestId, RequestState, UserData,
    };
}
