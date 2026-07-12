#![doc = include_str!("../README.md")]

/// Request data types and construction helpers.
pub mod request;

/// Commonly used items from this crate.
pub mod prelude {
    pub use crate::request::{
        HeaderMap, IntoUrl, Method, Request, RequestBody, RequestBuildError, RequestBuilder,
        RequestId, RequestState, UserData,
    };
}
