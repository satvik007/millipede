#![doc = include_str!("../README.md")]

mod client;
mod coalesce;
mod kind;

pub use client::{ReqwestClient, ReqwestClientOptions};
pub use coalesce::CoalescingClient;
pub use kind::{HttpContext, HttpCrawler, HttpKind, HttpKindBuilder};

/// Commonly used HTTP client types.
pub mod prelude {
    pub use crate::{
        CoalescingClient, HttpContext, HttpCrawler, HttpKind, HttpKindBuilder, ReqwestClient,
        ReqwestClientOptions,
    };
}
