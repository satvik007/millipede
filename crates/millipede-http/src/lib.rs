#![doc = include_str!("../README.md")]

mod client;
mod coalesce;
mod kind;
pub mod nav;

pub use client::{ReqwestClient, ReqwestClientOptions};
pub use coalesce::CoalescingClient;
pub use kind::{HttpContext, HttpCrawler, HttpKind, HttpKindBuilder};
pub use nav::{HttpPostHookCtx, HttpPostNavigationHook, HttpPreHookCtx, HttpPreNavigationHook};

/// Commonly used HTTP client types.
pub mod prelude {
    pub use crate::{
        CoalescingClient, HttpContext, HttpCrawler, HttpKind, HttpKindBuilder, HttpPostHookCtx,
        HttpPostNavigationHook, HttpPreHookCtx, HttpPreNavigationHook, ReqwestClient,
        ReqwestClientOptions,
    };
}
