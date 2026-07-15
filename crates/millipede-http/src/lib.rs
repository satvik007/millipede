#![doc = include_str!("../README.md")]

mod client;
mod coalesce;

pub use client::{ReqwestClient, ReqwestClientOptions};
pub use coalesce::CoalescingClient;

/// Commonly used HTTP client types.
pub mod prelude {
    pub use crate::{CoalescingClient, ReqwestClient, ReqwestClientOptions};
}
