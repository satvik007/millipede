#![doc = include_str!("../README.md")]

mod client;
mod dataset;
mod kvs;
mod layout;
mod queue;

pub use client::FsStorageClient;
pub use dataset::FsDataset;
pub use kvs::FsKeyValueStore;
pub use queue::FsRequestQueue;

/// Commonly used items from this crate.
pub mod prelude {}
