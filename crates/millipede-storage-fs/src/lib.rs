#![doc = include_str!("../README.md")]

mod client;
mod dataset;
mod kvs;
mod layout;

pub use client::FsStorageClient;
pub use dataset::FsDataset;
pub use kvs::FsKeyValueStore;

/// Commonly used items from this crate.
pub mod prelude {}
