#![doc = include_str!("../README.md")]

mod client;
mod dataset;
mod kvs;

pub use client::MemoryStorageClient;
pub use dataset::MemoryDataset;
pub use kvs::MemoryKeyValueStore;

/// Commonly used items from this crate.
pub mod prelude {
    pub use crate::{MemoryDataset, MemoryKeyValueStore, MemoryStorageClient};
}
