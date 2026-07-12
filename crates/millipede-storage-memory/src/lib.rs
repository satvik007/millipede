#![doc = include_str!("../README.md")]

mod client;
mod dataset;
mod kvs;
mod policy;
mod queue;

pub use client::MemoryStorageClient;
pub use dataset::MemoryDataset;
pub use kvs::MemoryKeyValueStore;
pub use policy::{DomainRoundRobin, MemoryQueuePolicy};
pub use queue::MemoryRequestQueue;

/// Commonly used items from this crate.
pub mod prelude {
    pub use crate::{
        DomainRoundRobin, MemoryDataset, MemoryKeyValueStore, MemoryQueuePolicy,
        MemoryRequestQueue, MemoryStorageClient,
    };
}
