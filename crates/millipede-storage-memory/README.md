# millipede-storage-memory

In-memory StorageClient implementation for the Millipede web crawler.

[![crates.io](https://img.shields.io/crates/v/millipede-storage-memory.svg)](https://crates.io/crates/millipede-storage-memory) [![docs.rs](https://docs.rs/millipede-storage-memory/badge.svg)](https://docs.rs/millipede-storage-memory) [![license](https://img.shields.io/crates/l/millipede-storage-memory.svg)](https://github.com/satvik007/millipede#license)

This crate provides [Millipede](https://github.com/satvik007/millipede)'s default storage backend: process-local datasets, key-value stores, and a lease-based request queue with deduplication.

## Installation

```toml
[dependencies]
millipede-storage-memory = "0.1"
```

Most users should depend on the umbrella [`millipede`](https://crates.io/crates/millipede) crate instead.

## Example

```rust
use millipede_core::prelude::{AddOptions, Request, RequestQueue};
use millipede_storage_memory::MemoryRequestQueue;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = MemoryRequestQueue::new("example");
    let request = Request::get("https://example.com/").build()?;

    queue.add(request, AddOptions::default()).await?;
    let lease = queue.fetch_next().await?.expect("request was queued");
    queue.mark_handled(lease).await?;

    assert!(queue.is_finished().await?);
    Ok(())
}
```

## Part of Millipede

See the [Millipede guide](https://github.com/satvik007/millipede/tree/main/docs/guide) for request queues, datasets, key-value stores, and backend selection.

## License

Licensed under either **MIT OR Apache-2.0** at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this crate is dual-licensed as above, without any additional terms or conditions.
