# millipede-storage-fs

File-system StorageClient implementation for the Millipede web crawler.

[![crates.io](https://img.shields.io/crates/v/millipede-storage-fs.svg)](https://crates.io/crates/millipede-storage-fs) [![docs.rs](https://docs.rs/millipede-storage-fs/badge.svg)](https://docs.rs/millipede-storage-fs) [![license](https://img.shields.io/crates/l/millipede-storage-fs.svg)](https://github.com/satvik007/millipede#license)

This crate persists [Millipede](https://github.com/satvik007/millipede) datasets, key-value stores, and request queues beneath a configurable local directory.

**Data-loss warning:** `purge_on_start` defaults to `true`. Starting a crawler with its default configuration purges datasets, request queues, and non-`INPUT` key-value-store records in the selected storage directory.

The on-disk layout is compatible with Crawlee's `./storage` directory; read the [Crawlee storage migration guide](https://github.com/satvik007/millipede/blob/main/docs/guide/crawlee-storage-migration.md) before opening existing data.

## Installation

```toml
[dependencies]
millipede-storage-fs = "0.1"
```

Most users should depend on the umbrella [`millipede`](https://crates.io/crates/millipede) crate with its file-system storage feature instead.

## Example

```rust,no_run
use millipede_core::prelude::{DatasetExt, StorageClient};
use millipede_storage_fs::FsStorageClient;
use serde_json::json;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let directory = tempfile::tempdir()?;
let storage = FsStorageClient::new(directory.path());
let dataset = storage.open_dataset(None).await?;
dataset.push(&json!({ "url": "https://example.com/" })).await?;
# Ok(())
# }
```

## Part of Millipede

See the [Millipede guide](https://github.com/satvik007/millipede/tree/main/docs/guide) for storage configuration, persistence, and migration guidance.

## License

Licensed under either **MIT OR Apache-2.0** at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this crate is dual-licensed as above, without any additional terms or conditions.
