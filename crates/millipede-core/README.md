# millipede-core

Core primitives for the Millipede web crawler: request model, storage traits, events, errors, configuration.

[![crates.io](https://img.shields.io/crates/v/millipede-core.svg)](https://crates.io/crates/millipede-core) [![docs.rs](https://docs.rs/millipede-core/badge.svg)](https://docs.rs/millipede-core) [![license](https://img.shields.io/crates/l/millipede-core.svg)](https://github.com/satvik007/millipede#license)

This crate provides the generic crawler engine and the request, handler, routing, session, proxy, storage, event, and statistics abstractions shared by the [Millipede](https://github.com/satvik007/millipede) ecosystem.

## Installation

```toml
[dependencies]
millipede-core = "0.1"
```

Most users should depend on the umbrella [`millipede`](https://crates.io/crates/millipede) crate instead.

## Example

```rust,no_run
use std::sync::Arc;

use millipede_core::prelude::{BasicContext, BasicKind, Crawler, Request};
use millipede_storage_memory::MemoryStorageClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let crawler = Crawler::builder(BasicKind)
        .storage_client(Arc::new(MemoryStorageClient::new()))
        .request_handler(|ctx: BasicContext| async move {
            println!("handling {}", ctx.request.url);
            Ok(())
        })
        .build()
        .await?;

    let seed = Request::get("https://example.com/").build()?;
    let stats = crawler.run([seed]).await?;
    assert_eq!(stats.requests_finished, 1);
    Ok(())
}
```

## Part of Millipede

See the [Millipede guide](https://github.com/satvik007/millipede/tree/main/docs/guide) for crawler concepts, storage, retries, sessions, and migration guidance.

## License

Licensed under either **MIT OR Apache-2.0** at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this crate is dual-licensed as above, without any additional terms or conditions.
