# Crawlee storage migration

Millipede's file-system backend is designed to open the `./storage` directory from a Crawlee
project. Point `FsStorageClient::new` at that directory to inspect existing datasets or use it as
the storage backend for a re-crawl. Datasets and key-value stores use compatible layouts.
Request-queue files written by Millipede are Crawlee-shaped, but pre-existing queues written by
Crawlee cannot currently be resumed by Millipede.

## 1. Back up first

> **Data-loss warning:** `purge_on_start` defaults to `true`. Building a crawler with its default
> configuration will delete datasets, request queues, and non-`INPUT` key-value records in the
> selected storage directory.

Before opening pre-existing data for a crawl:

1. Stop every process that writes to the storage directory.
2. Copy the complete `storage/` directory to a backup outside the project.
3. Set `Configuration::builder().purge_on_start(false)` before building the crawler.
4. Test the migration against the copy before using the original.

With purging enabled, Millipede follows Crawlee's `INPUT` preservation behavior: it retains an
`INPUT.<ext>` file in `key_value_stores/default/` while removing other managed contents. Setting
`purge_on_start(false)` retains all existing data.

## 2. Layout mapping

| Store | On-disk path | Compatibility |
|---|---|---|
| Dataset | `datasets/<id>/<9-digit>.json` | One pretty-printed JSON value per item, numbered from `000000001.json`; identical to Crawlee's layout. |
| Key-value store | `key_value_stores/<id>/<key>.<ext>` | The extension represents the media type; unknown extensions remain readable as binary data. |
| Request queue | `request_queues/<id>/requests/*.json` plus `state.json` | Request files are authoritative. `state.json` is a rebuildable Millipede cache. |

Key-value extensions map as follows:

| Extension | Content type |
|---|---|
| `.json` | `application/json` |
| `.txt` | `text/plain` |
| `.html` | `text/html` |
| `.xml` | `application/xml` |
| `.png` | `image/png` |
| `.jpeg` | `image/jpeg` |
| `.bin` or an unknown extension | `application/octet-stream` |

Each request file uses a Crawlee-shaped JSON envelope. Its public fields are `id`, `url`,
`uniqueKey`, `method`, `retryCount`, and `orderNo`; `orderNo: null` means handled. Millipede also
writes a `json` field containing the complete serialized Millipede request. On restart, Millipede
uses `orderNo` and `json` to reconstruct the queue. Crawlee queue metadata beyond these compatible
fields is not preserved byte-for-byte, so do not treat the directory as a lossless archive of
Crawlee's private queue internals.

An authentic queue envelope written by Crawlee does not contain Millipede's `json` field. Such a
record cannot currently be reconstructed as a Millipede request and is skipped as unreadable when
the queue is opened. Consequently, existing Crawlee-authored request queues are not imported for
resume or deduplication; this compatibility currently applies only to request files authored by
Millipede.

## 3. Resuming a queue

Pending requests are available after restart. Leases are process-local, so a request that was in
flight when the previous process crashed is pending again and will be re-crawled. Requests whose
`orderNo` is `null` remain handled, and queue deduplication continues to use `uniqueKey` so they are
not accepted as new work.

These resume semantics apply to queues previously written by Millipede. They do not apply to a
pre-existing Crawlee-authored request queue, whose records are skipped as described above.

This favors recovery over assuming that interrupted handler work completed. Handlers that perform
external side effects should therefore remain idempotent.

## 4. Inspecting an existing dataset

Opening a client does not purge data by itself. This example lists typed JSON values from an
existing Crawlee default dataset:

```rust,no_run
use millipede::{DatasetExt, FsStorageClient, ListOptions, StorageClient};
use serde_json::Value;

# async fn inspect() -> anyhow::Result<()> {
let storage = FsStorageClient::new("./storage");
let dataset = storage.open_dataset(Some("default")).await?;
let page = dataset.list::<Value>(ListOptions::default()).await?;

for item in page.items {
    println!("{}", serde_json::to_string_pretty(&item)?);
}
# Ok(())
# }
```

When the same client is attached to a crawler, disable startup purging explicitly:

```rust,no_run
use std::sync::Arc;
use millipede::{Configuration, Crawler, FsStorageClient};

# async fn build<K: millipede::CrawlerKind>(kind: K) -> anyhow::Result<()>
# where K::Context: Send + 'static {
let storage = Arc::new(FsStorageClient::new("./storage"));
let configuration = Configuration::builder().purge_on_start(false).build()?;

# let handler = |_ctx: K::Context| async { Ok(()) };
let crawler = Crawler::builder(kind)
    .configuration(configuration)
    .storage_client(storage)
    .request_handler(handler)
    .build()
    .await?;
# drop(crawler);
# Ok(())
# }
```

See `examples/scrape_books.rs` for a complete runnable crawl that writes a filesystem dataset in
this layout.
