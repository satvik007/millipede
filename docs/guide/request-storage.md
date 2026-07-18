# Requests and storage

Millipede keeps crawling state behind object-safe storage traits. A `StorageClient` opens a
`Dataset`, `KeyValueStore`, and `RequestQueue`; crawler contexts expose the opened objects through
`ctx.storage`, a `StorageHandle`.

## The storage client family

`StorageClient` has four async operations:

- `open_dataset` opens a named or default dataset.
- `open_key_value_store` opens a named or default key-value store.
- `open_request_queue` opens a named or default request queue.
- `purge` removes data managed by the backend.

Within a handler, `ctx.storage.dataset()`, `ctx.storage.key_value_store()`, and
`ctx.storage.request_queue()` return the default objects. `dataset_named()` and `kvs_named()` open
named objects, while `client()` exposes the configured `StorageClient` for other operations.

```rust
use millipede::DatasetExt;

ctx.storage.dataset().push(&serde_json::json!({
    "url": ctx.request.url.as_str(),
})).await?;

let archive = ctx.storage.dataset_named("archive").await?;
archive.push_json(serde_json::json!({ "saved": true })).await?;
```

## Request queue leases

`RequestQueue` owns frontier ordering and deduplication. `add` returns `QueueOpInfo`, an alias of
`ProcessedRequest`, with fields including `was_already_present` and `was_already_handled`.
`add_batch` instead returns a `BatchAddHandle`: its public `added` field reports requests added
synchronously, and `wait()` resolves deferred completion to `AddRequestsBatchedResult`. The queue
uses each request's `unique_key`, so adding the same logical request again reports the duplicate
instead of scheduling another copy.

Workers do not receive a bare `Request`. `fetch_next()` returns an optional `Lease` containing the
request, a `lease_id`, and an `expires_at` deadline. Exactly one terminal lease operation consumes
that value:

- `mark_handled(lease)` records successful completion.
- `reclaim(lease, ReclaimOptions)` requeues the request and increments `retry_count` by default.
- `abandon(lease)` requeues without incrementing the retry count.

`renew(&lease_id, duration)` extends an active deadline without consuming the lease. Queue health
can be inspected with `is_empty`, `is_finished`, `handled_count`, and `pending_count`.

```rust
use std::time::Duration;
use millipede::ReclaimOptions;

if let Some(lease) = queue.fetch_next().await? {
    queue.renew(&lease.lease_id, Duration::from_secs(30)).await?;
    queue.reclaim(lease, ReclaimOptions::default()).await?;
}
```

The memory backend intentionally does not reassign expired leases: lease expiry is a no-op for
that single-process implementation. It still validates `mark_handled`, `reclaim`, `renew`, and
`abandon`, so application code uses the same contract as persistent or future distributed
backends.

## Datasets

`Dataset` is an append-oriented collection of JSON values. Use `push_json` when the value is
already `serde_json::Value`. Import `DatasetExt` for typed `push`, `push_batch`, `list`, and
`stream` conveniences.

`list` accepts `ListOptions`, whose fields are `offset`, optional `limit`, and `desc`. It returns a
`Page<T>` with `items`, `total`, `offset`, and `limit`.

```rust
use millipede::{DatasetExt, ListOptions};

dataset.push(&record).await?;
dataset.push_json(serde_json::json!({ "kind": "summary" })).await?;

let mut options = ListOptions::default();
options.offset = 20;
options.limit = Some(10);
options.desc = true;
let page = dataset.list::<serde_json::Value>(options).await?;
```

The object-safe core also provides `push_json_batch`, `list_raw`, `stream_raw`, `export_json`,
`export_csv`, and `info`.

## Key-value stores and saved state

`KeyValueStore` stores bytes plus a content type through `get_bytes`, `set_bytes`, `delete`, and
`list_keys`. `KeyValueStoreExt` adds typed JSON `get` and `set` operations.

`AutoSaved<T>` is a typed in-memory value paired with one key. `open(kvs, key, default)` loads an
existing JSON value or uses the supplied default. `get` clones the current value; `set` replaces
it; `update` mutates it through a synchronous closure; `persist` explicitly serializes the current
value as JSON.

```rust
use millipede::AutoSaved;

let state = AutoSaved::open(kvs, "crawl-state", Vec::<String>::new()).await?;
state.update(|urls| urls.push("https://example.com/".to_owned())).await;
state.persist().await?;
```

`set` and `update` do not persist automatically. Call `persist()` at the durability points your
application requires; the current wrapper does not subscribe itself to crawler events.

## Memory and file-system backends

`MemoryStorageClient` is enabled by the default `storage-memory` feature. It shares named objects
within one client, keeps everything in process, and loses all state when the process exits. The
feature is a default dependency, but the crawler still requires an explicit
`.storage_client(Arc::new(MemoryStorageClient::new()))` or a client in `Configuration`.

`FsStorageClient` is enabled by `storage-fs` and uses Crawlee-compatible `datasets/`,
`key_value_stores/`, and `request_queues/` directories. See
[Crawlee storage migration](./crawlee-storage-migration.md) for the exact layout and resume
procedure.

**Data-loss warning: `purge_on_start` defaults to `true`. Building a crawler against an existing
file-system storage directory will purge managed datasets, request queues, and non-`INPUT`
key-value records. Set `Configuration::builder().purge_on_start(false)` before resuming data.**

## Overriding the crawler queue

Normally `CrawlerBuilder::build` asks the configured client to open the default request queue.
`CrawlerBuilder::request_queue` replaces that queue while storage continues to supply the default
dataset and key-value store.

```rust
let crawler = millipede::Crawler::builder(kind)
    .storage_client(storage)
    .request_queue(queue)
    .request_handler(handler)
    .build()
    .await?;
```

When the replacement queue contains persistent work, pair it with `purge_on_start(false)` so the
builder does not erase its backing storage first.

## Sitemap-backed frontiers

`SitemapRequestList` lazily turns one or more sitemap URLs into `Request` values. Its builder can
set `sitemap_url` or `sitemap_urls`, `label`, `user_data`, an item `limit`, an `http_client`, and
optional KVS persistence through `persist(kvs, key)`. The list exposes `fetch_next`,
`is_finished`, `processed_count`, and explicit `persist`.

`RequestQueueWithSitemap::new(queue, list)` combines a normal queue with a
`SitemapRequestList`. It implements the complete `RequestQueue` trait, filling the queue from the
sitemap in batches when needed; `batch_size` controls that fill size. Pass the wrapper to
`CrawlerBuilder::request_queue` to let queued requests and sitemap entries drive the same crawler.

The queue remains the deduplication authority, so overlap between explicitly added requests and
sitemap entries does not create duplicate work.
