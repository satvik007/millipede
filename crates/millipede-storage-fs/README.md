# millipede-storage-fs

File-system [`StorageClient`](https://docs.rs/millipede-core/latest/millipede_core/storage/trait.StorageClient.html)
implementation for the Millipede web crawler.

`FsStorageClient` persists datasets and key-value stores below a configurable
storage root:

| Resource | Layout |
| --- | --- |
| Dataset item | `datasets/<id>/<9-digit-sequence>.json` |
| Key-value entry | `key_value_stores/<id>/<key>.<content-type-extension>` |
| Request queue | `request_queues/<id>/` (reserved for a later Phase 5 step) |

The layout is wire-compatible with Crawlee's local `MemoryStorage` conventions:
an existing Crawlee `./storage` directory can be inspected and reopened. Purging
preserves `key_value_stores/default/INPUT.<ext>` for migration parity.

```no_run
use millipede_core::storage::{DatasetExt, StorageClient};
use millipede_storage_fs::FsStorageClient;
use serde_json::json;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let storage = FsStorageClient::new("./storage");
let dataset = storage.open_dataset(None).await?;
dataset.push(&json!({ "url": "https://example.com" })).await?;
# Ok(())
# }
```
