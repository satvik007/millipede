use futures_util::{stream, stream::BoxStream};
use millipede_core::storage::{
    Dataset, DatasetInfo, ListOptions, Page, StorageError, StorageResult,
};
use serde_json::Value;
use std::{collections::BTreeSet, path::Path, sync::Mutex};
use time::OffsetDateTime;

struct DatasetState {
    items: Vec<Value>,
    created_at: OffsetDateTime,
    modified_at: OffsetDateTime,
}

/// An in-process, append-only JSON dataset.
pub struct MemoryDataset {
    name: String,
    inner: Mutex<DatasetState>,
}

impl MemoryDataset {
    /// Creates an empty dataset with the supplied name.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            name: name.into(),
            inner: Mutex::new(DatasetState {
                items: Vec::new(),
                created_at: now,
                modified_at: now,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, DatasetState> {
        // A panic while holding this lock is a programming bug, so poisoning is unrecoverable.
        self.inner.lock().expect("MemoryDataset mutex poisoned")
    }

    fn sliced_items(&self, opts: &ListOptions) -> (Vec<Value>, u64) {
        let state = self.lock();
        let total = state.items.len() as u64;
        let items: Vec<_> = if opts.desc {
            state.items.iter().rev().cloned().collect()
        } else {
            state.items.clone()
        };
        let items = items
            .into_iter()
            .skip(usize::try_from(opts.offset).unwrap_or(usize::MAX))
            .take(opts.limit.map_or(usize::MAX, |limit| {
                usize::try_from(limit).unwrap_or(usize::MAX)
            }))
            .collect();
        (items, total)
    }

    pub(crate) fn clear(&self) {
        let mut state = self.lock();
        state.items.clear();
        state.modified_at = OffsetDateTime::now_utc();
    }
}

#[async_trait::async_trait]
impl Dataset for MemoryDataset {
    async fn push_json(&self, item: Value) -> StorageResult<()> {
        let mut state = self.lock();
        state.items.push(item);
        state.modified_at = OffsetDateTime::now_utc();
        Ok(())
    }

    async fn push_json_batch(&self, items: Vec<Value>) -> StorageResult<()> {
        let mut state = self.lock();
        state.items.extend(items);
        state.modified_at = OffsetDateTime::now_utc();
        Ok(())
    }

    async fn list_raw(&self, opts: ListOptions) -> StorageResult<Page<Value>> {
        let (items, total) = self.sliced_items(&opts);
        Ok(Page {
            items,
            total,
            offset: opts.offset,
            limit: opts.limit,
        })
    }

    fn stream_raw(&self, opts: ListOptions) -> BoxStream<'_, StorageResult<Value>> {
        let (items, _) = self.sliced_items(&opts);
        Box::pin(stream::iter(items.into_iter().map(Ok)))
    }

    async fn export_json(&self, path: &Path) -> StorageResult<()> {
        let items = self.lock().items.clone();
        let bytes = serde_json::to_vec_pretty(&items)?;
        // This synchronous write is bounded to an already-materialized in-memory buffer.
        std::fs::write(path, bytes).map_err(StorageError::Io)
    }

    async fn export_csv(&self, path: &Path) -> StorageResult<()> {
        let items = self.lock().items.clone();
        let mut columns = BTreeSet::new();
        for item in &items {
            let object = item.as_object().ok_or(StorageError::Unsupported(
                "export_csv requires object items",
            ))?;
            columns.extend(object.keys().cloned());
        }
        let columns: Vec<_> = columns.into_iter().collect();
        let mut rows = vec![
            columns
                .iter()
                .map(|key| csv_field(key))
                .collect::<Vec<_>>()
                .join(","),
        ];
        for item in &items {
            let object = item.as_object().expect("objects validated above");
            rows.push(
                columns
                    .iter()
                    .map(|key| match object.get(key) {
                        None => String::new(),
                        Some(Value::String(value)) => csv_field(value),
                        Some(value) => csv_field(&value.to_string()),
                    })
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
        // This synchronous write is bounded to an already-materialized in-memory buffer.
        std::fs::write(path, rows.join("\r\n")).map_err(StorageError::Io)
    }

    async fn info(&self) -> StorageResult<DatasetInfo> {
        let state = self.lock();
        Ok(DatasetInfo::new(
            self.name.clone(),
            state.items.len() as u64,
            state.created_at,
            state.modified_at,
        ))
    }
}

fn csv_field(value: &str) -> String {
    if value.contains([',', '"', '\r', '\n']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}
