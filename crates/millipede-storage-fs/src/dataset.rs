use futures_util::{StreamExt, stream, stream::BoxStream};
use millipede_core::storage::{
    Dataset, DatasetInfo, ListOptions, Page, StorageError, StorageResult,
};
use serde_json::Value;
use std::{collections::BTreeSet, path::Path, path::PathBuf, sync::Arc, time::SystemTime};
use time::OffsetDateTime;
use tokio::sync::{Mutex, RwLock};

use crate::layout::{is_temporary_file, temporary_suffix};

const MAX_SEQUENCE: u64 = 999_999_999;

/// A file-system-backed, append-only JSON dataset.
pub struct FsDataset {
    name: String,
    path: PathBuf,
    operations: Arc<RwLock<()>>,
    next_sequence: Mutex<u64>,
}

impl FsDataset {
    pub(crate) async fn open(
        name: String,
        path: PathBuf,
        operations: Arc<RwLock<()>>,
    ) -> StorageResult<Self> {
        let next_sequence = max_sequence(&path).await?.saturating_add(1).max(1);
        Ok(Self {
            name,
            path,
            operations,
            next_sequence: Mutex::new(next_sequence),
        })
    }

    pub(crate) async fn reset_sequence(&self) {
        *self.next_sequence.lock().await = 1;
    }

    async fn read_items(&self) -> StorageResult<Vec<Value>> {
        tokio::fs::create_dir_all(&self.path).await?;
        let files = item_files(&self.path).await?;
        let mut items = Vec::with_capacity(files.len());
        for (_, path) in files {
            let bytes = tokio::fs::read(&path).await?;
            match serde_json::from_slice(&bytes) {
                Ok(item) => items.push(item),
                Err(error) => {
                    // Atomic appends prevent this backend from exposing partial writes. Skip
                    // corrupt files as well because Crawlee-compatible directories may have
                    // been created by another process or left over from an older crash.
                    tracing::warn!(
                        dataset = %self.name,
                        path = %path.display(),
                        %error,
                        "skipping corrupt dataset item"
                    );
                }
            }
        }
        Ok(items)
    }

    async fn append_locked(&self, next: &mut u64, item: &Value) -> StorageResult<()> {
        if *next > MAX_SEQUENCE {
            return Err(StorageError::Backend(anyhow::anyhow!(
                "dataset {} exhausted its nine-digit sequence space",
                self.name
            )));
        }
        tokio::fs::create_dir_all(&self.path).await?;
        let destination = self.path.join(format!("{next:09}.json"));
        let temporary = self
            .path
            .join(format!("{next:09}.json.{}", temporary_suffix()));
        tokio::fs::write(&temporary, serde_json::to_vec_pretty(item)?).await?;
        if let Err(error) = tokio::fs::rename(&temporary, &destination).await {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(error.into());
        }
        *next += 1;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Dataset for FsDataset {
    async fn push_json(&self, item: Value) -> StorageResult<()> {
        let _operation = self.operations.read().await;
        let mut next = self.next_sequence.lock().await;
        self.append_locked(&mut next, &item).await
    }

    async fn push_json_batch(&self, items: Vec<Value>) -> StorageResult<()> {
        let _operation = self.operations.read().await;
        let mut next = self.next_sequence.lock().await;
        for item in items {
            self.append_locked(&mut next, &item).await?;
        }
        Ok(())
    }

    async fn list_raw(&self, opts: ListOptions) -> StorageResult<Page<Value>> {
        let _operation = self.operations.read().await;
        let _guard = self.next_sequence.lock().await;
        let mut items = self.read_items().await?;
        let total = items.len() as u64;
        if opts.desc {
            items.reverse();
        }
        let items = items
            .into_iter()
            .skip(usize::try_from(opts.offset).unwrap_or(usize::MAX))
            .take(opts.limit.map_or(usize::MAX, |limit| {
                usize::try_from(limit).unwrap_or(usize::MAX)
            }))
            .collect();
        Ok(Page {
            items,
            total,
            offset: opts.offset,
            limit: opts.limit,
        })
    }

    fn stream_raw(&self, opts: ListOptions) -> BoxStream<'_, StorageResult<Value>> {
        let page = async move {
            match self.list_raw(opts).await {
                Ok(page) => page.items.into_iter().map(Ok).collect::<Vec<_>>(),
                Err(error) => vec![Err(error)],
            }
        };
        Box::pin(stream::once(page).flat_map(stream::iter))
    }

    async fn export_json(&self, path: &Path) -> StorageResult<()> {
        let _operation = self.operations.read().await;
        let _guard = self.next_sequence.lock().await;
        let items = self.read_items().await?;
        tokio::fs::write(path, serde_json::to_vec_pretty(&items)?).await?;
        Ok(())
    }

    async fn export_csv(&self, path: &Path) -> StorageResult<()> {
        let _operation = self.operations.read().await;
        let _guard = self.next_sequence.lock().await;
        let items = self.read_items().await?;
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
        tokio::fs::write(path, rows.join("\r\n")).await?;
        Ok(())
    }

    /// Returns metadata derived from directory and item-file timestamps.
    ///
    /// File systems do not expose Crawlee's logical creation metadata, so the
    /// directory creation time (falling back to its modification time) and the
    /// newest directory or item modification time are approximations.
    async fn info(&self) -> StorageResult<DatasetInfo> {
        let _operation = self.operations.read().await;
        let _guard = self.next_sequence.lock().await;
        tokio::fs::create_dir_all(&self.path).await?;
        let directory = tokio::fs::metadata(&self.path).await?;
        let directory_modified = directory.modified()?;
        let created = directory.created().unwrap_or(directory_modified);
        let files = item_files(&self.path).await?;
        let mut modified = directory_modified;
        for (_, path) in &files {
            let candidate = tokio::fs::metadata(path).await?.modified()?;
            modified = modified.max(candidate);
        }
        Ok(DatasetInfo::new(
            self.name.clone(),
            files.len() as u64,
            system_time(created),
            system_time(modified),
        ))
    }
}

async fn max_sequence(path: &Path) -> StorageResult<u64> {
    Ok(item_files(path)
        .await?
        .last()
        .map_or(0, |(sequence, _)| *sequence))
}

async fn item_files(path: &Path) -> StorageResult<Vec<(u64, PathBuf)>> {
    let mut entries = tokio::fs::read_dir(path).await?;
    let mut files = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if is_temporary_file(name) {
            continue;
        }
        let Some(sequence) = dataset_sequence(name) else {
            continue;
        };
        files.push((sequence, entry.path()));
    }
    files.sort_unstable_by_key(|(sequence, _)| *sequence);
    Ok(files)
}

fn dataset_sequence(name: &str) -> Option<u64> {
    if name.len() != 14 || !name.ends_with(".json") {
        return None;
    }
    let digits = &name[..9];
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok().filter(|sequence| *sequence > 0)
}

fn csv_field(value: &str) -> String {
    if value.contains([',', '"', '\r', '\n']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

fn system_time(value: SystemTime) -> OffsetDateTime {
    OffsetDateTime::from(value)
}
