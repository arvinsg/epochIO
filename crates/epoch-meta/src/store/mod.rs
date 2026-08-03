//! State-machine storage abstraction: one neutral [`StoreOp`] batch interface
//! implemented per engine — [`RocksEngine`] (default) here, the experimental
//! MemEngine in M5b. A partition's raft state machine holds an
//! `Arc<dyn MetaStore>`; one engine instance is shared by every partition of
//! the same kind on a node, partitions staying logically isolated by their
//! routing-key ranges (no partition id enters this layer).
//!
//! Design: docs/design/03-metanode.md §7 (MetaStore trait 双实现), §2 (同进程
//! 共享存储实例承载全部分区)

pub mod keys;
pub mod mem;
pub mod rocks;

pub use keys::MetaCf;
pub use mem::MemEngine;
pub use rocks::RocksEngine;

/// One metadata mutation in a neutral, engine-independent form.
///
/// Design: docs/design/03-metanode.md §7 — "StoreOp = (Cf, Key, Op) 中立类型，
/// 非 RocksDB WriteBatch", so the MemEngine applies the same batches and the two
/// engines can be equivalence-tested against each other.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoreOp {
    /// The column family the key belongs to (its first byte is `cf.tag()`).
    pub cf: MetaCf,
    /// Full encoded key, built by [`keys`] helpers (03 §4.1 layout).
    pub key: Vec<u8>,
    /// `Some` puts, `None` deletes.
    pub value: Option<Vec<u8>>,
}

impl StoreOp {
    /// A put of `value` at `key` in `cf`.
    #[must_use]
    pub fn put(cf: MetaCf, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            cf,
            key: key.into(),
            value: Some(value.into()),
        }
    }

    /// A delete of `key` in `cf`.
    #[must_use]
    pub fn delete(cf: MetaCf, key: impl Into<Vec<u8>>) -> Self {
        Self {
            cf,
            key: key.into(),
            value: None,
        }
    }
}

/// A half-open key range `[start, end)` inside one column family — the export
/// unit for partition migration / learner catch-up (03 §2: 迁移才动数据; §4.1
/// 导出连续性: a partition interval is a contiguous segment per CF).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KeyRange {
    /// The column family to export from.
    pub cf: MetaCf,
    /// Inclusive range start (full encoded key).
    pub start: Vec<u8>,
    /// Exclusive range end (full encoded key).
    pub end: Vec<u8>,
}

/// A consistent point-in-time export over a set of [`KeyRange`]s.
///
/// v1 collects entries in memory: partitions split long before a range export
/// outgrows RAM (03 §2 任何热点分区都可即时分裂). Chunked streaming lands with
/// `partition/migrate.rs` if real sizes demand it (06 §9).
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct StoreSnapshot {
    /// `(cf, key, value)` entries, grouped by input range order, key-ordered
    /// within each range.
    pub entries: Vec<(MetaCf, Vec<u8>, Vec<u8>)>,
}

/// One page of an ordered scan: `(key, value)` pairs in key order.
pub type KvPage = Vec<(Vec<u8>, Vec<u8>)>;

/// Errors of the storage engines.
#[derive(Debug, thiserror::Error)]
pub enum MetaStoreError {
    /// A RocksDB failure, classified by epoch-rocks (disk failure vs other,
    /// 06 §5 EIO contract).
    #[error(transparent)]
    Rocks(#[from] epoch_rocks::RocksError),
    /// A CF declared in [`MetaCf`] was not opened with the engine.
    #[error("missing column family: {0}")]
    MissingCf(&'static str),
    /// A stored value failed to decode, or a value failed to encode (metadata
    /// corruption — distinct from engine failures for alerting).
    #[error("metadata value codec: {0}")]
    ValueCodec(String),
}

impl From<rocksdb::Error> for MetaStoreError {
    fn from(e: rocksdb::Error) -> Self {
        epoch_rocks::RocksError::from(e).into()
    }
}

/// The partition-level storage interface raft state machines apply through.
///
/// Design: docs/design/03-metanode.md §7. Implementations must make
/// [`apply`](MetaStore::apply) atomic and idempotent; reads are always served
/// from the local replica (linearity comes from the raft layer's ReadIndex,
/// 03 §5, not from this trait).
pub trait MetaStore: Send + Sync {
    /// Atomically persists one raft apply batch (03 §8: data ops and the
    /// applied record commit in the same batch).
    ///
    /// INVARIANT(design 03 §8): the batch's last op must be the partition's
    /// applied-record put (targeting [`MetaCf::Applied`], built by the state
    /// machine) — it caps log replay after a crash, and being in the same
    /// batch means data and apply position are lost or committed together.
    ///
    /// Idempotent: raft replay re-applies the same batch, and MemEngine's
    /// fuzzy-snapshot overlap replay relies on re-apply being a no-op (03 §7).
    /// Puts overwrite and deletes tolerate absence, so plain batches are
    /// idempotent by construction.
    fn apply(&self, ops: &[StoreOp]) -> Result<(), MetaStoreError>;

    /// Point lookup.
    ///
    /// # Errors
    ///
    /// Returns [`MetaStoreError`] on engine failure.
    fn get(&self, cf: MetaCf, key: &[u8]) -> Result<Option<Vec<u8>>, MetaStoreError>;

    /// Ordered scan of `[start, end)` in one CF, at most `limit` entries
    /// (LIST/readdir pagination; per 03 §4.2 a LIST only ever touches the
    /// primary-index CF).
    ///
    /// # Errors
    ///
    /// Returns [`MetaStoreError`] on engine failure.
    fn scan(
        &self,
        cf: MetaCf,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<KvPage, MetaStoreError>;

    /// Consistent point-in-time export of `ranges` (partition migration /
    /// learner snapshot, 03 §2). Collects the whole set in memory — used by the
    /// openraft snapshot builder, whose wire transfer is itself chunked by
    /// `Config::snapshot_max_chunk_size`. For a bounded-memory export over a
    /// large partition (03 §4.1 导出连续性), prefer [`export_page`](Self::export_page).
    ///
    /// # Errors
    ///
    /// Returns [`MetaStoreError`] on engine failure.
    fn snapshot(&self, ranges: &[KeyRange]) -> Result<StoreSnapshot, MetaStoreError>;

    /// One bounded page of a streaming range export (D2 — chunked streaming for
    /// migration, 03 §2/§4.1). Walks `ranges` in order, resuming strictly after
    /// `cursor` (`None` = from the first range's start), yielding at most
    /// `limit` `(cf, key, value)` entries and the cursor to pass to the next
    /// call (`None` when the export is exhausted). Because each range is a
    /// contiguous per-CF segment (03 §4.1), paging never rescans and holds only
    /// one page in memory — a 10-million-object partition exports in O(page)
    /// RAM rather than O(partition).
    ///
    /// The default implementation pages over [`scan`](Self::scan); both engines
    /// inherit it, keeping the streaming contract identical (and equivalence-
    /// testable) across Rocks and Mem.
    ///
    /// # Errors
    ///
    /// Returns [`MetaStoreError`] on engine failure.
    fn export_page(
        &self,
        ranges: &[KeyRange],
        cursor: Option<&ExportCursor>,
        limit: usize,
    ) -> Result<ExportPage, MetaStoreError> {
        let mut entries = Vec::new();
        // The range index to start from, and the key within it to resume after.
        let (start_idx, mut resume_after) = match cursor {
            Some(c) => (c.range_index, Some(c.after_key.clone())),
            None => (0, None),
        };
        for (idx, range) in ranges.iter().enumerate().skip(start_idx) {
            // Within a resumed range, start strictly after the cursor key; a
            // fresh range starts at its own lower bound.
            let mut scan_start = match resume_after.take() {
                Some(mut k) => {
                    k.push(0); // exclusive of the cursor key
                    k
                }
                None => range.start.clone(),
            };
            loop {
                if entries.len() >= limit {
                    // Page full: the next call resumes in this range after the
                    // last emitted key.
                    let after_key = entries
                        .last()
                        .map(|(_, k, _): &(MetaCf, Vec<u8>, Vec<u8>)| k.clone())
                        .expect("page full implies at least one entry");
                    return Ok(ExportPage {
                        entries,
                        next: Some(ExportCursor {
                            range_index: idx,
                            after_key,
                        }),
                    });
                }
                let page = self.scan(range.cf, &scan_start, &range.end, limit - entries.len())?;
                if page.is_empty() {
                    break; // this range exhausted → advance to the next
                }
                scan_start = page
                    .last()
                    .map(|(k, _)| {
                        let mut next = k.clone();
                        next.push(0);
                        next
                    })
                    .expect("non-empty page");
                for (key, value) in page {
                    entries.push((range.cf, key, value));
                }
            }
        }
        Ok(ExportPage {
            entries,
            next: None,
        })
    }
}

/// The resume position of a streaming range export ([`MetaStore::export_page`]):
/// the range being walked and the last key emitted from it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExportCursor {
    /// Index into the `ranges` slice the export is currently walking.
    pub range_index: usize,
    /// The last key emitted; the next page resumes strictly after it.
    pub after_key: Vec<u8>,
}

/// One page of a streaming range export: the entries plus the cursor to resume
/// from (`None` when the export is complete).
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ExportPage {
    /// `(cf, key, value)` entries, in export order.
    pub entries: Vec<(MetaCf, Vec<u8>, Vec<u8>)>,
    /// The resume cursor, or `None` at the end of the export.
    pub next: Option<ExportCursor>,
}
