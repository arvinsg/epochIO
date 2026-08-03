//! Shared persistence mechanics for the id-keyed record column families backed
//! by the PD state machine (`node`, `disk`, `chunk`).
//!
//! Each such column family stores one serde-encoded record per entity, keyed by
//! its 4-byte big-endian `u32` id, plus a monotonic id-allocation counter under
//! [`NEXT_ID_KEY`]. The three managers ([`NodeManager`](super::node::NodeManager),
//! [`DiskManager`](super::disk::DiskManager), and the chunk manager) keep their
//! own in-memory index and domain rules (idempotency keys, secondary indexes,
//! staging) but share these mechanics (AGENTS §11: extract the common owner once
//! a third copy appears).
//!
//! INVARIANT(design 01 §2 / AGENTS §8): ids come from the persisted counter, so
//! allocation is deterministic across replicas; the counter key is 7 bytes and
//! can never collide with a 4-byte record key.
//!
//! Design: docs/design/01-pd.md §1 (ID allocation); §2 (apply / snapshot)

// These helpers return openraft's intentionally-large `StorageError` (see the
// `raft` module): they run inside the raft state machine on behalf of the
// cluster managers, so boxing it is not an option. Scope the allow to this
// module.
#![allow(clippy::result_large_err)]

use rocksdb::{ColumnFamily, DB, IteratorMode, WriteBatch};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::raft::{SmError, missing_cf, sm_corrupt, sm_read_err, sm_write_err};

/// The first id handed out for any id-keyed column family. Starting at 1 keeps
/// the `0` id free as an unambiguous "unset" sentinel for callers.
pub(crate) const FIRST_ID: u32 = 1;

/// Key of the id-allocation counter. Seven bytes long, so it can never collide
/// with a 4-byte id record key.
const NEXT_ID_KEY: &[u8] = b"next_id";

/// The 4-byte big-endian record key for a `u32` id.
pub(crate) fn record_key(id: u32) -> [u8; 4] {
    id.to_be_bytes()
}

/// Resolves the column family handle, mapping a missing family to a
/// state-machine read error (an open-time invariant violation).
///
/// # Errors
///
/// Returns [`SmError`] if `name` is not an open column family.
pub(crate) fn open_cf<'a>(db: &'a DB, name: &str) -> Result<&'a ColumnFamily, SmError> {
    db.cf_handle(name).ok_or_else(|| missing_cf(name))
}

/// Loads every record from the column family and the restored id counter.
///
/// Records come out in key (id) order; the counter defaults to [`FIRST_ID`] when
/// no allocation has happened yet. The caller keys the records into its own
/// index by their embedded id.
///
/// # Errors
///
/// Returns [`SmError`] if the family is missing, iteration fails, the counter is
/// malformed, or a record cannot be decoded.
pub(crate) fn load_records<V: DeserializeOwned>(
    db: &DB,
    name: &str,
) -> Result<(Vec<V>, u32), SmError> {
    let cf = open_cf(db, name)?;
    let mut records = Vec::new();
    let mut next_id = FIRST_ID;
    for kv in db.iterator_cf(&cf, IteratorMode::Start) {
        let (key, value) = kv.map_err(sm_read_err)?;
        if key.as_ref() == NEXT_ID_KEY {
            let raw = <[u8; 4]>::try_from(value.as_ref())
                .map_err(|_| sm_corrupt("malformed id counter"))?;
            next_id = u32::from_be_bytes(raw);
        } else {
            records.push(serde_json::from_slice(&value).map_err(sm_read_err)?);
        }
    }
    Ok((records, next_id))
}

/// Returns the counter value after allocating `current`, failing on overflow.
///
/// # Errors
///
/// Returns [`SmError`] if the `u32` id space is exhausted (ids are never reused).
pub(crate) fn advance_counter(current: u32) -> Result<u32, SmError> {
    current
        .checked_add(1)
        .ok_or_else(|| sm_corrupt("id counter overflow"))
}

/// Stages the durable write of one serde record under its id key.
///
/// # Errors
///
/// Returns [`SmError`] if the record cannot be serialized.
pub(crate) fn stage_put_record<V: Serialize>(
    batch: &mut WriteBatch,
    cf: &ColumnFamily,
    id: u32,
    record: &V,
) -> Result<(), SmError> {
    let bytes = serde_json::to_vec(record).map_err(sm_write_err)?;
    batch.put_cf(cf, record_key(id), bytes);
    Ok(())
}

/// Stages the durable write of the id-allocation counter.
pub(crate) fn stage_put_counter(batch: &mut WriteBatch, cf: &ColumnFamily, next_id: u32) {
    batch.put_cf(cf, NEXT_ID_KEY, next_id.to_be_bytes());
}

/// Stages deletion of every key in the column family (records and counter),
/// used to fully replace the family on snapshot install.
///
/// # Errors
///
/// Returns [`SmError`] if iteration over the family fails.
pub(crate) fn stage_clear(
    batch: &mut WriteBatch,
    db: &DB,
    cf: &ColumnFamily,
) -> Result<(), SmError> {
    for kv in db.iterator_cf(cf, IteratorMode::Start) {
        let (key, _) = kv.map_err(sm_read_err)?;
        batch.delete_cf(cf, key);
    }
    Ok(())
}
