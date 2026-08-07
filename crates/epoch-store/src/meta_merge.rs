// Copyright 2026 arvinsg
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Field-scoped merge operands for [`ExtentMeta`](crate::index::ExtentMeta).
//!
//! `ExtentMeta`'s three mutable fields have three independent owners running
//! concurrently: the per-extent writer thread advances `size` at commit, the
//! delete path adds to `deleted_bytes`, and seal/compaction transition
//! `status`. Whole-value read-modify-write from any of them can interleave and
//! roll back another owner's update — the worst case being a stale `size`
//! that recovery then truncates to, destroying committed blobs (02 §1.7).
//!
//! The fix is structural: each owner writes only its own field as a RocksDB
//! merge operand, and the merge operator (installed by
//! [`DiskIndex::open`](crate::index::DiskIndex::open)) folds operands into the
//! base value inside RocksDB, where application is totally ordered per key.
//!
//! INVARIANT(design 02 §1.6/§1.7): no caller may `put` a whole `ExtentMeta`
//! over an existing key to change one field — full-value puts are reserved for
//! creation and recovery installs, where the caller owns the entire value.
//!
//! The operand language is associative (merge(a, merge(b, c)) == merge(merge(a,
//! b), c)): `SetSize`/`SetStatus` are last-writer-wins and `AddDeletedBytes`
//! is commutative addition — so RocksDB partial merges are safe.
//!
//! Design: docs/design/02-datanode.md §1.6/§1.7

use rocksdb::MergeOperands;

use crate::codec::read_array;
use crate::extent::state::ExtentStatus;
use crate::index::ExtentMeta;

/// Operand tag bytes (persisted in the WAL/SST; never renumber, AGENTS §7.1).
const TAG_SET_SIZE: u8 = 1;
const TAG_ADD_DELETED: u8 = 2;
const TAG_SET_STATUS: u8 = 3;

/// A single field mutation applied to an `ExtentMeta` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaMergeOp {
    /// Advance the append cursor / valid length (writer commit, 02 §1.7).
    SetSize(u64),
    /// Account a tombstoned record's on-disk bytes (delete path, 02 §1.6).
    AddDeletedBytes(u64),
    /// Transition the lifecycle status (seal / compaction rebind, 02 §1.5).
    SetStatus(ExtentStatus),
}

impl MetaMergeOp {
    /// Encodes to the fixed 9-byte operand form (`tag | u64 LE`).
    #[must_use]
    pub fn encode(&self) -> [u8; 9] {
        let mut buf = [0u8; 9];
        match self {
            MetaMergeOp::SetSize(size) => {
                buf[0] = TAG_SET_SIZE;
                buf[1..9].copy_from_slice(&size.to_le_bytes());
            }
            MetaMergeOp::AddDeletedBytes(bytes) => {
                buf[0] = TAG_ADD_DELETED;
                buf[1..9].copy_from_slice(&bytes.to_le_bytes());
            }
            MetaMergeOp::SetStatus(status) => {
                buf[0] = TAG_SET_STATUS;
                buf[1] = status.to_u8();
            }
        }
        buf
    }

    /// Decodes an operand, or `None` for a malformed/unknown one (the merge
    /// operator skips it rather than poisoning the value — see [`merge_meta`]).
    #[must_use]
    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() != 9 {
            return None;
        }
        match buf[0] {
            TAG_SET_SIZE => Some(MetaMergeOp::SetSize(u64::from_le_bytes(read_array::<8>(
                buf, 1,
            )))),
            TAG_ADD_DELETED => Some(MetaMergeOp::AddDeletedBytes(u64::from_le_bytes(
                read_array::<8>(buf, 1),
            ))),
            TAG_SET_STATUS => ExtentStatus::from_u8(buf[1]).map(MetaMergeOp::SetStatus),
            _ => None,
        }
    }

    /// Applies this operand to a meta value in place.
    fn apply(self, meta: &mut ExtentMeta) {
        match self {
            MetaMergeOp::SetSize(size) => meta.size = size,
            MetaMergeOp::AddDeletedBytes(bytes) => {
                meta.deleted_bytes = meta.deleted_bytes.saturating_add(bytes);
            }
            MetaMergeOp::SetStatus(status) => meta.status = status,
        }
    }
}

/// The RocksDB merge function for extent-meta keys (both full and partial
/// merge — the operand language is associative, see the module docs).
///
/// Defensive rules:
/// - a missing base (merge before any put — a caller bug or a lost put) keeps
///   the operands pending by re-emitting them concatenated, so a later put
///   still sees them applied; if this is a full merge (no base will ever
///   arrive), the key stays undecodable and reads report `Corrupt` rather
///   than fabricating a default meta;
/// - an undecodable operand is skipped (logged by the read path when the value
///   itself is corrupt); the remaining operands still apply.
///
/// Non-meta keys (`s`/`b` keyspaces) never receive merges — the owner only
/// issues `merge` on `e` keys.
pub fn merge_meta(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let Some(base) = existing else {
        // No base yet: keep operands pending as a concatenated stack so they
        // survive partial merges and apply once the base put lands.
        let mut pending = Vec::with_capacity(operands.len() * 9);
        for op in operands {
            pending.extend_from_slice(op);
        }
        return Some(pending);
    };

    // The base may itself be a pending-operand stack (9-byte multiples that
    // decode as operands) if merges arrived before any put; keep accumulating
    // in that case, since only a full put supplies the missing base value.
    let mut meta = match ExtentMeta::decode_value(base) {
        Ok(meta) => meta,
        Err(_) => {
            if decode_operand_stack(base).is_none() {
                // Truly corrupt base: preserve it untouched for forensic reads.
                return Some(base.to_vec());
            }
            let mut pending: Vec<u8> = base.to_vec();
            for op in operands {
                pending.extend_from_slice(op);
            }
            return Some(pending);
        }
    };
    for raw in operands {
        if let Some(op) = MetaMergeOp::decode(raw) {
            op.apply(&mut meta);
        }
    }
    Some(meta.encode().to_vec())
}

/// Decodes a concatenation of 9-byte operands, or `None` if the bytes are not
/// a well-formed operand stack.
fn decode_operand_stack(buf: &[u8]) -> Option<Vec<MetaMergeOp>> {
    if buf.is_empty() || !buf.len().is_multiple_of(9) {
        return None;
    }
    buf.chunks_exact(9).map(MetaMergeOp::decode).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use epoch_proto::ShardId;

    fn base_meta() -> ExtentMeta {
        ExtentMeta {
            shard_id: ShardId::from_raw(7),
            status: ExtentStatus::Writable,
            size: 4096,
            deleted_bytes: 0,
            create_ts: 42,
        }
    }

    #[test]
    fn operand_encoding_round_trips() {
        for op in [
            MetaMergeOp::SetSize(0),
            MetaMergeOp::SetSize(u64::MAX),
            MetaMergeOp::AddDeletedBytes(1),
            MetaMergeOp::SetStatus(ExtentStatus::Sealed),
            MetaMergeOp::SetStatus(ExtentStatus::Dropped),
        ] {
            assert_eq!(MetaMergeOp::decode(&op.encode()), Some(op));
        }
        assert_eq!(MetaMergeOp::decode(&[9u8; 9]), None, "unknown tag");
        assert_eq!(MetaMergeOp::decode(&[1u8; 4]), None, "short operand");
    }

    #[test]
    fn apply_updates_only_the_owned_field() {
        let mut meta = base_meta();
        MetaMergeOp::SetSize(8192).apply(&mut meta);
        MetaMergeOp::AddDeletedBytes(100).apply(&mut meta);
        MetaMergeOp::AddDeletedBytes(50).apply(&mut meta);
        MetaMergeOp::SetStatus(ExtentStatus::Sealed).apply(&mut meta);

        let expect = ExtentMeta {
            size: 8192,
            deleted_bytes: 150,
            status: ExtentStatus::Sealed,
            ..base_meta()
        };
        assert_eq!(meta, expect);
        // Untouched fields survive.
        assert_eq!(meta.shard_id, base_meta().shard_id);
        assert_eq!(meta.create_ts, base_meta().create_ts);
    }

    #[test]
    fn operand_application_is_associative() {
        // Folding [a, b, c] in one pass equals folding [a] then [b, c] — the
        // property RocksDB partial merges rely on.
        let ops = [
            MetaMergeOp::SetSize(9000),
            MetaMergeOp::AddDeletedBytes(11),
            MetaMergeOp::SetStatus(ExtentStatus::Full),
            MetaMergeOp::AddDeletedBytes(22),
            MetaMergeOp::SetSize(9500),
        ];
        let mut all_at_once = base_meta();
        for op in ops {
            op.apply(&mut all_at_once);
        }
        for split in 1..ops.len() {
            let mut staged = base_meta();
            for op in &ops[..split] {
                op.apply(&mut staged);
            }
            for op in &ops[split..] {
                op.apply(&mut staged);
            }
            assert_eq!(staged, all_at_once, "split at {split}");
        }
    }

    #[test]
    fn pending_stack_round_trips_through_decode() {
        let ops = [
            MetaMergeOp::AddDeletedBytes(5),
            MetaMergeOp::SetStatus(ExtentStatus::Sealed),
        ];
        let mut stack = Vec::new();
        for op in ops {
            stack.extend_from_slice(&op.encode());
        }
        assert_eq!(decode_operand_stack(&stack), Some(ops.to_vec()));
        assert_eq!(decode_operand_stack(&stack[..10]), None, "ragged stack");
        assert_eq!(decode_operand_stack(&[]), None, "empty");
    }
}
