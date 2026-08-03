//! The M3 object-coding metadata: how an object is erasure-coded ([`CodeMode`]),
//! where its shards live ([`ChunkPlacement`]), and the per-blob directory a GET
//! replays ([`ObjectLayout`]).
//!
//! This stands in for the MetaNode `ObjectMeta` that M5 will persist: a PUT
//! returns an [`ObjectLayout`] and a GET consumes it, with no MetaNode in the
//! loop. Design: docs/design/04-ec-io.md §3.1; docs/design/06-code-layout.md §10.

use epoch_ec::Erasure;
use epoch_proto::{BlobId, ChunkId, NodeId, ShardId};

use crate::error::GatewayError;

/// Erasure code parameters for an object: `data + parity` shards, a stripe
/// (coding unit) size, and the blob (object cut) size.
///
/// Design: docs/design/04-ec-io.md §1.2/§2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodeMode {
    /// Data shards per stripe (`N`).
    pub data: usize,
    /// Parity shards per stripe (`M`).
    pub parity: usize,
    /// Stripe size in bytes (the EC coding unit; 1 MiB by default).
    pub stripe_size: usize,
    /// Blob size in bytes (the object-cut unit; 32 MiB by default).
    pub blob_size: usize,
}

impl CodeMode {
    /// Builds and validates a code mode.
    ///
    /// # Errors
    ///
    /// - [`EcError::InvalidCodeMode`](epoch_ec::EcError::InvalidCodeMode) if the
    ///   data/parity counts are out of range;
    /// - [`GatewayError::InvalidSizing`] if `stripe_size == 0` or
    ///   `blob_size < stripe_size`.
    pub fn new(
        data: usize,
        parity: usize,
        stripe_size: usize,
        blob_size: usize,
    ) -> Result<Self, GatewayError> {
        // Validate data/parity through the erasure coder's own range check.
        Erasure::new(data, parity)?;
        if stripe_size == 0 || blob_size < stripe_size {
            return Err(GatewayError::InvalidSizing {
                stripe_size,
                blob_size,
            });
        }
        Ok(Self {
            data,
            parity,
            stripe_size,
            blob_size,
        })
    }

    /// Total shards per stripe (`data + parity`).
    #[must_use]
    pub fn total(&self) -> usize {
        self.data + self.parity
    }

    /// The erasure coder for this mode.
    ///
    /// # Errors
    ///
    /// [`EcError::InvalidCodeMode`](epoch_ec::EcError::InvalidCodeMode) if the
    /// counts are out of range (already excluded by [`CodeMode::new`]).
    pub fn erasure(&self) -> Result<Erasure, GatewayError> {
        Ok(Erasure::new(self.data, self.parity)?)
    }

    /// Shards that must commit for a durable write: `data + parity − t` with
    /// `t = max(1, parity / 2)`, so `t` failures are tolerated while `parity − t`
    /// slack remains for later repair. Design: docs/design/99-open-questions.md
    /// Q5 (v0.13).
    #[must_use]
    pub fn write_quorum(&self) -> usize {
        let tolerate = (self.parity / 2).max(1);
        self.total() - tolerate
    }
}

/// The shard slots of one chunk and the node each lives on. `shards[i]` is the
/// slot for EC shard index `i` (`0..data+parity`): data shards first, then
/// parity. Design: docs/design/01-pd.md §3; docs/design/04-ec-io.md §3.1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkPlacement {
    /// The chunk these shards belong to.
    pub chunk_id: ChunkId,
    /// `(shard slot, hosting node)` in shard-index order.
    pub shards: Vec<(ShardId, NodeId)>,
}

/// One blob of an object: its writer-constructed id, logical byte length, and
/// the chunk its shards were written to (per-blob placement, 02 §2.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobDesc {
    /// The blob's id (`writer_token | seq`).
    pub blob_id: BlobId,
    /// The blob's logical length in bytes (before EC padding).
    pub len: usize,
    /// The chunk (and its shard endpoints) the blob was written to.
    pub chunk: ChunkPlacement,
}

/// The full read-back directory for one object: everything a GET needs without
/// a MetaNode. Returned by a PUT, consumed by a GET.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectLayout {
    /// Total object length in bytes.
    pub size: u64,
    /// How the object is erasure-coded.
    pub code: CodeMode,
    /// The object's blobs, in order (each with its own placement).
    pub blobs: Vec<BlobDesc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_quorum_tolerates_derived_failures() {
        // (data, parity) -> (total, expected quorum, tolerated failures).
        for (data, parity, quorum) in [
            (2usize, 1usize, 2usize), // EC2+1: t=1
            (1, 2, 2),                // replica-3: t=1
            (4, 2, 5),                // EC4+2: t=1
            (12, 4, 14),              // EC12+4: t=2 (Q5 example)
            (10, 4, 12),              // t=2
        ] {
            let code = CodeMode::new(data, parity, 1024, 1024).unwrap();
            assert_eq!(code.total(), data + parity);
            assert_eq!(code.write_quorum(), quorum, "quorum for EC{data}+{parity}");
            assert!(code.write_quorum() >= data, "quorum must be >= data");
        }
    }

    #[test]
    fn new_rejects_bad_counts_and_sizing() {
        assert!(matches!(
            CodeMode::new(0, 2, 1024, 1024),
            Err(GatewayError::Ec(_))
        ));
        assert!(matches!(
            CodeMode::new(2, 1, 0, 1024),
            Err(GatewayError::InvalidSizing { .. })
        ));
        assert!(matches!(
            CodeMode::new(2, 1, 2048, 1024),
            Err(GatewayError::InvalidSizing {
                stripe_size: 2048,
                blob_size: 1024
            })
        ));
        assert!(CodeMode::new(2, 1, 1024, 1024).is_ok());
    }
}
