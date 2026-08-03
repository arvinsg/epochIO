//! The `Erasure` pure-computation type wrapping reed-solomon-simd.
//!
//! Fixed `(data, parity)` Reed–Solomon coding over equal-length, even-sized
//! shards. No I/O. Integrity is not checked here — corrupt shards must be
//! dropped (see [`crate::frame`]) before [`Erasure::reconstruct`].
//!
//! Design: docs/design/04-ec-io.md §2/§3

/// Errors from erasure configuration or coding.
#[derive(Debug, thiserror::Error)]
pub enum EcError {
    /// `data`/`parity` counts are out of the supported `1..=32768` range.
    #[error("invalid code mode: data={data}, parity={parity} (each must be 1..=32768)")]
    InvalidCodeMode {
        /// Requested data-shard count.
        data: usize,
        /// Requested parity-shard count.
        parity: usize,
    },
    /// The caller supplied the wrong number of shards.
    #[error("expected {expected} shards, got {got}")]
    ShardCount {
        /// Expected shard count.
        expected: usize,
        /// Supplied shard count.
        got: usize,
    },
    /// A shard length is zero or odd (reed-solomon-simd requires non-zero even).
    #[error("shard length {len} must be a non-zero even number")]
    ShardLen {
        /// The offending length.
        len: usize,
    },
    /// Present shards do not all share the same length.
    #[error("shards have inconsistent lengths")]
    InconsistentShardLen,
    /// Fewer than `data` shards are present, so reconstruction is impossible.
    #[error("too few shards to reconstruct: have {have}, need {need}")]
    TooFewShards {
        /// Number of shards present.
        have: usize,
        /// Number required (`data`).
        need: usize,
    },
    /// The underlying reed-solomon-simd backend reported an error.
    #[error("erasure backend error: {0}")]
    Backend(String),
}

const MAX_SHARDS: usize = 32768;

/// Reed–Solomon erasure coder for a fixed `(data, parity)` code mode.
///
/// Shards must be equal length and an even number of bytes; size them with
/// [`crate::layout::unit_size`].
#[derive(Debug, Clone, Copy)]
pub struct Erasure {
    data: usize,
    parity: usize,
}

impl Erasure {
    /// Creates a coder for `data` original and `parity` recovery shards.
    ///
    /// Both counts must be in `1..=32768` (Replica-3 is the `data=1, parity=2`
    /// case). Design: docs/design/04-ec-io.md §2.
    pub fn new(data: usize, parity: usize) -> Result<Self, EcError> {
        if data == 0 || parity == 0 || data > MAX_SHARDS || parity > MAX_SHARDS {
            return Err(EcError::InvalidCodeMode { data, parity });
        }
        Ok(Self { data, parity })
    }

    /// Number of data (original) shards.
    #[must_use]
    pub fn data_shards(&self) -> usize {
        self.data
    }
    /// Number of parity (recovery) shards.
    #[must_use]
    pub fn parity_shards(&self) -> usize {
        self.parity
    }
    /// Total shards in a stripe (`data + parity`).
    #[must_use]
    pub fn total_shards(&self) -> usize {
        self.data + self.parity
    }

    /// Encodes `data` original shards into `parity` recovery shards.
    ///
    /// All shards must be equal length and a non-zero even number of bytes.
    /// Returns the recovery shards in index order.
    pub fn encode(&self, data: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, EcError> {
        if data.len() != self.data {
            return Err(EcError::ShardCount {
                expected: self.data,
                got: data.len(),
            });
        }
        self.check_uniform_len(data.iter().map(Vec::as_slice))?;
        reed_solomon_simd::encode(self.data, self.parity, data)
            .map_err(|e| EcError::Backend(e.to_string()))
    }

    /// Reconstructs missing data shards in place.
    ///
    /// `shards` holds [`Self::total_shards`] slots: `Some` = present (already
    /// bitrot-verified), `None` = missing. On success every data slot
    /// (`0..data`) is `Some`. Fails if fewer than `data` shards are present.
    pub fn reconstruct(&self, shards: &mut [Option<Vec<u8>>]) -> Result<(), EcError> {
        if shards.len() != self.total_shards() {
            return Err(EcError::ShardCount {
                expected: self.total_shards(),
                got: shards.len(),
            });
        }
        let present = shards.iter().filter(|s| s.is_some()).count();
        if present < self.data {
            return Err(EcError::TooFewShards {
                have: present,
                need: self.data,
            });
        }
        if shards[..self.data].iter().all(Option::is_some) {
            return Ok(());
        }
        self.check_uniform_len(shards.iter().filter_map(|s| s.as_deref()))?;

        let restored = {
            let originals = shards[..self.data]
                .iter()
                .enumerate()
                .filter_map(|(i, s)| s.as_deref().map(|d| (i, d)));
            let recoveries = shards[self.data..]
                .iter()
                .enumerate()
                .filter_map(|(i, s)| s.as_deref().map(|d| (i, d)));
            reed_solomon_simd::decode(self.data, self.parity, originals, recoveries)
                .map_err(|e| EcError::Backend(e.to_string()))?
        };

        for (idx, shard) in restored {
            shards[idx] = Some(shard);
        }
        Ok(())
    }

    /// Validates that all shards share one non-zero, even length; returns it.
    fn check_uniform_len<'a>(
        &self,
        mut shards: impl Iterator<Item = &'a [u8]>,
    ) -> Result<usize, EcError> {
        let Some(first) = shards.next() else {
            return Err(EcError::TooFewShards {
                have: 0,
                need: self.data,
            });
        };
        let len = first.len();
        if len == 0 || len % 2 != 0 {
            return Err(EcError::ShardLen { len });
        }
        for shard in shards {
            if shard.len() != len {
                return Err(EcError::InconsistentShardLen);
            }
        }
        Ok(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shards(count: usize, len: usize) -> Vec<Vec<u8>> {
        (0..count).map(|i| vec![i as u8; len]).collect()
    }

    fn slots(data: &[Vec<u8>], parity: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        data.iter()
            .cloned()
            .map(Some)
            .chain(parity.iter().cloned().map(Some))
            .collect()
    }

    #[test]
    fn rejects_bad_code_mode() {
        assert!(Erasure::new(0, 4).is_err());
        assert!(Erasure::new(4, 0).is_err());
        assert!(Erasure::new(4, 4).is_ok());
    }

    #[test]
    fn encode_returns_parity_and_reconstruct_is_noop_when_complete() {
        let ec = Erasure::new(4, 2).unwrap();
        let data = shards(4, 64);
        let parity = ec.encode(&data).unwrap();
        assert_eq!(parity.len(), 2);

        let mut have = slots(&data, &parity);
        ec.reconstruct(&mut have).unwrap();
        for (i, slot) in have[..4].iter().enumerate() {
            assert_eq!(slot.as_deref().unwrap(), &data[i][..]);
        }
    }

    #[test]
    fn reconstructs_data_from_parity() {
        let ec = Erasure::new(4, 2).unwrap();
        let data = shards(4, 128);
        let parity = ec.encode(&data).unwrap();

        let mut have = slots(&data, &parity);
        have[0] = None; // drop 2 data shards (== parity count)
        have[2] = None;
        ec.reconstruct(&mut have).unwrap();
        assert_eq!(have[0].as_deref().unwrap(), &data[0][..]);
        assert_eq!(have[2].as_deref().unwrap(), &data[2][..]);
    }

    #[test]
    fn replica3_reconstructs_from_a_single_copy() {
        let ec = Erasure::new(1, 2).unwrap();
        let data = shards(1, 64);
        let parity = ec.encode(&data).unwrap();
        assert_eq!(parity.len(), 2);

        let mut have = vec![None, Some(parity[0].clone()), Some(parity[1].clone())];
        ec.reconstruct(&mut have).unwrap();
        assert_eq!(have[0].as_deref().unwrap(), &data[0][..]);
    }

    #[test]
    fn too_few_shards_fails() {
        let ec = Erasure::new(4, 2).unwrap();
        let data = shards(4, 64);
        let parity = ec.encode(&data).unwrap();

        let mut have = slots(&data, &parity);
        have[0] = None;
        have[1] = None;
        have[4] = None; // 3 present < 4 data
        assert!(matches!(
            ec.reconstruct(&mut have),
            Err(EcError::TooFewShards { have: 3, need: 4 })
        ));
    }

    #[test]
    fn rejects_odd_or_inconsistent_lengths() {
        let ec = Erasure::new(2, 1).unwrap();
        assert!(matches!(
            ec.encode(&[vec![0u8; 3], vec![0u8; 3]]),
            Err(EcError::ShardLen { len: 3 })
        ));
        assert!(matches!(
            ec.encode(&[vec![0u8; 4], vec![0u8; 6]]),
            Err(EcError::InconsistentShardLen)
        ));
    }
}
