//! Erasure-code parameters shared across components: how many data / parity
//! shards a chunk uses and the stripe / blob cut sizes.
//!
//! [`CodeModeId`] is the stable registry key PD assigns a configured code mode;
//! [`CodeMode`] carries the resolved parameters PD stores on a chunk and
//! publishes to gateways so they can encode without a second lookup. The config
//! registry that mints ids lands with the PD config manager (a later
//! milestone); until then a fixed mode is injected.
//!
//! Design: docs/design/01-pd.md §3 (Chunk model); docs/design/04-ec-io.md §1.2/§2

/// Stable identity of a configured erasure-code mode (the PD registry key).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct CodeModeId(u16);

impl CodeModeId {
    /// Wraps a raw `u16`.
    pub const fn new(raw: u16) -> Self {
        Self(raw)
    }
    /// Returns the raw `u16`.
    pub const fn get(self) -> u16 {
        self.0
    }
}

impl From<u16> for CodeModeId {
    fn from(raw: u16) -> Self {
        Self(raw)
    }
}

/// Resolved erasure-code parameters for a chunk: `data + parity` shards per
/// stripe, plus the stripe (coding unit) and blob (object cut) sizes.
///
/// Shard counts are `u8`: an EC stripe has at most a few dozen shards, well
/// within both the 8-bit `ShardId` index field and a byte. This is a plain data
/// carrier — the valid data/parity range is enforced by the erasure coder at the
/// boundary that defines a mode (gateway / config), not here in the L0 vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CodeMode {
    /// Registry identity (stable across the cluster).
    pub id: CodeModeId,
    /// Data shards per stripe (`N`).
    pub data: u8,
    /// Parity shards per stripe (`M`).
    pub parity: u8,
    /// Stripe size in bytes (the EC coding unit).
    pub stripe_size: u32,
    /// Blob size in bytes (the object-cut unit).
    pub blob_size: u64,
}

impl CodeMode {
    /// Total shards per stripe (`data + parity`) — the number of shard slots a
    /// chunk of this mode holds.
    #[must_use]
    pub fn shards_total(self) -> u16 {
        u16::from(self.data) + u16::from(self.parity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shards_total_sums_data_and_parity() {
        let mode = CodeMode {
            id: CodeModeId::new(1),
            data: 12,
            parity: 4,
            stripe_size: 1 << 20,
            blob_size: 32 << 20,
        };
        assert_eq!(mode.shards_total(), 16);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn code_mode_serde_round_trip() {
        let mode = CodeMode {
            id: CodeModeId::new(7),
            data: 4,
            parity: 2,
            stripe_size: 1 << 20,
            blob_size: 32 << 20,
        };
        let json = serde_json::to_string(&mode).expect("serialize");
        let back: CodeMode = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(mode, back);
    }
}
