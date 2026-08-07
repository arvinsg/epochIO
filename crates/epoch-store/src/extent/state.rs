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

//! Extent lifecycle status.
//!
//! The status set is `Rebuilding → Writable ⇄ Full → Sealed → Dropped` (02 §1.5).
//! M2 only needs the enum and its stable on-disk encoding (persisted inside
//! `ExtentMeta`, see [`crate::index`]); the transition state machine (compaction
//! rebind, repair) lands with those features in a later milestone.
//!
//! Design: docs/design/02-datanode.md §1.5

/// Lifecycle status of an extent (persisted as a single byte in the index).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtentStatus {
    /// Receiving repair/migration data only; not serving foreground writes.
    Rebuilding,
    /// Accepting appends.
    Writable,
    /// Full; paused for writes until compaction reclaims space.
    Full,
    /// Sealed: rejects writes (including OPEN); reads unaffected.
    Sealed,
    /// Unbound and awaiting physical deletion.
    Dropped,
}

impl ExtentStatus {
    /// Stable on-disk byte encoding. These values are persisted — never renumber
    /// (AGENTS §7.1); append new variants with new values instead.
    #[must_use]
    pub const fn to_u8(self) -> u8 {
        match self {
            ExtentStatus::Rebuilding => 0,
            ExtentStatus::Writable => 1,
            ExtentStatus::Full => 2,
            ExtentStatus::Sealed => 3,
            ExtentStatus::Dropped => 4,
        }
    }

    /// Decodes a status byte, or `None` for an unknown value.
    #[must_use]
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(ExtentStatus::Rebuilding),
            1 => Some(ExtentStatus::Writable),
            2 => Some(ExtentStatus::Full),
            3 => Some(ExtentStatus::Sealed),
            4 => Some(ExtentStatus::Dropped),
            _ => None,
        }
    }

    /// Whether the extent currently accepts new blob appends (only `Writable`).
    #[must_use]
    pub const fn is_writable(self) -> bool {
        matches!(self, ExtentStatus::Writable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u8_encoding_round_trips_every_variant() {
        for status in [
            ExtentStatus::Rebuilding,
            ExtentStatus::Writable,
            ExtentStatus::Full,
            ExtentStatus::Sealed,
            ExtentStatus::Dropped,
        ] {
            assert_eq!(ExtentStatus::from_u8(status.to_u8()), Some(status));
        }
    }

    #[test]
    fn unknown_status_byte_is_rejected() {
        assert_eq!(ExtentStatus::from_u8(5), None);
        assert_eq!(ExtentStatus::from_u8(u8::MAX), None);
    }

    #[test]
    fn only_writable_accepts_appends() {
        assert!(ExtentStatus::Writable.is_writable());
        for status in [
            ExtentStatus::Rebuilding,
            ExtentStatus::Full,
            ExtentStatus::Sealed,
            ExtentStatus::Dropped,
        ] {
            assert!(!status.is_writable());
        }
    }
}
