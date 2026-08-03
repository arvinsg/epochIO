//! `.superblock`: fixed-length disk identity and layout descriptor.
//!
//! Each copy is a 4 KiB block: a fixed field region followed by a CRC32C over
//! those fields, then zero padding. Scalar fields are little-endian — these are
//! not ordering keys (only index keys need big-endian, see `epoch-proto`).
//! Crash-safe update (two copies, tail-before-head) is the caller's job
//! ([`crate::disk`]); this module only encodes/decodes one copy.
//!
//! Design: docs/design/02-datanode.md §1.1

use epoch_proto::DiskId;

use crate::codec::read_array;

/// On-disk size of one superblock copy: 4 KiB, fixed.
pub const SUPERBLOCK_SIZE: usize = 4096;

/// Current superblock layout version (bumped when the field region changes).
pub const SUPERBLOCK_VERSION: u16 = 1;

/// 8-byte magic identifying an epochIO superblock.
pub const SUPERBLOCK_MAGIC: [u8; 8] = *b"EPOCHIO\0";

// Little-endian field offsets within one copy.
const OFF_MAGIC: usize = 0; //        [0..8)   magic
const OFF_VERSION: usize = 8; //      [8..10)  version   u16
const OFF_DISK_ID: usize = 10; //     [10..14) disk_id   u32
const OFF_CLUSTER_ID: usize = 14; //  [14..30) cluster   u128
const OFF_CREATED_AT: usize = 30; //  [30..38) created   i64
const OFF_FLAGS: usize = 38; //       [38..42) flags     u32
const OFF_EXTENT_SIZE: usize = 42; // [42..50) ext_size  u64
const OFF_CHECKSUM: usize = 50; //    [50..54) crc32c    u32
/// Bytes covered by the checksum: the whole field region before it.
const CHECKSUM_COVERAGE: usize = OFF_CHECKSUM;

// The field region plus checksum must fit inside one copy.
const _: () = assert!(OFF_CHECKSUM + 4 <= SUPERBLOCK_SIZE);

/// A decode failure for a superblock copy.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SuperblockError {
    /// The buffer is shorter than [`SUPERBLOCK_SIZE`].
    #[error("superblock buffer too short: {0} bytes")]
    Truncated(usize),
    /// The 8-byte magic did not match [`SUPERBLOCK_MAGIC`].
    #[error("superblock magic mismatch")]
    BadMagic,
    /// The stored layout version is not supported by this build.
    #[error("unsupported superblock version {found} (expected {expected})")]
    Version {
        /// Version read from the copy.
        found: u16,
        /// Version this build writes and accepts.
        expected: u16,
    },
    /// The stored CRC32C did not match the recomputed value (torn/rotted copy).
    #[error("superblock checksum mismatch (stored {stored:#010x}, computed {computed:#010x})")]
    Checksum {
        /// CRC read from the copy.
        stored: u32,
        /// CRC recomputed over the field region.
        computed: u32,
    },
}

/// Disk identity and layout descriptor persisted in the `.superblock` file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Superblock {
    /// Disk id assigned by PD at registration.
    pub disk_id: DiskId,
    /// Owning cluster id — guards against plugging a disk into the wrong cluster.
    pub cluster_id: u128,
    /// Creation timestamp (unix nanos or seconds; caller's convention).
    pub created_at: i64,
    /// Reserved flag bits.
    pub flags: u32,
    /// Extent container size for this disk.
    pub extent_size: u64,
}

impl Superblock {
    /// Encodes this superblock into one fixed 4 KiB copy (field region, CRC32C,
    /// then zero padding).
    #[must_use]
    pub fn encode(&self) -> [u8; SUPERBLOCK_SIZE] {
        let mut buf = [0u8; SUPERBLOCK_SIZE];
        buf[OFF_MAGIC..OFF_VERSION].copy_from_slice(&SUPERBLOCK_MAGIC);
        buf[OFF_VERSION..OFF_DISK_ID].copy_from_slice(&SUPERBLOCK_VERSION.to_le_bytes());
        buf[OFF_DISK_ID..OFF_CLUSTER_ID].copy_from_slice(&self.disk_id.get().to_le_bytes());
        buf[OFF_CLUSTER_ID..OFF_CREATED_AT].copy_from_slice(&self.cluster_id.to_le_bytes());
        buf[OFF_CREATED_AT..OFF_FLAGS].copy_from_slice(&self.created_at.to_le_bytes());
        buf[OFF_FLAGS..OFF_EXTENT_SIZE].copy_from_slice(&self.flags.to_le_bytes());
        buf[OFF_EXTENT_SIZE..OFF_CHECKSUM].copy_from_slice(&self.extent_size.to_le_bytes());
        let checksum = crc32c::crc32c(&buf[..CHECKSUM_COVERAGE]);
        buf[OFF_CHECKSUM..OFF_CHECKSUM + 4].copy_from_slice(&checksum.to_le_bytes());
        buf
    }

    /// Decodes and validates one superblock copy (magic, version, CRC32C).
    ///
    /// # Errors
    ///
    /// Returns [`SuperblockError`] if `buf` is too short, the magic or version
    /// is wrong, or the checksum does not match the field region.
    pub fn decode(buf: &[u8]) -> Result<Self, SuperblockError> {
        let Some(buf) = buf.first_chunk::<SUPERBLOCK_SIZE>() else {
            return Err(SuperblockError::Truncated(buf.len()));
        };
        if read_array::<8>(buf, OFF_MAGIC) != SUPERBLOCK_MAGIC {
            return Err(SuperblockError::BadMagic);
        }
        let found = u16::from_le_bytes(read_array::<2>(buf, OFF_VERSION));
        if found != SUPERBLOCK_VERSION {
            return Err(SuperblockError::Version {
                found,
                expected: SUPERBLOCK_VERSION,
            });
        }
        let stored = u32::from_le_bytes(read_array::<4>(buf, OFF_CHECKSUM));
        let computed = crc32c::crc32c(&buf[..CHECKSUM_COVERAGE]);
        if stored != computed {
            return Err(SuperblockError::Checksum { stored, computed });
        }
        Ok(Self {
            disk_id: DiskId::new(u32::from_le_bytes(read_array::<4>(buf, OFF_DISK_ID))),
            cluster_id: u128::from_le_bytes(read_array::<16>(buf, OFF_CLUSTER_ID)),
            created_at: i64::from_le_bytes(read_array::<8>(buf, OFF_CREATED_AT)),
            flags: u32::from_le_bytes(read_array::<4>(buf, OFF_FLAGS)),
            extent_size: u64::from_le_bytes(read_array::<8>(buf, OFF_EXTENT_SIZE)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Superblock {
        Superblock {
            disk_id: DiskId::new(0xABCD_1234),
            cluster_id: 0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10,
            created_at: 1_700_000_000,
            flags: 0,
            extent_size: epoch_proto::consts::DEFAULT_EXTENT_SIZE,
        }
    }

    #[test]
    fn encoded_copy_is_exactly_one_block() {
        assert_eq!(sample().encode().len(), SUPERBLOCK_SIZE);
    }

    #[test]
    fn round_trip_preserves_all_fields() {
        for sb in [
            sample(),
            Superblock {
                disk_id: DiskId::new(0),
                cluster_id: 0,
                created_at: i64::MIN,
                flags: u32::MAX,
                extent_size: 0,
            },
            Superblock {
                disk_id: DiskId::new(u32::MAX),
                cluster_id: u128::MAX,
                created_at: i64::MAX,
                flags: u32::MAX,
                extent_size: u64::MAX,
            },
        ] {
            assert_eq!(Superblock::decode(&sb.encode()), Ok(sb));
        }
    }

    #[test]
    fn decode_accepts_trailing_bytes_beyond_one_block() {
        let mut buf = sample().encode().to_vec();
        buf.extend_from_slice(&[0xFFu8; 4096]); // second copy region, ignored here
        assert_eq!(Superblock::decode(&buf), Ok(sample()));
    }

    #[test]
    fn short_buffer_is_truncated() {
        assert_eq!(
            Superblock::decode(&[0u8; SUPERBLOCK_SIZE - 1]),
            Err(SuperblockError::Truncated(SUPERBLOCK_SIZE - 1))
        );
    }

    #[test]
    fn bad_magic_is_rejected() {
        let mut buf = sample().encode();
        buf[0] ^= 0xFF;
        assert_eq!(Superblock::decode(&buf), Err(SuperblockError::BadMagic));
    }

    #[test]
    fn wrong_version_is_rejected() {
        let mut buf = sample().encode();
        // Bump the version field, then repair the checksum so version is the
        // only failing check.
        buf[OFF_VERSION..OFF_DISK_ID].copy_from_slice(&(SUPERBLOCK_VERSION + 1).to_le_bytes());
        let fixed = crc32c::crc32c(&buf[..CHECKSUM_COVERAGE]);
        buf[OFF_CHECKSUM..OFF_CHECKSUM + 4].copy_from_slice(&fixed.to_le_bytes());
        assert_eq!(
            Superblock::decode(&buf),
            Err(SuperblockError::Version {
                found: SUPERBLOCK_VERSION + 1,
                expected: SUPERBLOCK_VERSION,
            })
        );
    }

    #[test]
    fn corrupted_field_fails_checksum() {
        let mut buf = sample().encode();
        buf[OFF_EXTENT_SIZE] ^= 0xFF; // flip a covered byte
        assert!(matches!(
            Superblock::decode(&buf),
            Err(SuperblockError::Checksum { .. })
        ));
    }
}
