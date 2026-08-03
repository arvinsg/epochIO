//! Extent files: a 4 KiB self-describing header followed by 4 KiB-aligned,
//! append-only blob records (02 §1.2). Provides record append, positioned
//! record read, and the tail-truncation scan used for crash recovery (02 §1.7).
//!
//! ```text
//! extent file = header(4096B) + [ record ]*   (each record 4 KiB-aligned)
//!   header: magic(8B) | version(u16) | extent_id(16B) | crc32c(4B) | pad
//! ```
//!
//! `extent_id` embeds `shard_id` and `create_ts` (see `epoch-proto`), so the
//! header is fully self-describing without duplicating those fields. Positioned
//! reads/writes (Unix `FileExt`) keep the append cursor independent of any file
//! seek position.
//!
//! Design: docs/design/02-datanode.md §1.2/§1.7

use std::fs::OpenOptions;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

use epoch_proto::{BlobId, ExtentId, ShardId};

use super::record::{self, RECORD_FOOTER_LEN, RECORD_HEADER_LEN, RecordError, RecordHeader};
use crate::codec::read_array;

/// Fixed extent-header length: 4 KiB (records start immediately after).
pub const EXTENT_HEADER_LEN: usize = 4096;
/// Current extent-header layout version.
pub const EXTENT_VERSION: u16 = 1;

const EXTENT_MAGIC: [u8; 8] = *b"EPEXTENT";

// Header field offsets (little-endian).
const EH_MAGIC: usize = 0; //      [0..8)
const EH_VERSION: usize = 8; //    [8..10)
const EH_EXTENT_ID: usize = 10; // [10..26)
const EH_CRC: usize = 26; //       [26..30) crc32c over [0..26)
const EH_CRC_COVERAGE: usize = EH_CRC;

const _: () = assert!(EH_CRC + 4 <= EXTENT_HEADER_LEN);

/// An extent-file failure (I/O or format).
#[derive(Debug, thiserror::Error)]
pub enum ExtentError {
    /// An underlying filesystem I/O error.
    #[error("extent I/O error: {0}")]
    Io(#[from] io::Error),
    /// The extent header magic did not match.
    #[error("extent header magic mismatch")]
    BadHeaderMagic,
    /// The stored header layout version is unsupported.
    #[error("unsupported extent version {found} (expected {expected})")]
    Version {
        /// Version read from the header.
        found: u16,
        /// Version this build accepts.
        expected: u16,
    },
    /// The extent header CRC32C did not match.
    #[error("extent header checksum mismatch (stored {stored:#010x}, computed {computed:#010x})")]
    HeaderChecksum {
        /// CRC read from the header.
        stored: u32,
        /// CRC recomputed over the header field region.
        computed: u32,
    },
    /// A record header/footer failed to decode.
    #[error("record at offset {offset}: {source}")]
    Record {
        /// File offset of the record start.
        offset: u64,
        /// Underlying record decode error.
        #[source]
        source: RecordError,
    },
    /// A record body's CRC32C did not match its footer.
    #[error("record body checksum mismatch at offset {offset}")]
    BodyChecksum {
        /// File offset of the record start.
        offset: u64,
    },
}

/// Self-describing extent-file header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtentHeader {
    /// Identity of this extent (embeds shard slot and creation timestamp).
    pub extent_id: ExtentId,
}

impl ExtentHeader {
    /// Encodes the header into its fixed 4 KiB on-disk form.
    #[must_use]
    pub fn encode(&self) -> [u8; EXTENT_HEADER_LEN] {
        let mut buf = [0u8; EXTENT_HEADER_LEN];
        buf[EH_MAGIC..EH_VERSION].copy_from_slice(&EXTENT_MAGIC);
        buf[EH_VERSION..EH_EXTENT_ID].copy_from_slice(&EXTENT_VERSION.to_le_bytes());
        buf[EH_EXTENT_ID..EH_CRC].copy_from_slice(self.extent_id.as_bytes());
        let crc = crc32c::crc32c(&buf[..EH_CRC_COVERAGE]);
        buf[EH_CRC..EH_CRC + 4].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decodes and validates an extent header (magic, version, CRC32C).
    ///
    /// # Errors
    ///
    /// [`ExtentError::BadHeaderMagic`], [`ExtentError::Version`] or
    /// [`ExtentError::HeaderChecksum`] on the respective validation failure.
    pub fn decode(buf: &[u8; EXTENT_HEADER_LEN]) -> Result<Self, ExtentError> {
        if read_array::<8>(buf, EH_MAGIC) != EXTENT_MAGIC {
            return Err(ExtentError::BadHeaderMagic);
        }
        let found = u16::from_le_bytes(read_array::<2>(buf, EH_VERSION));
        if found != EXTENT_VERSION {
            return Err(ExtentError::Version {
                found,
                expected: EXTENT_VERSION,
            });
        }
        let stored = u32::from_le_bytes(read_array::<4>(buf, EH_CRC));
        let computed = crc32c::crc32c(&buf[..EH_CRC_COVERAGE]);
        if stored != computed {
            return Err(ExtentError::HeaderChecksum { stored, computed });
        }
        Ok(Self {
            extent_id: ExtentId::from_bytes(read_array::<16>(buf, EH_EXTENT_ID)),
        })
    }
}

/// One record found intact by a recovery [`scan`](ExtentFile::scan).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveredRecord {
    /// Blob this record stores a shard of.
    pub blob_id: BlobId,
    /// Shard slot the record belongs to.
    pub shard_id: ShardId,
    /// File offset where the record starts.
    pub offset: u64,
    /// Body length recorded in the header.
    pub body_len: u32,
    /// Body CRC32C read from the footer, reused when rebuilding the index
    /// entry so recovery never re-hashes the body.
    pub crc: u32,
}

/// Result of a tail-truncation recovery scan (02 §1.7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recovery {
    /// Intact records in append order.
    pub records: Vec<RecoveredRecord>,
    /// Byte offset just past the last intact record: the valid file length.
    pub valid_end: u64,
    /// Whether a torn/garbage tail was found past `valid_end`.
    pub truncated: bool,
}

/// An open extent file: its validated header and current append cursor.
#[derive(Debug)]
pub struct ExtentFile {
    file: std::fs::File,
    extent_id: ExtentId,
    write_offset: u64,
}

impl ExtentFile {
    /// Creates a new extent file at `path`, writing and fsync-ing its header.
    /// Fails if the file already exists.
    ///
    /// # Errors
    ///
    /// [`ExtentError::Io`] if the file exists or the header cannot be written.
    pub fn create(path: impl AsRef<Path>, extent_id: ExtentId) -> Result<Self, ExtentError> {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        let header = ExtentHeader { extent_id }.encode();
        file.write_all_at(&header, 0)?;
        file.sync_all()?;
        Ok(Self {
            file,
            extent_id,
            write_offset: EXTENT_HEADER_LEN as u64,
        })
    }

    /// Opens an existing extent file, validating its header. The append cursor
    /// starts at the current file length; callers recovering after a crash
    /// should [`scan`](Self::scan) and [`truncate_to`](Self::truncate_to).
    ///
    /// # Errors
    ///
    /// [`ExtentError::Io`] or a header validation error from [`ExtentHeader::decode`].
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ExtentError> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut header_buf = [0u8; EXTENT_HEADER_LEN];
        file.read_exact_at(&mut header_buf, 0)?;
        let header = ExtentHeader::decode(&header_buf)?;
        let len = file.metadata()?.len();
        Ok(Self {
            file,
            extent_id: header.extent_id,
            write_offset: len.max(EXTENT_HEADER_LEN as u64),
        })
    }

    /// This extent's identity.
    #[must_use]
    pub fn extent_id(&self) -> ExtentId {
        self.extent_id
    }

    /// Current append cursor (also the valid file length).
    #[must_use]
    pub fn write_offset(&self) -> u64 {
        self.write_offset
    }

    /// Appends one blob record (header + body + footer, zero-padded to
    /// [`RECORD_ALIGN`](super::record::RECORD_ALIGN)) at the append cursor and
    /// advances it. Returns `(start_offset, body_crc32c)` so the caller reuses
    /// the body CRC for the index entry (no second hash pass). Does **not**
    /// fsync; call [`sync`](Self::sync) at the commit point (02 §1.7).
    ///
    /// # Errors
    ///
    /// [`ExtentError::Record`] if `body` is too large for the size field, or
    /// [`ExtentError::Io`] on a write failure.
    pub fn append_record(
        &mut self,
        blob_id: BlobId,
        shard_id: ShardId,
        body: &[u8],
    ) -> Result<(u64, u32), ExtentError> {
        let header = RecordHeader::new(blob_id, shard_id, body.len()).map_err(|source| {
            ExtentError::Record {
                offset: self.write_offset,
                source,
            }
        })?;
        let start = self.write_offset;
        let crc = record::body_crc(body);
        let footer = record::encode_footer(crc);

        let mut pos = start;
        self.file.write_all_at(&header.encode(), pos)?;
        pos += RECORD_HEADER_LEN as u64;
        self.file.write_all_at(body, pos)?;
        pos += body.len() as u64;
        self.file.write_all_at(&footer, pos)?;
        pos += RECORD_FOOTER_LEN as u64;

        let on_disk = record::record_on_disk_len(header.body_len);
        let pad = start + on_disk as u64 - pos;
        if pad > 0 {
            let zeros = [0u8; record::RECORD_ALIGN];
            self.file.write_all_at(&zeros[..pad as usize], pos)?;
        }
        self.write_offset = start + on_disk as u64;
        Ok((start, crc))
    }

    /// Reads and fully validates the record at `offset` (header CRC, footer
    /// magic, body CRC32C), returning its header and body.
    ///
    /// # Errors
    ///
    /// [`ExtentError::Record`] on a malformed header/footer,
    /// [`ExtentError::BodyChecksum`] on body corruption, or [`ExtentError::Io`].
    pub fn read_record(&self, offset: u64) -> Result<(RecordHeader, Vec<u8>), ExtentError> {
        let mut header_buf = [0u8; RECORD_HEADER_LEN];
        self.file.read_exact_at(&mut header_buf, offset)?;
        let header = RecordHeader::decode(&header_buf)
            .map_err(|source| ExtentError::Record { offset, source })?;

        let mut body = vec![0u8; header.body_len as usize];
        self.file
            .read_exact_at(&mut body, offset + RECORD_HEADER_LEN as u64)?;

        let mut footer_buf = [0u8; RECORD_FOOTER_LEN];
        self.file.read_exact_at(
            &mut footer_buf,
            offset + RECORD_HEADER_LEN as u64 + u64::from(header.body_len),
        )?;
        let stored_crc = record::decode_footer(&footer_buf)
            .map_err(|source| ExtentError::Record { offset, source })?;

        if record::body_crc(&body) != stored_crc {
            return Err(ExtentError::BodyChecksum { offset });
        }
        Ok((header, body))
    }

    /// fsyncs the file (durability barrier at a blob's commit point, 02 §1.7).
    ///
    /// # Errors
    ///
    /// [`ExtentError::Io`] if the sync fails.
    pub fn sync(&self) -> Result<(), ExtentError> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Reserves on-disk blocks for the first `len` bytes without changing the
    /// logical file length (Linux `fallocate(FALLOC_FL_KEEP_SIZE)`, 02 §1.7): an
    /// extent's full capacity is allocated up front so appends land on reserved
    /// blocks (less fragmentation) and out-of-space surfaces at create time
    /// rather than mid-write. `KEEP_SIZE` leaves the append cursor and file
    /// length untouched, so [`scan`](Self::scan) and recovery still see only
    /// written records.
    ///
    /// Best-effort and platform-gated: a no-op on non-Linux (macOS dev) and when
    /// the filesystem lacks fallocate support (`ENOSYS`/`EOPNOTSUPP`), per the
    /// design's degrade-and-skip rule (02 §1.7).
    ///
    /// # Errors
    ///
    /// [`ExtentError::Io`] if the reservation fails for a reason other than lack
    /// of kernel/filesystem support.
    #[cfg(target_os = "linux")]
    pub fn preallocate(&self, len: u64) -> Result<(), ExtentError> {
        use std::os::unix::io::AsRawFd;

        let len = i64::try_from(len).map_err(|_| {
            ExtentError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "preallocate length exceeds off_t",
            ))
        })?;
        if len == 0 {
            return Ok(());
        }
        // SAFETY: `self.file` owns a valid open fd for the whole call; offset 0
        // and a non-negative `len` are valid fallocate(2) arguments, and the call
        // reads no user memory — it only reserves blocks for this fd.
        let rc =
            unsafe { libc::fallocate(self.file.as_raw_fd(), libc::FALLOC_FL_KEEP_SIZE, 0, len) };
        if rc == 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        // Filesystems without fallocate support (some tmpfs/overlay/network
        // mounts) report ENOSYS/EOPNOTSUPP: degrade to a no-op (02 §1.7).
        match err.raw_os_error() {
            Some(libc::ENOSYS | libc::EOPNOTSUPP) => Ok(()),
            _ => Err(ExtentError::Io(err)),
        }
    }

    /// Reserves on-disk blocks for the first `len` bytes (02 §1.7). A no-op on
    /// non-Linux targets, where `fallocate` is unavailable (macOS dev).
    ///
    /// # Errors
    ///
    /// Never fails on this platform.
    #[cfg(not(target_os = "linux"))]
    pub fn preallocate(&self, _len: u64) -> Result<(), ExtentError> {
        Ok(())
    }

    /// Walks records from the first one, stopping at the first torn or
    /// unwritten record (bad header magic/CRC, or a missing/garbage footer).
    /// Reads only headers and footers — never bodies — so it is cheap and does
    /// not depend on body integrity (02 §1.7 tail-truncation reconciliation).
    ///
    /// # Errors
    ///
    /// [`ExtentError::Io`] if a positioned read fails for a reason other than a
    /// short (truncated) tail, which is treated as the end of valid data.
    pub fn scan(&self) -> Result<Recovery, ExtentError> {
        let file_len = self.file.metadata()?.len();
        let mut offset = EXTENT_HEADER_LEN as u64;
        let mut records = Vec::new();

        loop {
            if offset + RECORD_HEADER_LEN as u64 > file_len {
                break;
            }
            let mut header_buf = [0u8; RECORD_HEADER_LEN];
            self.file.read_exact_at(&mut header_buf, offset)?;
            let Ok(header) = RecordHeader::decode(&header_buf) else {
                break; // unwritten (zeros) or corrupt header → tail ends here
            };

            let footer_off = offset + RECORD_HEADER_LEN as u64 + u64::from(header.body_len);
            if footer_off + RECORD_FOOTER_LEN as u64 > file_len {
                break; // body/footer truncated → torn write
            }
            let mut footer_buf = [0u8; RECORD_FOOTER_LEN];
            self.file.read_exact_at(&mut footer_buf, footer_off)?;
            let Ok(crc) = record::decode_footer(&footer_buf) else {
                break; // footer missing/garbage → torn write
            };

            records.push(RecoveredRecord {
                blob_id: header.blob_id,
                shard_id: header.shard_id,
                offset,
                body_len: header.body_len,
                crc,
            });
            offset += record::record_on_disk_len(header.body_len) as u64;
        }

        Ok(Recovery {
            records,
            valid_end: offset,
            truncated: offset < file_len,
        })
    }

    /// Truncates the file to `valid_end`, fsyncs, and resets the append cursor.
    /// Used after [`scan`](Self::scan) to drop an uncommitted torn tail.
    ///
    /// # Errors
    ///
    /// [`ExtentError::Io`] if the truncation or sync fails.
    pub fn truncate_to(&mut self, valid_end: u64) -> Result<(), ExtentError> {
        self.file.set_len(valid_end)?;
        self.file.sync_all()?;
        self.write_offset = valid_end;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use epoch_proto::{ChunkId, WriterToken};

    fn extent_id() -> ExtentId {
        ExtentId::new(ShardId::new(ChunkId::new(1), 0, 0), 1_700_000_000)
    }

    fn blob(seq: u32) -> BlobId {
        BlobId::new(WriterToken::new(1), seq)
    }

    #[test]
    fn header_round_trip() {
        let header = ExtentHeader {
            extent_id: extent_id(),
        };
        assert_eq!(
            ExtentHeader::decode(&header.encode()).expect("decode"),
            header
        );
    }

    #[test]
    fn header_bad_magic_and_checksum() {
        let header = ExtentHeader {
            extent_id: extent_id(),
        };
        let mut bytes = header.encode();
        bytes[0] ^= 0xFF;
        assert!(matches!(
            ExtentHeader::decode(&bytes),
            Err(ExtentError::BadHeaderMagic)
        ));

        let mut bytes = header.encode();
        bytes[EH_EXTENT_ID] ^= 0xFF; // covered by crc, magic intact
        assert!(matches!(
            ExtentHeader::decode(&bytes),
            Err(ExtentError::HeaderChecksum { .. })
        ));
    }

    #[test]
    fn append_read_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("extent");
        let shard = ShardId::new(ChunkId::new(1), 0, 0);

        let mut ext = ExtentFile::create(&path, extent_id()).expect("create");
        let bodies: [&[u8]; 3] = [b"first", &[0xABu8; 5000], b""];
        let mut offsets = Vec::new();
        for (i, body) in bodies.iter().enumerate() {
            let seq = u32::try_from(i).unwrap();
            let (off, _crc) = ext.append_record(blob(seq), shard, body).expect("append");
            offsets.push(off);
        }
        ext.sync().expect("sync");

        for (i, (&off, body)) in offsets.iter().zip(bodies.iter()).enumerate() {
            let (header, read) = ext.read_record(off).expect("read");
            assert_eq!(header.blob_id, blob(u32::try_from(i).unwrap()));
            assert_eq!(header.shard_id, shard);
            assert_eq!(&read, body);
        }
    }

    #[test]
    fn preallocate_keeps_cursor_and_stays_appendable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("extent");
        let shard = ShardId::new(ChunkId::new(1), 0, 0);

        let mut ext = ExtentFile::create(&path, extent_id()).expect("create");
        let before = ext.write_offset();
        ext.preallocate(4 * 1024 * 1024).expect("preallocate");
        // KEEP_SIZE leaves the logical cursor untouched (a no-op on non-Linux).
        assert_eq!(ext.write_offset(), before);

        // The extent still appends and reads back after preallocation.
        let (off, _crc) = ext
            .append_record(blob(0), shard, b"after-prealloc")
            .expect("append");
        ext.sync().expect("sync");
        let (_header, body) = ext.read_record(off).expect("read");
        assert_eq!(&body, b"after-prealloc");

        // On Linux the blocks are actually reserved beyond the written tail.
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::MetadataExt;
            let blocks = std::fs::metadata(&path).expect("meta").blocks();
            assert!(
                blocks * 512 >= 4 * 1024 * 1024,
                "fallocate reserved the requested blocks"
            );
        }
    }

    #[test]
    fn records_are_4kib_aligned() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("extent");
        let shard = ShardId::new(ChunkId::new(1), 0, 0);

        let mut ext = ExtentFile::create(&path, extent_id()).expect("create");
        let (first, _) = ext.append_record(blob(0), shard, b"tiny").expect("append");
        let (second, _) = ext.append_record(blob(1), shard, b"tiny").expect("append");
        assert_eq!(first, EXTENT_HEADER_LEN as u64);
        assert_eq!(second % record::RECORD_ALIGN as u64, 0);
        assert_eq!(second, (EXTENT_HEADER_LEN + record::RECORD_ALIGN) as u64);
    }

    #[test]
    fn read_detects_body_corruption() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("extent");
        let shard = ShardId::new(ChunkId::new(1), 0, 0);

        let off = {
            let mut ext = ExtentFile::create(&path, extent_id()).expect("create");
            let (off, _crc) = ext
                .append_record(blob(0), shard, b"payload-body")
                .expect("append");
            ext.sync().expect("sync");
            off
        };
        // Flip a byte inside the body region (after the 32-byte record header).
        let mut data = std::fs::read(&path).expect("read");
        let body_byte = (off + RECORD_HEADER_LEN as u64 + 2) as usize;
        data[body_byte] ^= 0xFF;
        std::fs::write(&path, &data).expect("write");

        let ext = ExtentFile::open(&path).expect("open");
        assert!(matches!(
            ext.read_record(off),
            Err(ExtentError::BodyChecksum { .. })
        ));
    }

    #[test]
    fn scan_recovers_all_intact_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("extent");
        let shard = ShardId::new(ChunkId::new(1), 0, 0);

        let end = {
            let mut ext = ExtentFile::create(&path, extent_id()).expect("create");
            for i in 0..3u32 {
                ext.append_record(blob(i), shard, &[i as u8; 100])
                    .expect("append");
            }
            ext.sync().expect("sync");
            ext.write_offset()
        };

        let ext = ExtentFile::open(&path).expect("open");
        let recovery = ext.scan().expect("scan");
        assert_eq!(recovery.records.len(), 3);
        assert_eq!(recovery.valid_end, end);
        assert!(!recovery.truncated);
        assert_eq!(recovery.records[2].blob_id, blob(2));
    }

    #[test]
    fn scan_truncates_torn_tail() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("extent");
        let shard = ShardId::new(ChunkId::new(1), 0, 0);

        let valid_end = {
            let mut ext = ExtentFile::create(&path, extent_id()).expect("create");
            ext.append_record(blob(0), shard, b"committed-1")
                .expect("append");
            ext.append_record(blob(1), shard, b"committed-2")
                .expect("append");
            ext.sync().expect("sync");
            ext.write_offset()
        };

        // Simulate a crash mid-append: a bare record header with no body/footer.
        {
            let ext = ExtentFile::open(&path).expect("open");
            let torn = RecordHeader::new(blob(2), shard, 4096).expect("header");
            ext.file
                .write_all_at(&torn.encode(), valid_end)
                .expect("write torn");
            ext.file.sync_all().expect("sync");
        }

        let mut ext = ExtentFile::open(&path).expect("reopen");
        let recovery = ext.scan().expect("scan");
        assert_eq!(
            recovery.records.len(),
            2,
            "only committed records recovered"
        );
        assert_eq!(recovery.valid_end, valid_end);
        assert!(recovery.truncated);

        ext.truncate_to(recovery.valid_end).expect("truncate");
        let clean = ext.scan().expect("rescan");
        assert_eq!(clean.records.len(), 2);
        assert!(!clean.truncated, "tail is clean after truncation");
    }
}
