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

//! Metadata key encoding: the single physical layout every MetaStore engine
//! shares (03 §4.1), with the partition membership rule baked in (03 §2 —
//! every key embeds its namespace routing key, so membership is a pure function
//! of the key, mechanically checkable per the zero-transaction axiom in 03 §1).
//!
//! ```text
//! key = ns(1B) | bucket_id(8B BE) | routing_key | suffix…
//!
//! flat namespace (routing key = object_key):
//!   'o'  object_key                    → ObjectHead
//!   'g'  object_key | seg_no(u32 BE)   → SliceSegment
//!   'u'  object_key | upload_id(16B) | part_no(u32 BE) → PartMeta
//!   'q'  object_key | seq(u64 BE) | seg_no(u32 BE)     → PendingDelete
//!   't'  expire_ts(u64 BE) | object_key                → ttl index
//!
//! hierarchical namespace (routing key = (parent_ino, name)):
//!   'f'  parent_ino(u64 BE) | name     → FileRecord / DirEntry / DirRecord
//!   'g'  parent_ino(u64 BE) | name | seg_no(u32 BE)
//!   'u'  parent_ino(u64 BE) | name | upload_id(16B) | part_no(u32 BE)
//!   'q'  parent_ino(u64 BE) | name | seq(u64 BE) | seg_no(u32 BE)
//! ```

use epoch_proto::BucketId;

/// A column family of the unified metadata layout (03 §4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum MetaCf {
    /// `o` — flat primary index (ObjectHead).
    Meta,
    /// `f` — hierarchical primary index (FileRecord / DirEntry / DirRecord).
    Fs,
    /// `g` — slice overflow segments (both namespaces).
    MetaSeg,
    /// `u` — multipart sessions and parts.
    Upload,
    /// `q` — persistent delete queue.
    Delq,
    /// `t` — lifecycle expiry index.
    Ttl,
    /// `applied` — per-partition applied raft index (not user data).
    Applied,
}

impl MetaCf {
    /// The stable one-byte namespace tag of a key in this CF.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            MetaCf::Meta => b'o',
            MetaCf::Fs => b'f',
            MetaCf::MetaSeg => b'g',
            MetaCf::Upload => b'u',
            MetaCf::Delq => b'q',
            MetaCf::Ttl => b't',
            MetaCf::Applied => 0xFF,
        }
    }

    /// The stable RocksDB column-family name backing this CF.
    #[must_use]
    pub const fn cf_name(self) -> &'static str {
        match self {
            MetaCf::Meta => "meta",
            MetaCf::Fs => "fs",
            MetaCf::MetaSeg => "meta_seg",
            MetaCf::Upload => "upload",
            MetaCf::Delq => "delq",
            MetaCf::Ttl => "ttl",
            MetaCf::Applied => "applied",
        }
    }

    /// All user-data CFs (everything except `Applied`).
    pub const ALL: &'static [MetaCf] = &[
        MetaCf::Meta,
        MetaCf::Fs,
        MetaCf::MetaSeg,
        MetaCf::Upload,
        MetaCf::Delq,
        MetaCf::Ttl,
    ];
}

/// Encodes a flat-namespace key: `ns | bucket_id | object_key [| suffix…]`.
#[must_use]
pub fn flat_key(cf: MetaCf, bucket: BucketId, object_key: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 8 + object_key.len() + suffix.len());
    key.push(cf.tag());
    key.extend_from_slice(&bucket.get().to_be_bytes());
    key.extend_from_slice(object_key);
    key.extend_from_slice(suffix);
    key
}

/// Encodes a hierarchical-namespace key: `ns | bucket_id | parent_ino | name [| suffix…]`.
#[must_use]
pub fn hier_key(
    cf: MetaCf,
    bucket: BucketId,
    parent_ino: u64,
    name: &[u8],
    suffix: &[u8],
) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 8 + 8 + name.len() + suffix.len());
    key.push(cf.tag());
    key.extend_from_slice(&bucket.get().to_be_bytes());
    key.extend_from_slice(&parent_ino.to_be_bytes());
    key.extend_from_slice(name);
    key.extend_from_slice(suffix);
    key
}

/// The hierarchical routing key `parent_ino(u64 BE) | name` (03 §2: hier
/// coordinate space) — the bytes a [`hier_key`] embeds after the bucket, and
/// what `PartitionRange` compares against for membership/routing.
#[must_use]
pub fn hier_routing_key(parent_ino: u64, name: &[u8]) -> Vec<u8> {
    let mut routing = Vec::with_capacity(8 + name.len());
    routing.extend_from_slice(&parent_ino.to_be_bytes());
    routing.extend_from_slice(name);
    routing
}

/// The `(bucket_id, routing_key)` membership coordinates of a key (03 §2 归属规则):
/// for flat keys the routing key is the object key; for hierarchical keys it is
/// `(parent_ino, name)` encoded big-endian. For ttl keys the leading `expire_ts`
/// is stripped (03 §4.1: `t | bucket | expire_ts | 路由key`).
///
/// Returns `None` for malformed keys (wrong tag / too short) and for `Applied`.
#[must_use]
pub fn routing_key(key: &[u8]) -> Option<(BucketId, Vec<u8>)> {
    let (&tag, rest) = key.split_first()?;
    if rest.len() < 8 {
        return None;
    }
    let (id, routing) = rest.split_at(8);
    let bucket = BucketId::new(u64::from_be_bytes(id.try_into().ok()?));
    match tag {
        b'o' | b'g' | b'u' | b'q' | b'f' => Some((bucket, routing.to_vec())),
        b't' => {
            // Skip expire_ts(u64 BE); what follows is the routing key.
            routing.get(8..).map(|routing| (bucket, routing.to_vec()))
        }
        _ => None,
    }
}

/// The prefix covering every key of `bucket` in one CF (`ns | bucket_id`).
#[must_use]
pub fn bucket_prefix(cf: MetaCf, bucket: BucketId) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(9);
    prefix.push(cf.tag());
    prefix.extend_from_slice(&bucket.get().to_be_bytes());
    prefix
}

/// The exclusive upper bound of a prefix range: the smallest key strictly
/// greater than every key carrying `prefix` (strip trailing `0xFF`, increment
/// the last byte). Returns `None` when the prefix is all `0xFF` (unbounded).
#[must_use]
pub fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(&last) = end.last() {
        if last == 0xFF {
            end.pop();
        } else {
            *end.last_mut().expect("non-empty") = last + 1;
            return Some(end);
        }
    }
    None
}

/// Suffix encoders for the variable tails of each key family.
pub mod suffix {
    /// `seg_no(u32 BE)` for meta_seg keys.
    #[must_use]
    pub fn seg_no(seg_no: u32) -> [u8; 4] {
        seg_no.to_be_bytes()
    }

    /// `upload_id(16B) | part_no(u32 BE)` for multipart keys.
    #[must_use]
    pub fn upload_part(upload_id: u128, part_no: u32) -> [u8; 20] {
        let mut out = [0u8; 20];
        out[..16].copy_from_slice(&upload_id.to_be_bytes());
        out[16..].copy_from_slice(&part_no.to_be_bytes());
        out
    }

    /// `seq(u64 BE) | seg_no(u32 BE)` for delq keys.
    #[must_use]
    pub fn delq_seq(seq: u64, seg_no: u32) -> [u8; 12] {
        let mut out = [0u8; 12];
        out[..8].copy_from_slice(&seq.to_be_bytes());
        out[8..].copy_from_slice(&seg_no.to_be_bytes());
        out
    }

    /// `expire_ts(u64 BE)` for ttl keys (precedes the routing key).
    #[must_use]
    pub fn expire_ts(expire_ts: u64) -> [u8; 8] {
        expire_ts.to_be_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_key_layout_and_membership() {
        let bucket = BucketId::new(0x0102);
        let key = flat_key(MetaCf::Meta, bucket, b"models/a.ckpt", &suffix::seg_no(7));
        assert_eq!(&key[..2], &[MetaCf::Meta.tag(), 0]);
        let (b, routing) = routing_key(&key).expect("membership");
        assert_eq!(b, bucket);
        assert!(routing.starts_with(b"models/a.ckpt"));
        assert!(
            bucket_prefix(MetaCf::Meta, bucket)
                .iter()
                .zip(key.iter())
                .all(|(a, b)| a == b)
        );
    }

    #[test]
    fn hier_key_membership_and_suffixes() {
        let bucket = BucketId::new(9);
        let key = hier_key(
            MetaCf::Delq,
            bucket,
            42,
            b"dir/file",
            &suffix::delq_seq(5, 3),
        );
        let (b, routing) = routing_key(&key).expect("membership");
        assert_eq!(b, bucket);
        assert_eq!(&routing[..8], &42u64.to_be_bytes());
        assert!(routing[8..].starts_with(b"dir/file"));
    }

    #[test]
    fn ttl_key_membership_skips_expire_ts() {
        let bucket = BucketId::new(3);
        // t | bucket | expire_ts | object_key (03 §4.1): membership must see the
        // object key, not the timestamp (03 §2 归属规则 for export filtering).
        let mut ttl_key = Vec::new();
        ttl_key.push(MetaCf::Ttl.tag());
        ttl_key.extend_from_slice(&bucket.get().to_be_bytes());
        ttl_key.extend_from_slice(&suffix::expire_ts(1_700_000_000));
        ttl_key.extend_from_slice(b"obj");
        assert_eq!(routing_key(&ttl_key), Some((bucket, b"obj".to_vec())));

        // A ttl key missing the expire_ts field is malformed.
        let short = flat_key(MetaCf::Ttl, bucket, b"obj", &[]);
        assert_eq!(routing_key(&short), None);
    }

    #[test]
    fn prefix_end_computes_tight_upper_bounds() {
        assert_eq!(prefix_end(b"img/"), Some(b"img0".to_vec()));
        assert_eq!(prefix_end(&[0x61, 0xFF]), Some(vec![0x62]));
        assert_eq!(prefix_end(&[0xFF, 0xFF]), None);
        assert_eq!(prefix_end(&[]), None);
    }

    #[test]
    fn routing_key_rejects_malformed() {
        assert_eq!(routing_key(&[]), None);
        assert_eq!(routing_key(&[b'o', 1, 2]), None);
        assert_eq!(routing_key(&[0xFF, 0, 0, 0, 0, 0, 0, 0, 1]), None);
    }
}
