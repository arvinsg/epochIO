//! Partition coordinates: the routing-key interval one partition owns, the
//! membership check derived from it, and its per-CF export bounds.
//!
//! A partition is an interval in one namespace's `(bucket_id, routing_key)`
//! coordinate system backed by one raft group (03 §2). Keys never embed the
//! partition id — membership is a pure function of the key
//! ([`PartitionRange::contains`] parses the embedded routing key, 03 §2
//! 归属规则), which is what makes the zero-transaction axiom mechanically
//! checkable (03 §1).
//!
//! Design: docs/design/03-metanode.md §2 (分区模型与归属规则), §4.1 (导出连续性)

use epoch_proto::BucketId;
use serde::{Deserialize, Serialize};

use crate::store::KeyRange;
use crate::store::keys::{MetaCf, routing_key};

/// The namespace a partition belongs to (03 §2: 分区不跨命名空间).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Namespace {
    /// Object-key dictionary order (03 §3 flat mode).
    Flat,
    /// `(parent_ino, name)` order (03 §3/§6 hierarchical mode).
    Hier,
}

impl Namespace {
    /// The primary-index CF of this namespace (`meta` for flat, `fs` for hier).
    #[must_use]
    pub const fn primary_cf(self) -> MetaCf {
        match self {
            Namespace::Flat => MetaCf::Meta,
            Namespace::Hier => MetaCf::Fs,
        }
    }
}

/// A half-open routing-key interval `[(bucket, routing), …)` in one namespace.
///
/// `None` bounds denote ±∞. The routing key bytes use the namespace encoding:
/// the bare object key for flat, `parent_ino(8B BE) | name` for hier — exactly
/// what [`routing_key`] extracts from an encoded key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionRange {
    /// The namespace this interval lives in.
    pub ns: Namespace,
    /// Inclusive start in `(bucket, routing-key)` coordinates (`None` = -∞).
    pub start: Option<(BucketId, Vec<u8>)>,
    /// Exclusive end in `(bucket, routing-key)` coordinates (`None` = +∞).
    pub end: Option<(BucketId, Vec<u8>)>,
}

impl PartitionRange {
    /// The whole coordinate space of one namespace (the bootstrap partition).
    #[must_use]
    pub fn full(ns: Namespace) -> Self {
        Self {
            ns,
            start: None,
            end: None,
        }
    }

    /// Splits the interval at `(bucket, routing_key)` into a left child
    /// `[start, at)` and a right child `[at, end)` (03 §2: 分裂 = 纯路由变更;
    /// the boundary is an object/directory-entry key so an object's whole key
    /// family stays together). Returns `None` if `at` is not strictly inside
    /// the interval (equal to `start`, or ≥ `end`) — an empty child is never
    /// produced.
    #[must_use]
    pub fn split(&self, at: (BucketId, Vec<u8>)) -> Option<(PartitionRange, PartitionRange)> {
        // `at` must be strictly after start and strictly before end.
        if !self.contains_coord(at.0, &at.1) {
            return None;
        }
        if self
            .start
            .as_ref()
            .is_some_and(|(b, k)| (b.get(), k.as_slice()) == (at.0.get(), at.1.as_slice()))
        {
            return None; // `at == start` → left child would be empty
        }
        let left = PartitionRange {
            ns: self.ns,
            start: self.start.clone(),
            end: Some(at.clone()),
        };
        let right = PartitionRange {
            ns: self.ns,
            start: Some(at),
            end: self.end.clone(),
        };
        Some((left, right))
    }

    /// Whether the coordinate `(bucket, routing_key)` falls inside this
    /// interval — the coordinate form of [`contains`](Self::contains), for
    /// route lookups that have not encoded a full key (03 §2).
    #[must_use]
    pub fn contains_coord(&self, bucket: BucketId, routing_key: &[u8]) -> bool {
        let coord = (bucket.get(), routing_key);
        let after_start = self
            .start
            .as_ref()
            .is_none_or(|(b, k)| coord >= (b.get(), k.as_slice()));
        let before_end = self
            .end
            .as_ref()
            .is_none_or(|(b, k)| coord < (b.get(), k.as_slice()));
        after_start && before_end
    }

    /// Whether `key` (a full encoded key, 03 §4.1) belongs to this partition —
    /// the mechanical 归属校验 of 03 §2.
    ///
    /// A partition never owns keys of the other namespace's primary CF, and
    /// PD guarantees flat/hier bucket id spaces do not interleave inside one
    /// partition's bucket span (01 §3 MetaPartition.ns), so in the shared CFs
    /// (`meta_seg`/`upload`/`delq`/`ttl`) coordinate comparison alone is exact.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        let Some((&tag, _)) = key.split_first() else {
            return false;
        };
        if tag == self.ns.primary_cf().tag() || self.owns_shared_cf(tag) {
            let Some((bucket, routing)) = routing_key(key) else {
                return false;
            };
            self.contains_coord(bucket, &routing)
        } else {
            false
        }
    }

    /// Shared CFs hold both namespaces' keys (03 §4.1); the primary CF of the
    /// *other* namespace is the only CF a partition never owns.
    fn owns_shared_cf(&self, tag: u8) -> bool {
        let other_primary = match self.ns {
            Namespace::Flat => MetaCf::Fs.tag(),
            Namespace::Hier => MetaCf::Meta.tag(),
        };
        MetaCf::ALL.iter().any(|cf| cf.tag() == tag) && tag != other_primary
    }

    /// The per-CF byte bounds covering this interval, for snapshot export
    /// (03 §4.1 导出连续性: an interval is a contiguous segment per CF).
    ///
    /// The `ttl` CF is bounded by bucket span only (its keys order by
    /// `expire_ts` inside a bucket, not by routing key), so export must filter
    /// its entries with [`contains`](Self::contains) — exactly the "散射 + 过滤"
    /// treatment of 03 §4.1. Filtering all CFs uniformly is equally correct
    /// and costs nothing for the contiguous ones.
    ///
    /// The shared CFs (`meta_seg`/`upload`/`delq`/`ttl`) are additionally
    /// clamped by the namespace-bit boundary (HIER_BUCKET_BIT, 03 §2 归属不变量):
    /// a flat partition's scan stops at the bit, a hierarchical one's starts
    /// there — the other namespace's keys can never leak into the export.
    #[must_use]
    pub fn key_ranges(&self) -> Vec<KeyRange> {
        let cfs: &[MetaCf] = match self.ns {
            Namespace::Flat => &[
                MetaCf::Meta,
                MetaCf::MetaSeg,
                MetaCf::Upload,
                MetaCf::Delq,
                MetaCf::Ttl,
            ],
            Namespace::Hier => &[
                MetaCf::Fs,
                MetaCf::MetaSeg,
                MetaCf::Upload,
                MetaCf::Delq,
                MetaCf::Ttl,
            ],
        };
        cfs.iter()
            .map(|&cf| {
                let mut start = self
                    .start
                    .as_ref()
                    .map_or_else(|| vec![cf.tag()], |(b, k)| cf_bound(cf, b, k));
                let mut end = self.end.as_ref().map_or_else(
                    || vec![cf.tag() + 1],
                    |(b, k)| {
                        if cf == MetaCf::Ttl {
                            // Routing bounds do not translate to byte bounds in
                            // the expire_ts-ordered ttl CF; bound by bucket.
                            vec![cf.tag()]
                                .into_iter()
                                .chain(b.get().to_be_bytes())
                                .collect()
                        } else {
                            cf_bound(cf, b, k)
                        }
                    },
                );
                if is_shared_cf(cf) {
                    let bit_bound = ns_bit_bound(cf);
                    match self.ns {
                        Namespace::Flat => {
                            if end > bit_bound {
                                end = bit_bound;
                            }
                        }
                        Namespace::Hier => {
                            if start < bit_bound {
                                start = bit_bound;
                            }
                        }
                    }
                }
                KeyRange { cf, start, end }
            })
            .collect()
    }

    /// The primary-index CF and its `[start, end)` byte bounds for this range —
    /// the scan bounds a split-point picker walks to find a median object /
    /// directory-entry key (03 §2). The primary CF (`meta`/`fs`) is
    /// namespace-exclusive, so no shared-CF bit clamping is needed.
    #[must_use]
    pub fn primary_scan_bounds(&self) -> (MetaCf, Vec<u8>, Vec<u8>) {
        let cf = self.ns.primary_cf();
        let start = self
            .start
            .as_ref()
            .map_or_else(|| vec![cf.tag()], |(b, k)| cf_bound(cf, b, k));
        let end = self
            .end
            .as_ref()
            .map_or_else(|| vec![cf.tag() + 1], |(b, k)| cf_bound(cf, b, k));
        (cf, start, end)
    }
}

/// `tag | bucket | routing` — the byte bound of a coordinate in one CF.
fn cf_bound(cf: MetaCf, bucket: &BucketId, routing: &[u8]) -> Vec<u8> {
    let mut bound = Vec::with_capacity(9 + routing.len());
    bound.push(cf.tag());
    bound.extend_from_slice(&bucket.get().to_be_bytes());
    bound.extend_from_slice(routing);
    bound
}

/// Whether a CF holds both namespaces' keys (03 §4.1: `meta_seg`/`upload`/
/// `delq`/`ttl` are shared; `meta`/`fs` are namespace-exclusive).
fn is_shared_cf(cf: MetaCf) -> bool {
    !matches!(cf, MetaCf::Meta | MetaCf::Fs)
}

/// The namespace-bit boundary key inside a shared CF: `tag | HIER_BUCKET_BIT`
/// — flat keys sort strictly below it, hierarchical keys at or above it
/// (03 §2/§4.1 归属不变量, enforced by PD's bucket-id allocation).
fn ns_bit_bound(cf: MetaCf) -> Vec<u8> {
    let mut bound = Vec::with_capacity(9);
    bound.push(cf.tag());
    bound.extend_from_slice(&epoch_proto::consts::HIER_BUCKET_BIT.to_be_bytes());
    bound
}

/// One locally-hosted partition's routing info: its interval plus the peer
/// address book (for NotLeader hints, 06 §9 service.rs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionInfo {
    /// The partition's routing-key interval.
    pub range: PartitionRange,
    /// Raft voter `(node_id, addr)` pairs of the group.
    pub peers: Vec<(u64, String)>,
}

/// The local partition registry (06 §9 partition/mod.rs: 分区注册表/路由
/// range 判定/归属校验).
///
/// Populated by the `CreateRaftGroup` admin path; consulted by every metadata
/// op to decide whether *this* node hosts the key's partition. Lookups are a
/// linear scan with the 归属校验 — a node hosts O(百) groups (03 §8), and a
/// keyed route index lands with splits (M5b) when the count grows.
#[derive(Default)]
pub struct PartitionRegistry {
    inner: std::sync::RwLock<std::collections::BTreeMap<u64, PartitionInfo>>,
}

impl PartitionRegistry {
    /// Registers (or refreshes) a partition's local routing info. Idempotent:
    /// a repeated `CreateRaftGroup` push re-registers the same descriptor.
    pub fn register(&self, group: u64, info: PartitionInfo) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(group, info);
    }

    /// The locally-hosted partition covering `(bucket, routing_key)`, if any.
    #[must_use]
    pub fn route(&self, bucket: BucketId, routing_key: &[u8]) -> Option<u64> {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|(_, info)| info.range.contains_coord(bucket, routing_key))
            .map(|(group, _)| *group)
    }

    /// The addr of one raft peer of a group (NotLeader hint lookup).
    #[must_use]
    pub fn peer_addr(&self, group: u64, node: u64) -> Option<String> {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&group)?
            .peers
            .iter()
            .find(|(id, _)| *id == node)
            .map(|(_, addr)| addr.clone())
    }

    /// The routing info of one group, if hosted.
    #[must_use]
    pub fn info(&self, group: u64) -> Option<PartitionInfo> {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&group)
            .cloned()
    }
}

#[cfg(test)]
mod registry_tests {
    use super::*;

    fn info(start: Option<(u64, &[u8])>, end: Option<(u64, &[u8])>) -> PartitionInfo {
        PartitionInfo {
            range: PartitionRange {
                ns: Namespace::Flat,
                start: start.map(|(b, k)| (BucketId::new(b), k.to_vec())),
                end: end.map(|(b, k)| (BucketId::new(b), k.to_vec())),
            },
            peers: vec![(1, "a:1".to_string()), (2, "a:2".to_string())],
        }
    }

    #[test]
    fn route_finds_the_covering_local_partition() {
        let registry = PartitionRegistry::default();
        registry.register(1, info(None, Some((10, b"img/5"))));
        registry.register(2, info(Some((10, b"img/5")), None));

        assert_eq!(registry.route(BucketId::new(1), b"a"), Some(1));
        assert_eq!(registry.route(BucketId::new(10), b"img/49999"), Some(1));
        assert_eq!(registry.route(BucketId::new(10), b"img/5"), Some(2));
        assert_eq!(registry.route(BucketId::new(99), b"z"), Some(2));
        assert_eq!(registry.peer_addr(2, 1).as_deref(), Some("a:1"));
        assert!(registry.peer_addr(2, 99).is_none());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::keys::{flat_key, hier_key, suffix};

    fn bucket(id: u64) -> BucketId {
        BucketId::new(id)
    }

    #[test]
    fn full_range_contains_every_key_of_its_namespace() {
        let flat = PartitionRange::full(Namespace::Flat);
        assert!(flat.contains(&flat_key(MetaCf::Meta, bucket(1), b"a/b", &[])));
        assert!(flat.contains(&flat_key(
            MetaCf::MetaSeg,
            bucket(2),
            b"big",
            &suffix::seg_no(0)
        )));
        assert!(flat.contains(&flat_key(
            MetaCf::Delq,
            bucket(3),
            b"d",
            &suffix::delq_seq(1, 0)
        )));
        // Not the other namespace's primary index, and not bookkeeping CFs.
        assert!(!flat.contains(&hier_key(MetaCf::Fs, bucket(1), 1, b"x", &[])));
        assert!(!flat.contains(&[MetaCf::Applied.tag(), 0, 0]));

        let hier = PartitionRange::full(Namespace::Hier);
        assert!(hier.contains(&hier_key(MetaCf::Fs, bucket(1), 9, b"dir/f", &[])));
        assert!(!hier.contains(&flat_key(MetaCf::Meta, bucket(1), b"a", &[])));
    }

    #[test]
    fn interval_bounds_are_start_inclusive_end_exclusive() {
        let range = PartitionRange {
            ns: Namespace::Flat,
            start: Some((bucket(1), b"img/5".to_vec())),
            end: Some((bucket(2), b"".to_vec())),
        };
        assert!(!range.contains(&flat_key(MetaCf::Meta, bucket(1), b"img/49999", &[])));
        assert!(range.contains(&flat_key(MetaCf::Meta, bucket(1), b"img/5", &[])));
        assert!(range.contains(&flat_key(MetaCf::Meta, bucket(1), b"zzz", &[])));
        // The end bound is exclusive: (bucket-2, "") itself is already outside.
        assert!(!range.contains(&flat_key(MetaCf::Meta, bucket(2), b"", &[])));
        assert!(!range.contains(&flat_key(MetaCf::Meta, bucket(2), b"a", &[])));
    }

    #[test]
    fn split_divides_interval_and_preserves_coverage() {
        // A full flat range split at (bucket 5, "m").
        let full = PartitionRange::full(Namespace::Flat);
        let (left, right) = full
            .split((bucket(5), b"m".to_vec()))
            .expect("split full range");
        assert_eq!(left.start, None);
        assert_eq!(left.end, Some((bucket(5), b"m".to_vec())));
        assert_eq!(right.start, Some((bucket(5), b"m".to_vec())));
        assert_eq!(right.end, None);
        // Every key routes to exactly one child, and the boundary is on the
        // right (start-inclusive).
        let before = flat_key(MetaCf::Meta, bucket(5), b"a", &[]);
        let at = flat_key(MetaCf::Meta, bucket(5), b"m", &[]);
        let after = flat_key(MetaCf::Meta, bucket(5), b"z", &[]);
        assert!(left.contains(&before) && !right.contains(&before));
        assert!(right.contains(&at) && !left.contains(&at));
        assert!(right.contains(&after) && !left.contains(&after));

        // A bounded range splits within its bounds.
        let bounded = PartitionRange {
            ns: Namespace::Flat,
            start: Some((bucket(1), b"a".to_vec())),
            end: Some((bucket(1), b"z".to_vec())),
        };
        let (l, r) = bounded
            .split((bucket(1), b"m".to_vec()))
            .expect("split bounded");
        assert_eq!(l.start, Some((bucket(1), b"a".to_vec())));
        assert_eq!(r.end, Some((bucket(1), b"z".to_vec())));

        // A boundary at/outside the interval yields no split (no empty child).
        assert!(bounded.split((bucket(1), b"a".to_vec())).is_none()); // == start
        assert!(bounded.split((bucket(1), b"z".to_vec())).is_none()); // == end (excluded)
        assert!(bounded.split((bucket(9), b"x".to_vec())).is_none()); // outside
    }

    #[test]
    fn ttl_membership_uses_embedded_object_key() {
        let range = PartitionRange {
            ns: Namespace::Flat,
            start: None,
            end: Some((bucket(1), b"m".to_vec())),
        };
        let mut ttl_key = Vec::new();
        ttl_key.push(MetaCf::Ttl.tag());
        ttl_key.extend_from_slice(&bucket(1).get().to_be_bytes());
        ttl_key.extend_from_slice(&suffix::expire_ts(u64::MAX));
        ttl_key.extend_from_slice(b"a");
        // expire_ts = u64::MAX must not push the key out of the routing range.
        assert!(range.contains(&ttl_key));
    }

    #[test]
    fn key_ranges_cover_namespace_cfs_with_byte_bounds() {
        let range = PartitionRange {
            ns: Namespace::Flat,
            start: Some((bucket(1), b"img/".to_vec())),
            end: Some((bucket(2), b"".to_vec())),
        };
        let ranges = range.key_ranges();
        assert_eq!(ranges.len(), 5);
        let meta = &ranges[0];
        assert_eq!(meta.cf, MetaCf::Meta);
        assert_eq!(meta.start, flat_key(MetaCf::Meta, bucket(1), b"img/", &[]));
        assert_eq!(meta.end, flat_key(MetaCf::Meta, bucket(2), b"", &[]));
        // Every owned key falls inside its CF's byte bounds.
        let key = flat_key(MetaCf::Meta, bucket(1), b"img/1", &[]);
        assert!(meta.start <= key && key < meta.end);

        // A full range spans each CF tag byte-space.
        let full = PartitionRange::full(Namespace::Hier);
        let fs = &full.key_ranges()[0];
        assert_eq!(fs.cf, MetaCf::Fs);
        assert_eq!(fs.start, vec![MetaCf::Fs.tag()]);
        assert_eq!(fs.end, vec![MetaCf::Fs.tag() + 1]);
    }

    #[test]
    fn shared_cfs_are_clamped_by_the_namespace_bit() {
        let bit = epoch_proto::consts::HIER_BUCKET_BIT;
        // A full-range flat partition: shared CF scans stop at the ns bit.
        let flat = PartitionRange::full(Namespace::Flat);
        for r in flat.key_ranges() {
            match r.cf {
                MetaCf::Meta => assert_eq!(r.end, vec![MetaCf::Meta.tag() + 1]),
                _ => {
                    let mut expected = vec![r.cf.tag()];
                    expected.extend_from_slice(&bit.to_be_bytes());
                    assert_eq!(r.end, expected, "flat shared CF clamped at the bit");
                }
            }
        }
        // A full-range hier partition: shared CF scans start at the ns bit.
        let hier = PartitionRange::full(Namespace::Hier);
        for r in hier.key_ranges() {
            match r.cf {
                MetaCf::Fs => assert_eq!(r.start, vec![MetaCf::Fs.tag()]),
                _ => {
                    let mut expected = vec![r.cf.tag()];
                    expected.extend_from_slice(&bit.to_be_bytes());
                    assert_eq!(r.start, expected, "hier shared CF starts at the bit");
                }
            }
        }
    }
}
