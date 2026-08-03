//! epoch-meta (L3): the MetaNode service.
//!
//! Object/file metadata with a zero-transaction partitioning axiom, bucket-level
//! dual namespaces (flat / hierarchical), a dual MetaStore engine (Rocks / Mem),
//! TiKV-style range partitions over openraft multi-raft, and a persistent
//! delete queue that directly drives tombstoning.
//!
//! Design: docs/design/03-metanode.md; docs/design/06-code-layout.md §9
//!
//! M5a delivered (docs/design/07-iteration-plan.md §M5a 实际交付): the storage
//! engines ([`store`], 03 §7), the multi-raft runtime with the batched
//! transport ([`raft`] + [`partition`] coordinates, 03 §8), the flat
//! namespace with apply-time old-slices capture ([`ns_flat`], 03 §5), the
//! delete queue ([`deleter`] + injected DeleteSink, 03 §12.2), the inline
//! guard ([`guard`], 03 §4.3), and the service/ticker assembly.
//!
//! M5b delivered (§M5b 实际交付): the hierarchical namespace ([`ns_hier`],
//! 03 §6) sharing the head/segment + delq model via [`ns_common`]; the
//! [`MemEngine`](store::MemEngine) as an equivalence-tested second engine
//! (03 §7); partition split (a deterministic parent-log op + async child
//! derivation, 03 §2) and migrate ([`raft::migrate`], AddLearner→Promote→
//! RemovePeer) with the streaming range export; and the orphan-sentinel sweep.

pub mod deleter;
pub mod error;
pub mod gc_export;
pub mod guard;
pub mod ns_common;
pub mod ns_flat;
pub mod ns_hier;
pub mod partition;
pub mod raft;
pub mod ref_extractor;
pub mod service;
pub mod store;
pub mod ticker;

pub use error::MetaError;
