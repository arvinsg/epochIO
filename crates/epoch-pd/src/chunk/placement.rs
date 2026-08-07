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

//! Chunk placement: choosing which disk hosts each shard of a new chunk.
//!
//! Pure and side-effect free (directly unit-testable): the PD leader gathers
//! live topology + heartbeat watermarks into [`DiskCandidate`]s and calls
//! [`plan_placement`] to pick one disk per shard slot. The result feeds the
//! [`CreateChunkStaging`](super::model::CreateChunkStaging) plan.
//!
//! Policy (design 01 §4.1): shards land on the lowest-watermark disks first
//! (fewest writable extents), so a freshly added disk — watermark 0 — is filled
//! first and new writes are drawn onto it. Placement also enforces topology
//! anti-affinity so a single fault domain cannot hold two shards of one EC
//! stripe.
//!
//! Design: docs/design/01-pd.md §4.1 (watermark-driven creation, anti-affinity)

use epoch_proto::{DiskId, NodeId};

/// A disk eligible to host a shard, with the topology and watermark placement
/// weighs. Built by the leader from `Normal` disks plus their latest heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskCandidate {
    /// Disk identity.
    pub disk_id: DiskId,
    /// Owning node (host anti-affinity domain).
    pub node_id: NodeId,
    /// Rack (rack anti-affinity domain).
    pub rack: String,
    /// Writable extents reported by the latest heartbeat (the watermark; lower
    /// is preferred).
    pub writable_extents: u32,
}

/// How far apart placement must spread the shards of one chunk (design 01 §4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AntiAffinity {
    /// At most one shard per node.
    HostAware,
    /// At most one shard per rack (implies host-aware).
    RackAware,
}

/// Chooses one disk per shard slot for a chunk of `shards_total` shards.
///
/// Returns the chosen disks in slot order (slot `i` → `result[i]`), or `None`
/// if the candidates cannot satisfy `shards_total` under `affinity` (the caller
/// leaves the watermark unfilled this cycle and retries once topology changes).
///
/// Selection is deterministic: candidates are ordered by ascending watermark,
/// then by `disk_id` to break ties stably (AGENTS §8 — no reliance on input or
/// hash-map ordering), and taken greedily while respecting anti-affinity.
#[must_use]
pub fn plan_placement(
    candidates: &[DiskCandidate],
    shards_total: u16,
    affinity: AntiAffinity,
) -> Option<Vec<DiskId>> {
    let mut ordered: Vec<&DiskCandidate> = candidates.iter().collect();
    ordered.sort_by(|a, b| {
        a.writable_extents
            .cmp(&b.writable_extents)
            .then_with(|| a.disk_id.get().cmp(&b.disk_id.get()))
    });

    let mut chosen = Vec::with_capacity(usize::from(shards_total));
    let mut used_nodes: Vec<NodeId> = Vec::new();
    let mut used_racks: Vec<&str> = Vec::new();

    for candidate in ordered {
        if used_nodes.contains(&candidate.node_id) {
            continue;
        }
        if affinity == AntiAffinity::RackAware && used_racks.contains(&candidate.rack.as_str()) {
            continue;
        }
        chosen.push(candidate.disk_id);
        used_nodes.push(candidate.node_id);
        used_racks.push(&candidate.rack);
        if chosen.len() == usize::from(shards_total) {
            return Some(chosen);
        }
    }

    None
}

/// One shard slot's fault domains, as resolved from committed state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotDomain {
    /// The slot's index within its chunk.
    pub index: u8,
    /// The node hosting the slot's disk.
    pub node_id: NodeId,
    /// The rack of the slot's disk.
    pub rack: String,
}

/// Whether moving slot `index` onto `target` keeps the chunk's shards spread
/// across fault domains (design 01 §4.1 anti-affinity).
///
/// `others` must list the chunk's *other* slots (the moving slot excluded), each
/// with the node/rack resolved from committed state. Returns `true` when the move
/// is safe.
///
/// INVARIANT(design 01 §4.1): anti-affinity is a property of the chunk, not of
/// the creation path. Repair and migration rebind slots too, and a rebind that
/// collocates two shards of one EC stripe on a single node silently converts
/// erasure-coded redundancy into a correlated-failure risk — the chunk still
/// reads fine, so nothing surfaces it until that one node dies and takes two
/// shards with it. Every rebind therefore passes through this check, not just
/// [`plan_placement`].
///
/// A move that leaves the slot on its current disk (`others` cannot contain it)
/// is always allowed, so an in-place rebuild is never blocked.
#[must_use]
pub fn rebind_preserves_affinity(
    others: &[SlotDomain],
    target: &DiskCandidate,
    affinity: AntiAffinity,
) -> bool {
    if others.iter().any(|slot| slot.node_id == target.node_id) {
        return false;
    }
    if affinity == AntiAffinity::RackAware && others.iter().any(|slot| slot.rack == target.rack) {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(disk: u32, node: u32, rack: &str, writable: u32) -> DiskCandidate {
        DiskCandidate {
            disk_id: DiskId::new(disk),
            node_id: NodeId::new(node),
            rack: rack.to_string(),
            writable_extents: writable,
        }
    }

    #[test]
    fn prefers_lowest_watermark_disks() {
        // Four disks on distinct nodes; the two lowest-watermark win, in
        // watermark order.
        let candidates = [
            candidate(1, 1, "r1", 8),
            candidate(2, 2, "r2", 0),
            candidate(3, 3, "r3", 3),
            candidate(4, 4, "r4", 5),
        ];
        let chosen = plan_placement(&candidates, 2, AntiAffinity::HostAware).expect("plan");
        assert_eq!(chosen, vec![DiskId::new(2), DiskId::new(3)]);
    }

    #[test]
    fn ties_break_by_disk_id() {
        let candidates = [
            candidate(5, 1, "r1", 4),
            candidate(2, 2, "r2", 4),
            candidate(9, 3, "r3", 4),
        ];
        let chosen = plan_placement(&candidates, 2, AntiAffinity::HostAware).expect("plan");
        assert_eq!(chosen, vec![DiskId::new(2), DiskId::new(5)]);
    }

    #[test]
    fn host_aware_rejects_two_shards_on_one_node() {
        // Two disks but on the same node: host-aware cannot place 2 shards.
        let candidates = [candidate(1, 1, "r1", 0), candidate(2, 1, "r1", 0)];
        assert_eq!(
            plan_placement(&candidates, 2, AntiAffinity::HostAware),
            None
        );
        // One shard is fine.
        assert_eq!(
            plan_placement(&candidates, 1, AntiAffinity::HostAware),
            Some(vec![DiskId::new(1)])
        );
    }

    #[test]
    fn host_aware_spreads_across_nodes() {
        let candidates = [
            candidate(1, 1, "r1", 0),
            candidate(2, 1, "r1", 0),
            candidate(3, 2, "r2", 1),
        ];
        // Slot 0 picks the lowest watermark on node 1; slot 1 must skip the
        // other node-1 disk and take node 2.
        let chosen = plan_placement(&candidates, 2, AntiAffinity::HostAware).expect("plan");
        assert_eq!(chosen, vec![DiskId::new(1), DiskId::new(3)]);
    }

    #[test]
    fn rack_aware_rejects_two_shards_in_one_rack() {
        // Distinct nodes but same rack: rack-aware cannot place 2 shards.
        let candidates = [candidate(1, 1, "r1", 0), candidate(2, 2, "r1", 0)];
        assert_eq!(
            plan_placement(&candidates, 2, AntiAffinity::RackAware),
            None
        );
        // Host-aware only cares about nodes, so it succeeds.
        assert_eq!(
            plan_placement(&candidates, 2, AntiAffinity::HostAware),
            Some(vec![DiskId::new(1), DiskId::new(2)])
        );
    }

    #[test]
    fn rack_aware_spreads_across_racks() {
        let candidates = [
            candidate(1, 1, "r1", 0),
            candidate(2, 2, "r1", 0),
            candidate(3, 3, "r2", 2),
        ];
        let chosen = plan_placement(&candidates, 2, AntiAffinity::RackAware).expect("plan");
        assert_eq!(chosen, vec![DiskId::new(1), DiskId::new(3)]);
    }

    #[test]
    fn insufficient_candidates_yields_none() {
        let candidates = [candidate(1, 1, "r1", 0)];
        assert_eq!(
            plan_placement(&candidates, 3, AntiAffinity::HostAware),
            None
        );
        assert_eq!(plan_placement(&[], 1, AntiAffinity::HostAware), None);
    }

    fn domain(index: u8, node: u32, rack: &str) -> SlotDomain {
        SlotDomain {
            index,
            node_id: NodeId::new(node),
            rack: rack.to_string(),
        }
    }

    #[test]
    fn rebind_rejects_collocating_two_shards_on_one_node() {
        // Slots 1 and 2 sit on nodes 2 and 3; rebuilding slot 0 onto node 2
        // would put two shards of this chunk on one host.
        let others = [domain(1, 2, "r2"), domain(2, 3, "r3")];
        assert!(
            !rebind_preserves_affinity(&others, &candidate(9, 2, "r2", 0), AntiAffinity::HostAware),
            "a rebind onto an occupied node must be rejected"
        );
        assert!(
            rebind_preserves_affinity(&others, &candidate(9, 4, "r4", 0), AntiAffinity::HostAware),
            "a rebind onto a free node is allowed"
        );
    }

    #[test]
    fn rebind_rack_aware_rejects_a_second_shard_in_one_rack() {
        let others = [domain(1, 2, "r2")];
        let same_rack_other_node = candidate(9, 5, "r2", 0);
        assert!(
            rebind_preserves_affinity(&others, &same_rack_other_node, AntiAffinity::HostAware),
            "host-aware only constrains nodes"
        );
        assert!(
            !rebind_preserves_affinity(&others, &same_rack_other_node, AntiAffinity::RackAware),
            "rack-aware rejects a second shard in the same rack"
        );
    }

    #[test]
    fn rebind_in_place_is_always_allowed() {
        // `others` excludes the moving slot, so rebuilding onto its own disk (the
        // repair coordinator's data-affinity target) never trips the check.
        let others = [domain(1, 2, "r2"), domain(2, 3, "r3")];
        assert!(rebind_preserves_affinity(
            &others,
            &candidate(1, 1, "r1", 0),
            AntiAffinity::RackAware
        ));
    }

    #[test]
    fn rebind_on_a_single_shard_chunk_has_no_constraint() {
        assert!(rebind_preserves_affinity(
            &[],
            &candidate(1, 1, "r1", 0),
            AntiAffinity::RackAware
        ));
    }
}
