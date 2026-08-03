//! Writer session liveness: deriving which `Live` tokens to retire from their
//! owning gateway's heartbeat staleness (design 01 §4.3).
//!
//! Pure decision logic (no I/O, no clock read): the leader ticker supplies the
//! live sessions, a staleness lookup over the heartbeat tracker, and the
//! threshold, and proposes the resulting [`MarkWriterDead`] commands. Kept
//! separate from the node liveness sweep so the writer domain stays self
//! contained; the running ticker that drives it lands with the PD node role.
//!
//! INVARIANT(design 01 §4.3 / AGENTS §8): staleness is read only on the leader
//! to decide *when* to propose retirement; the wall clock never enters a
//! replicated apply.
//!
//! Design: docs/design/01-pd.md §4.3

use epoch_proto::{NodeId, WriterToken};

use crate::writer::model::MarkWriterDead;

/// Default session grace: a token whose gateway heartbeat has lapsed this long
/// is retired (design 01 §4.3 uses 90s).
pub const DEFAULT_WRITER_DEAD_AFTER_MILLIS: u64 = 90_000;

/// The tokens to retire this sweep: every live session whose owning node has
/// been *seen* but whose heartbeat is now staler than `dead_after_millis`.
///
/// A never-observed node (staleness [`u64::MAX`]) is intentionally skipped: its
/// token is not force-retired on a session that never started, and a truly
/// absent node is handled by node liveness (its status decays independently).
/// This keeps a freshly registered token from being retired before its gateway
/// has had a chance to heartbeat.
pub(crate) fn dead_writer_plan(
    live: &[(WriterToken, NodeId)],
    staleness: impl Fn(NodeId) -> u64,
    dead_after_millis: u64,
) -> Vec<MarkWriterDead> {
    live.iter()
        .filter(|(_, node_id)| {
            let stale = staleness(*node_id);
            stale != u64::MAX && stale > dead_after_millis
        })
        .map(|(token, _)| MarkWriterDead { token: *token })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    const DEAD_AFTER: u64 = 90_000;

    fn sessions(tokens_nodes: &[(u32, u32)]) -> Vec<(WriterToken, NodeId)> {
        tokens_nodes
            .iter()
            .map(|&(t, n)| (WriterToken::new(t), NodeId::new(n)))
            .collect()
    }

    fn staleness_map(entries: &[(u32, u64)]) -> impl Fn(NodeId) -> u64 + '_ {
        let map: HashMap<u32, u64> = entries.iter().copied().collect();
        move |node_id: NodeId| map.get(&node_id.get()).copied().unwrap_or(u64::MAX)
    }

    #[test]
    fn retires_only_seen_and_stale_sessions() {
        // token 1 (node 1): fresh heartbeat -> kept.
        // token 2 (node 2): stale past threshold -> retired.
        // token 3 (node 3): never seen (MAX) -> skipped (not force-retired).
        // token 4 (node 2): shares the stale node -> retired.
        let live = sessions(&[(1, 1), (2, 2), (3, 3), (4, 2)]);
        let staleness = staleness_map(&[(1, 1_000), (2, DEAD_AFTER + 1)]);

        let plan = dead_writer_plan(&live, &staleness, DEAD_AFTER);
        let retired: Vec<u32> = plan.iter().map(|c| c.token.get()).collect();
        assert_eq!(retired, vec![2, 4]);
    }

    #[test]
    fn boundary_staleness_is_still_live() {
        let live = sessions(&[(1, 1)]);
        let staleness = staleness_map(&[(1, DEAD_AFTER)]); // exactly at threshold
        assert!(dead_writer_plan(&live, &staleness, DEAD_AFTER).is_empty());
    }
}
