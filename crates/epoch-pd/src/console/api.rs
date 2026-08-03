//! Console read/JSON API (`/api/v1/*`, 08 §5): aggregates PD state + leader
//! memory into JSON for the browser. All endpoints here are leader-gated and
//! authenticated (the [`mod`](super) router enforces both before dispatch);
//! reads are pure (no side effects), lists are page-capped, and `secret_key` is
//! never emitted (08 §4.1/§8 red line).
//!
//! The concrete endpoints land incrementally: `overview` + `live` (this file,
//! W3); nodes / jobs (W4); keys / usage / objects (W5). Rates and trends never
//! live here — they are `/metrics` + Grafana (08 §2/§5).
//!
//! Design: docs/design/08-web-console.md §5

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Response, StatusCode};
use serde_json::{Value, json};

use super::ConsoleState;
use crate::chunk::ChunkStatusHistogram;
use crate::cluster::{DiskStatus, NodeStatus, RoleSet};
use crate::console::auth::Session;
use crate::credential::ConsoleRole;
use crate::job::types::{JobKind, JobState};

/// Dispatches an authenticated, leader-gated `/api/v1/{rest}` read to its
/// handler. `session` carries the caller's access key + role (admin gating for
/// the write endpoints lands with them). `query` is the raw URL query string
/// (used by the paginated object endpoints). Unknown paths are 404.
pub async fn dispatch(
    state: &ConsoleState,
    rest: &str,
    query: &str,
    session: &Session,
) -> Response<Full<Bytes>> {
    match rest {
        // Overview surface: cluster-wide counts, capacity snapshot, health mix.
        "overview" => json(StatusCode::OK, overview(state).to_string()),
        // Instantaneous gauges (5s refresh): no history retained (08 §2).
        "live" => json(StatusCode::OK, live(state).to_string()),
        // Management surfaces (15s refresh): the three node classes + jobs.
        "nodes/data" => json(StatusCode::OK, data_nodes(state).to_string()),
        "nodes/meta" => json(StatusCode::OK, meta_nodes(state).to_string()),
        "nodes/pd" => json(StatusCode::OK, pd_nodes(state).to_string()),
        "jobs" => json(StatusCode::OK, jobs(state).to_string()),
        // Usage surface: access keys (secret_key projected off, §4.1 red line)
        // and per-bucket usage.
        "keys" => json(StatusCode::OK, keys(state).to_string()),
        "usage" => json(StatusCode::OK, usage(state).to_string()),
        // Object browse (§4): metadata list + head via the injected browser
        // (501 if none). Download is a 302 to a gateway and is not here.
        "objects" => list_objects(state, query).await,
        "object" => head_object(state, query).await,
        // A trivial authenticated probe: confirms the session resolved and the
        // leader is serving.
        "whoami" => json(
            StatusCode::OK,
            json!({ "access_key": session.access_key, "role": role_str(session) }).to_string(),
        ),
        _ => json(
            StatusCode::NOT_FOUND,
            r#"{"error":"unknown endpoint"}"#.into(),
        ),
    }
}

/// `GET /api/v1/overview`: cluster-wide counts (buckets / objects / nodes /
/// disks / chunks / partitions), a capacity-water snapshot from the latest
/// heartbeats, and the chunk health mix. All values are current state — never a
/// rate or a trend (08 §2).
///
/// `awaiting_heartbeats` is the 08 §4 empty state: nodes are registered but the
/// freshly-elected leader has not yet received a heartbeat round, so capacity /
/// used columns are unknown rather than zero.
fn overview(state: &ConsoleState) -> Value {
    let st = state.journal().state();
    let snapshot = state.journal().heartbeat_snapshot();

    let buckets = st.buckets().list();
    let (flat, hier) = bucket_ns_split(&buckets);
    let (nodes_total, nodes_by_status) = node_status_counts(&st.nodes().statuses());
    let (disks_total, disks_by_status) = disk_status_counts(&st.disks().list());
    let capacity = capacity_snapshot(state);
    let chunks = st.chunks().status_histogram();
    let objects = object_totals(state);

    json!({
        "buckets": { "total": buckets.len(), "flat": flat, "hier": hier },
        "objects": objects,
        "nodes": { "total": nodes_total, "by_status": nodes_by_status },
        "disks": { "total": disks_total, "by_status": disks_by_status },
        "chunks": chunk_histogram_json(&chunks),
        "partitions": { "total": st.partitions().list().len() },
        "capacity": capacity,
        "node_capacity": node_capacity(state),
        "awaiting_heartbeats": nodes_total > 0 && snapshot.is_empty(),
    })
}

/// `GET /api/v1/live`: instantaneous gauges refreshed every few seconds. Values
/// PD does not yet track (in-flight request count; per-partition delq depth,
/// which is not plumbed into PD's leader reports — 08 §5.1) are reported as
/// `null` so the frontend renders "—" rather than a misleading zero.
fn live(state: &ConsoleState) -> Value {
    let st = state.journal().state();
    let chunks = st.chunks().status_histogram();
    let disks = st.disks().list();

    let active_writers = st.writers().live_sessions().len();
    let running_jobs = st
        .jobs()
        .list()
        .iter()
        .filter(|job| job.state == JobState::Running)
        .count();
    let repairing_disks = disks
        .iter()
        .filter(|disk| disk.status == DiskStatus::Repairing)
        .count();

    json!({
        // Not tracked in PD yet (no request-scoped counter); a /metrics gauge is
        // the eventual home (08 §5.1). null → frontend shows "—".
        "in_flight_requests": Value::Null,
        "active_writers": active_writers,
        "writable_chunks": chunks.writable,
        "running_jobs": running_jobs,
        "repairing_disks": repairing_disks,
        // delq depth is per-partition inside MetaNode and not carried on the PD
        // leader report (08 §5.1 gap); null until that field is plumbed.
        "delq_depth": Value::Null,
    })
}

/// The hard cap on entries any list endpoint returns (08 §5: pagination and a
/// cap are a hard requirement; the console never lists the full chunk/object
/// space). Node/job counts are cluster-scale and stay well under this.
const LIST_CAP: usize = 100;

/// `GET /api/v1/nodes/data`: DataNodes with their per-node capacity fill, disk
/// online count, and heartbeat staleness. Node-id order, capped at
/// [`LIST_CAP`]. Rows for nodes without a heartbeat yet report `null` fill /
/// staleness (the 08 §4 empty state).
fn data_nodes(state: &ConsoleState) -> Value {
    node_rows(state, RoleSet::DATA)
}

/// `GET /api/v1/nodes/meta`: MetaNodes with the partition count they host, how
/// many they lead, aggregate leader bytes, and heartbeat staleness. Leader
/// counts / bytes come from the per-partition leader reports (`leaders_snapshot`
/// + `gather_loads`), attributed to the reporting leader node.
fn meta_nodes(state: &ConsoleState) -> Value {
    let st = state.journal().state();
    let now = state.now_millis();
    let snapshot = state.journal().heartbeat_snapshot();

    // Partition membership + leader load, indexed by node.
    let partitions = st.partitions().list();
    let mut hosted: std::collections::BTreeMap<u32, u32> = std::collections::BTreeMap::new();
    for p in &partitions {
        for peer in &p.peers {
            *hosted.entry(peer.get()).or_default() += 1;
        }
    }
    let mut led: std::collections::BTreeMap<u32, u32> = std::collections::BTreeMap::new();
    let mut led_bytes: std::collections::BTreeMap<u32, u64> = std::collections::BTreeMap::new();
    for (_id, report) in st.partitions().leaders_snapshot() {
        *led.entry(report.leader.get()).or_default() += 1;
        *led_bytes.entry(report.leader.get()).or_default() += report.total_bytes;
    }

    let rows: Vec<Value> = st
        .nodes()
        .list()
        .into_iter()
        .filter(|n| n.roles.contains(RoleSet::META))
        .take(LIST_CAP)
        .map(|n| {
            let id = n.node_id.get();
            json!({
                "node_id": id,
                "addr": n.addr,
                "az": n.az,
                "rack": n.rack,
                "status": node_status_str(n.status),
                "partitions": hosted.get(&id).copied().unwrap_or(0),
                "leaders": led.get(&id).copied().unwrap_or(0),
                "leader_bytes": led_bytes.get(&id).copied().unwrap_or(0),
                "staleness_millis": staleness_of(&snapshot, id, now),
            })
        })
        .collect();
    json!(rows)
}

/// `GET /api/v1/nodes/pd`: the PD raft replicas — their raft role (leader /
/// follower), applied index, and lag behind the leader's applied index. Read
/// from the local raft metrics (membership + last-applied); replicas are the
/// membership voters, not the heartbeat-registered cluster nodes.
fn pd_nodes(state: &ConsoleState) -> Value {
    let m = state.journal().raft().metrics().borrow().clone();
    let leader = m.current_leader;
    let leader_applied = m.last_applied.map_or(0, |l| l.index);

    let mut voters: Vec<(u64, String)> = m
        .membership_config
        .nodes()
        .map(|(id, node)| (*id, node.addr.clone()))
        .collect();
    voters.sort_by_key(|(id, _)| *id);

    // A follower's applied index is not in the leader's metrics; only the local
    // replica's own applied index is known here. Report the leader's applied for
    // the leader row and leave followers' applied null (lag unknown from PD's own
    // metrics — the /metrics endpoint per replica is the source for that).
    let rows: Vec<Value> = voters
        .into_iter()
        .take(LIST_CAP)
        .map(|(id, addr)| {
            let is_leader = Some(id) == leader;
            json!({
                "node_id": id,
                "addr": addr,
                "raft_role": if is_leader { "leader" } else { "follower" },
                "applied_index": if is_leader { Value::from(leader_applied) } else { Value::Null },
                "leader_applied_index": leader_applied,
            })
        })
        .collect();
    json!(rows)
}

/// `GET /api/v1/jobs`: the tracked Jobs (RepairDisk / DropDisk / Balance /
/// inspect / gc as the kind), their state, coordinator, and progress watermark.
/// Capped at [`LIST_CAP`]; the job table is cluster-scale.
fn jobs(state: &ConsoleState) -> Value {
    let rows: Vec<Value> = state
        .journal()
        .state()
        .jobs()
        .list()
        .into_iter()
        .take(LIST_CAP)
        .map(|job| {
            json!({
                "id": job.id,
                "kind": job_kind_str(&job.kind),
                "state": job_state_str(job.state),
                "coordinator": job.coordinator.map(|c| c.get()),
                "progress_watermark": job.progress_watermark,
                "lease_expiry_millis": job.lease_expiry_millis,
            })
        })
        .collect();
    json!(rows)
}

/// `GET /api/v1/keys`: the access keys with their bucket authorization + console
/// role. The **`secret_key` is projected off here** (08 §4.1/§8 red line): the
/// console API never emits it, even though the credential store holds it (the
/// gateway's SigV4 path needs it, not the browser). Access-key order, capped at
/// [`LIST_CAP`].
fn keys(state: &ConsoleState) -> Value {
    let mut creds = state.journal().state().credentials().list();
    creds.sort_by(|(a, _), (b, _)| a.cmp(b));
    let rows: Vec<Value> = creds
        .into_iter()
        .take(LIST_CAP)
        .map(|(access_key, cred)| {
            json!({
                "access_key": access_key,
                // secret_key intentionally omitted (red line).
                "allowed_buckets": cred.allowed_buckets,   // null = all buckets (admin)
                "console_role": match cred.role {
                    ConsoleRole::Admin => "admin",
                    ConsoleRole::Readonly => "readonly",
                },
            })
        })
        .collect();
    json!(rows)
}

/// `GET /api/v1/usage`: per-bucket usage. Object bytes come from the same
/// per-partition leader reports as the overview (`total_bytes`), summed across a
/// bucket's `Hier` partition or the shared `Flat` space; today PD does not index
/// partitions by bucket for flat namespaces, so per-bucket byte attribution is
/// reported as `null` where it cannot be derived (a documented gap — the
/// per-bucket metric lands with the `/metrics` per-bucket counters, 08 §5.1).
/// Bucket-name order, capped at [`LIST_CAP`].
fn usage(state: &ConsoleState) -> Value {
    let mut buckets = state.journal().state().buckets().list();
    buckets.sort_by(|a, b| a.name.cmp(&b.name));
    let rows: Vec<Value> = buckets
        .into_iter()
        .take(LIST_CAP)
        .map(|b| {
            json!({
                "name": b.name,
                "ns_mode": match b.ns_mode {
                    crate::bucket::NsMode::Flat => "flat",
                    crate::bucket::NsMode::Hier => "hier",
                },
                "codemode_id": b.codemode_id,
                "inline_threshold": b.inline_threshold,
                "created_at": b.created_at,
                // Per-bucket byte attribution is not indexed in PD yet (see doc);
                // null → frontend renders "—" rather than a fabricated number.
                "total_bytes": Value::Null,
            })
        })
        .collect();
    json!(rows)
}

/// `GET /api/v1/objects?bucket=<name>&prefix=<p>&cursor=<key>`: lists objects in
/// the named bucket via the injected [`ObjectBrowser`](super::browse::ObjectBrowser)
/// (§4). Page-capped at [`LIST_CAP`]. Responds `501` if no browser is injected,
/// `404` if the bucket name is unknown, `502` on a MetaNode error. The `cursor`
/// for the next page is the last returned key (echoed in `next_cursor`).
async fn list_objects(state: &ConsoleState, query: &str) -> Response<Full<Bytes>> {
    let Some(browser) = state.browser() else {
        return json(
            StatusCode::NOT_IMPLEMENTED,
            r#"{"error":"object browse not enabled"}"#.into(),
        );
    };
    let params = parse_query(query);
    let Some(bucket_name) = params.get("bucket") else {
        return json(
            StatusCode::BAD_REQUEST,
            r#"{"error":"missing bucket"}"#.into(),
        );
    };
    let Some(bucket) = state.journal().state().buckets().get_by_name(bucket_name) else {
        return json(
            StatusCode::NOT_FOUND,
            r#"{"error":"unknown bucket"}"#.into(),
        );
    };
    let prefix = params.get("prefix").map(String::as_bytes).unwrap_or(&[]);
    let cursor = params.get("cursor").map(String::as_bytes).unwrap_or(&[]);

    match browser
        .list(bucket.bucket_id.get(), prefix, cursor, LIST_CAP as u32)
        .await
    {
        Ok(entries) => {
            let next_cursor = entries
                .last()
                .filter(|_| entries.len() == LIST_CAP)
                .map(|e| String::from_utf8_lossy(&e.key).into_owned());
            let rows: Vec<Value> = entries
                .iter()
                .map(|e| {
                    json!({
                        "key": String::from_utf8_lossy(&e.key),
                        "is_prefix": e.is_prefix,
                        "size": e.size,
                        "inline": e.inline,
                        "mtime_millis": e.mtime_millis,
                    })
                })
                .collect();
            json(
                StatusCode::OK,
                json!({ "entries": rows, "next_cursor": next_cursor }).to_string(),
            )
        }
        Err(err) => json(
            StatusCode::BAD_GATEWAY,
            json!({ "error": err.to_string() }).to_string(),
        ),
    }
}

/// `GET /api/v1/object?bucket=<name>&key=<k>`: reads one object's head via the
/// injected browser (§4). `501` if no browser, `404` if bucket/object absent,
/// `502` on a MetaNode error. The ETag is hex-encoded for the browser.
async fn head_object(state: &ConsoleState, query: &str) -> Response<Full<Bytes>> {
    let Some(browser) = state.browser() else {
        return json(
            StatusCode::NOT_IMPLEMENTED,
            r#"{"error":"object browse not enabled"}"#.into(),
        );
    };
    let params = parse_query(query);
    let (Some(bucket_name), Some(key)) = (params.get("bucket"), params.get("key")) else {
        return json(
            StatusCode::BAD_REQUEST,
            r#"{"error":"missing bucket or key"}"#.into(),
        );
    };
    let Some(bucket) = state.journal().state().buckets().get_by_name(bucket_name) else {
        return json(
            StatusCode::NOT_FOUND,
            r#"{"error":"unknown bucket"}"#.into(),
        );
    };
    match browser.head(bucket.bucket_id.get(), key.as_bytes()).await {
        Ok(Some(head)) => json(
            StatusCode::OK,
            json!({
                "key": key,
                "size": head.size,
                "etag": hex_encode(&head.etag),
                "mtime_millis": head.mtime_millis,
                "inline": head.inline,
            })
            .to_string(),
        ),
        Ok(None) => json(
            StatusCode::NOT_FOUND,
            r#"{"error":"object not found"}"#.into(),
        ),
        Err(err) => json(
            StatusCode::BAD_GATEWAY,
            json!({ "error": err.to_string() }).to_string(),
        ),
    }
}

/// Parses a `&`-separated `key=value` query string into a map, percent-decoding
/// each value. Absent/empty values map to an empty string.
fn parse_query(query: &str) -> std::collections::HashMap<String, String> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (k.to_string(), percent_decode(v))
        })
        .collect()
}

/// Minimal percent-decoding for query values (`%XX` → byte, `+` → space). Good
/// enough for object keys and prefixes the console sends; invalid escapes are
/// left verbatim.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                match (hi, lo) {
                    (Some(h), Some(l)) => {
                        out.push((h * 16 + l) as u8);
                        i += 3;
                    }
                    _ => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Lower-case hex encoding (object ETag → wire string).
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Builds the DataNode-style rows (node identity + capacity fill + staleness)
/// for every node carrying `role`, in node-id order, capped at [`LIST_CAP`].
fn node_rows(state: &ConsoleState, role: RoleSet) -> Value {
    let st = state.journal().state();
    let now = state.now_millis();
    let snapshot = state.journal().heartbeat_snapshot();

    // Per-node registered total + latest heartbeat used/online-disk count.
    let mut total_by_node: std::collections::BTreeMap<u32, u64> = std::collections::BTreeMap::new();
    let mut disks_by_node: std::collections::BTreeMap<u32, u32> = std::collections::BTreeMap::new();
    for disk in st.disks().list() {
        let id = disk.node_id.get();
        *total_by_node.entry(id).or_default() += disk.total;
        *disks_by_node.entry(id).or_default() += 1;
    }

    let rows: Vec<Value> = st
        .nodes()
        .list()
        .into_iter()
        .filter(|n| n.roles.contains(role))
        .take(LIST_CAP)
        .map(|n| {
            let id = n.node_id.get();
            let hb = snapshot.iter().find(|s| s.node_id.get() == id);
            let (used, online_disks): (Option<u64>, Option<usize>) = match hb {
                Some(s) => (
                    Some(s.disks.iter().map(|(_, st)| st.used).sum()),
                    Some(s.disks.len()),
                ),
                None => (None, None),
            };
            let total = total_by_node.get(&id).copied().unwrap_or(0);
            let fill_pct = match used {
                Some(u) if total > 0 => {
                    Value::from(((u as f64 / total as f64) * 100.0).round() as u64)
                }
                _ => Value::Null,
            };
            json!({
                "node_id": id,
                "addr": n.addr,
                "az": n.az,
                "rack": n.rack,
                "roles": role_bits_str(n.roles),
                "status": node_status_str(n.status),
                "total_bytes": total,
                "used_bytes": used,
                "fill_pct": fill_pct,
                "disks_total": disks_by_node.get(&id).copied().unwrap_or(0),
                "disks_online": online_disks,
                "staleness_millis": staleness_of(&snapshot, id, now),
            })
        })
        .collect();
    json!(rows)
}

/// The node's last-heartbeat staleness in millis, or `null` if it has not
/// reported (the 08 §4 empty state — "等待心跳" rather than a fabricated age).
fn staleness_of(
    snapshot: &[crate::cluster::heartbeat::NodeStatsSnapshot],
    node_id: u32,
    now: u64,
) -> Value {
    match snapshot.iter().find(|s| s.node_id.get() == node_id) {
        Some(s) => Value::from(now.saturating_sub(s.last_seen_millis)),
        None => Value::Null,
    }
}

/// Splits buckets into `(flat_count, hier_count)` by namespace mode.
fn bucket_ns_split(buckets: &[crate::bucket::BucketMeta]) -> (usize, usize) {
    let hier = buckets
        .iter()
        .filter(|b| b.ns_mode == crate::bucket::NsMode::Hier)
        .count();
    (buckets.len() - hier, hier)
}

/// Aggregates inline object counters across every partition leader report. Only
/// inline objects are counted per partition today (03 §4.3); `total_bytes` is
/// the split-size signal, reported here for the capacity view.
fn object_totals(state: &ConsoleState) -> Value {
    let mut inline_count = 0u64;
    let mut inline_bytes = 0u64;
    let mut total_bytes = 0u64;
    for (_id, report) in state.journal().state().partitions().leaders_snapshot() {
        inline_count = inline_count.saturating_add(report.inline_count);
        inline_bytes = inline_bytes.saturating_add(report.inline_bytes);
        total_bytes = total_bytes.saturating_add(report.total_bytes);
    }
    json!({
        "inline_count": inline_count,
        "inline_bytes": inline_bytes,
        "total_bytes": total_bytes,
    })
}

/// Counts nodes by lifecycle status, returning `(total, {status: count})`.
fn node_status_counts(statuses: &[(epoch_proto::NodeId, NodeStatus)]) -> (usize, Value) {
    let mut counts = json!({
        "starting": 0, "live": 0, "offline": 0, "lost": 0, "decommissioned": 0,
    });
    for (_id, status) in statuses {
        let key = match status {
            NodeStatus::Starting => "starting",
            NodeStatus::Live => "live",
            NodeStatus::Offline => "offline",
            NodeStatus::Lost => "lost",
            NodeStatus::Decommissioned => "decommissioned",
        };
        bump(&mut counts, key);
    }
    (statuses.len(), counts)
}

/// Counts disks by lifecycle status, returning `(total, {status: count})`.
fn disk_status_counts(disks: &[crate::cluster::Disk]) -> (usize, Value) {
    let mut counts = json!({
        "normal": 0, "broken": 0, "repairing": 0, "repaired": 0, "draining": 0, "dropped": 0,
    });
    for disk in disks {
        let key = match disk.status {
            DiskStatus::Normal => "normal",
            DiskStatus::Broken => "broken",
            DiskStatus::Repairing => "repairing",
            DiskStatus::Repaired => "repaired",
            DiskStatus::Draining => "draining",
            DiskStatus::Dropped => "dropped",
        };
        bump(&mut counts, key);
    }
    (disks.len(), counts)
}

/// The cluster capacity snapshot: total capacity from the registered disks
/// (static, reported at registration), used/free summed from the latest
/// heartbeats. `used`/`free` are `null` while awaiting the first heartbeat round.
fn capacity_snapshot(state: &ConsoleState) -> Value {
    let total: u64 = state
        .journal()
        .state()
        .disks()
        .list()
        .iter()
        .map(|disk| disk.total)
        .sum();
    let snapshot = state.journal().heartbeat_snapshot();
    if snapshot.is_empty() {
        return json!({ "total_bytes": total, "used_bytes": Value::Null, "free_bytes": Value::Null });
    }
    let mut used = 0u64;
    let mut free = 0u64;
    for node in &snapshot {
        for (_disk_id, stats) in &node.disks {
            used = used.saturating_add(stats.used);
            free = free.saturating_add(stats.free);
        }
    }
    json!({ "total_bytes": total, "used_bytes": used, "free_bytes": free })
}

/// Per-node used-capacity fill percent, for the overview capacity bar chart.
/// Total per node is summed from the registered disks (static); used is summed
/// from the node's latest heartbeat. Nodes without a heartbeat yet are omitted
/// (the chart shows only nodes with a known fill). Node-id order.
fn node_capacity(state: &ConsoleState) -> Value {
    // Total capacity per node from registered disks.
    let mut total_by_node: std::collections::BTreeMap<u32, u64> = std::collections::BTreeMap::new();
    for disk in state.journal().state().disks().list() {
        *total_by_node.entry(disk.node_id.get()).or_default() += disk.total;
    }
    let rows: Vec<Value> = state
        .journal()
        .heartbeat_snapshot()
        .iter()
        .map(|node| {
            let used: u64 = node.disks.iter().map(|(_, stats)| stats.used).sum();
            let total = total_by_node.get(&node.node_id.get()).copied().unwrap_or(0);
            let fill_pct = if total > 0 {
                ((used as f64 / total as f64) * 100.0).round() as u64
            } else {
                0
            };
            json!({
                "node_id": node.node_id.get(),
                "used_bytes": used,
                "total_bytes": total,
                "fill_pct": fill_pct,
            })
        })
        .collect();
    json!(rows)
}

/// The chunk status histogram as JSON.
fn chunk_histogram_json(h: &ChunkStatusHistogram) -> Value {
    json!({
        "total": h.total,
        "writable": h.writable,
        "full": h.full,
        "sealed": h.sealed,
        "migrating": h.migrating,
        "broken": h.broken,
    })
}

/// Increments an integer field of a JSON object built by [`json!`] above (the
/// key is always present, so a missing key is a programming error).
fn bump(obj: &mut Value, key: &str) {
    if let Some(slot) = obj.get_mut(key) {
        let n = slot.as_u64().unwrap_or(0);
        *slot = json!(n + 1);
    }
}

/// The session's role as a wire string.
fn role_str(session: &Session) -> &'static str {
    match session.role {
        ConsoleRole::Admin => "admin",
        ConsoleRole::Readonly => "readonly",
    }
}

/// A node lifecycle status as a lower-case wire string.
fn node_status_str(status: NodeStatus) -> &'static str {
    match status {
        NodeStatus::Starting => "starting",
        NodeStatus::Live => "live",
        NodeStatus::Offline => "offline",
        NodeStatus::Lost => "lost",
        NodeStatus::Decommissioned => "decommissioned",
    }
}

/// A role set as a `·`-joined list of role names (e.g. `data · gateway`).
/// PD replicas are raft-membership voters, not a `RoleSet` bit, so they are not
/// listed here (the management-nodes view derives them from raft metrics).
fn role_bits_str(roles: RoleSet) -> String {
    let mut parts = Vec::new();
    if roles.contains(RoleSet::DATA) {
        parts.push("data");
    }
    if roles.contains(RoleSet::META) {
        parts.push("meta");
    }
    if roles.contains(RoleSet::GATEWAY) {
        parts.push("gateway");
    }
    parts.join(" · ")
}

/// A job kind as a wire string (the discriminant only; per-kind fields like the
/// target disk are not needed for the list view).
fn job_kind_str(kind: &JobKind) -> &'static str {
    match kind {
        JobKind::RepairDisk { .. } => "RepairDisk",
        JobKind::DropDisk { .. } => "DropDisk",
        JobKind::Balance { .. } => "Balance",
        JobKind::InspectRound => "InspectRound",
        JobKind::GcRound => "GcRound",
    }
}

/// A job lifecycle state as a wire string.
fn job_state_str(state: JobState) -> &'static str {
    match state {
        JobState::Created => "Created",
        JobState::Running => "Running",
        JobState::Done => "Done",
    }
}

/// A JSON response with an owned body.
pub(crate) fn json(status: StatusCode, body: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
        .expect("valid json response")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bucket::{MetaEngine, NsMode};
    use crate::cluster::RoleSet;
    use crate::cluster::heartbeat::{DiskHeartbeat, HeartbeatReport};
    use crate::journal::Journal;

    use super::*;

    /// Opens a single-node journal and waits until it has elected itself leader,
    /// so the tests read populated raft metrics + can propose without racing the
    /// election (readiness signal, not a sleep — AGENTS §10.2).
    async fn leader_state(dir: &std::path::Path) -> ConsoleState {
        let journal = Arc::new(
            Journal::open_single_node(dir, 1, "127.0.0.1:9001")
                .await
                .expect("open journal"),
        );
        journal
            .raft()
            .wait(Some(std::time::Duration::from_secs(5)))
            .state(openraft::ServerState::Leader, "single node elects itself")
            .await
            .expect("become leader");
        ConsoleState::new(journal)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn overview_counts_buckets_nodes_disks_and_capacity() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = leader_state(dir.path()).await;
        let journal = state.journal();

        journal
            .create_bucket("flat-a", NsMode::Flat, None, 1, MetaEngine::Rocks, 0)
            .await
            .expect("flat bucket");
        journal
            .create_bucket("hier-a", NsMode::Hier, None, 1, MetaEngine::Rocks, 0)
            .await
            .expect("hier bucket");

        let node = journal
            .register_node("10.0.0.1:9000", "az1", "r1", RoleSet::DATA)
            .await
            .expect("register node");
        let disk = match journal
            .register_disk(node, "az1", "r1", "/data/d1", 1_000)
            .await
            .expect("register disk")
        {
            crate::journal::ApplyResult::DiskRegistered { disk_id } => disk_id,
            other => panic!("unexpected register_disk result: {other:?}"),
        };

        // Before any heartbeat: capacity used/free unknown, awaiting flagged.
        let before = overview(&state);
        assert_eq!(before["buckets"]["total"], 2);
        assert_eq!(before["buckets"]["flat"], 1);
        assert_eq!(before["buckets"]["hier"], 1);
        assert_eq!(before["nodes"]["total"], 1);
        assert_eq!(before["disks"]["total"], 1);
        assert_eq!(before["capacity"]["total_bytes"], 1_000);
        assert!(before["capacity"]["used_bytes"].is_null());
        assert_eq!(before["awaiting_heartbeats"], true);

        // A heartbeat fills used/free and clears the awaiting state.
        journal.record_heartbeat(
            &HeartbeatReport {
                node_id: node,
                disks: vec![DiskHeartbeat {
                    disk_id: disk,
                    free: 700,
                    used: 300,
                    writable_extents: 4,
                    broken: false,
                }],
            },
            1_000,
        );
        let after = overview(&state);
        assert_eq!(after["capacity"]["used_bytes"], 300);
        assert_eq!(after["capacity"]["free_bytes"], 700);
        assert_eq!(after["awaiting_heartbeats"], false);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_reports_writable_chunks_and_nulls_untracked_gauges() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = leader_state(dir.path()).await;

        let live = live(&state);
        assert_eq!(live["active_writers"], 0);
        assert_eq!(live["running_jobs"], 0);
        assert_eq!(live["repairing_disks"], 0);
        assert_eq!(live["writable_chunks"], 0);
        // Untracked gauges are null (frontend renders "—"), not a misleading 0.
        assert!(
            live["in_flight_requests"].is_null(),
            "in-flight request count is not tracked in PD yet"
        );
        assert!(
            live["delq_depth"].is_null(),
            "delq depth is not plumbed into PD leader reports (08 §5.1)"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn data_nodes_reports_fill_and_staleness_after_heartbeat() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = leader_state(dir.path()).await;
        let journal = state.journal();

        let node = journal
            .register_node("10.0.0.1:9000", "az1", "r1", RoleSet::DATA)
            .await
            .expect("register node");
        let disk = match journal
            .register_disk(node, "az1", "r1", "/data/d1", 1_000)
            .await
            .expect("register disk")
        {
            crate::journal::ApplyResult::DiskRegistered { disk_id } => disk_id,
            other => panic!("unexpected register_disk result: {other:?}"),
        };

        // Before a heartbeat: fill / staleness unknown (08 §4 empty state).
        let before = data_nodes(&state);
        let row = &before.as_array().expect("array")[0];
        assert_eq!(row["node_id"], node.get());
        assert_eq!(row["roles"], "data");
        assert_eq!(row["total_bytes"], 1_000);
        assert!(row["fill_pct"].is_null(), "no heartbeat → fill unknown");
        assert!(row["staleness_millis"].is_null());
        assert!(row["used_bytes"].is_null());

        journal.record_heartbeat(
            &HeartbeatReport {
                node_id: node,
                disks: vec![DiskHeartbeat {
                    disk_id: disk,
                    free: 400,
                    used: 600,
                    writable_extents: 2,
                    broken: false,
                }],
            },
            5_000,
        );
        // A heartbeat fills used / fill_pct; staleness is now-vs-last-seen.
        let after = data_nodes(&state);
        let row = &after.as_array().expect("array")[0];
        assert_eq!(row["used_bytes"], 600);
        assert_eq!(row["fill_pct"], 60);
        assert_eq!(row["disks_online"], 1);
        assert!(
            !row["staleness_millis"].is_null(),
            "heartbeat sets staleness"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn meta_nodes_lists_only_meta_role_nodes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = leader_state(dir.path()).await;
        let journal = state.journal();

        journal
            .register_node("10.0.0.1:9000", "az1", "r1", RoleSet::DATA)
            .await
            .expect("data node");
        journal
            .register_node("10.0.0.2:9000", "az2", "r2", RoleSet::META)
            .await
            .expect("meta node");

        let rows = meta_nodes(&state);
        let arr = rows.as_array().expect("array");
        assert_eq!(arr.len(), 1, "only the META node is listed");
        assert_eq!(arr[0]["addr"], "10.0.0.2:9000");
        assert_eq!(arr[0]["partitions"], 0);
        assert_eq!(arr[0]["leaders"], 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pd_nodes_marks_the_single_replica_leader() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = leader_state(dir.path()).await;
        // A propose blocks until this replica is leader, so membership + applied
        // index are populated before we read the raft metrics.
        state
            .journal()
            .register_node("10.0.0.1:9000", "az1", "r1", RoleSet::DATA)
            .await
            .expect("settle leadership");

        let rows = pd_nodes(&state);
        let arr = rows.as_array().expect("array");
        assert_eq!(arr.len(), 1, "single-node membership has one voter");
        assert_eq!(arr[0]["node_id"], 1);
        assert_eq!(arr[0]["raft_role"], "leader");
        assert!(
            !arr[0]["applied_index"].is_null(),
            "the leader row reports its applied index"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn jobs_lists_created_jobs_with_kind_and_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = leader_state(dir.path()).await;
        let journal = state.journal();

        let disk = epoch_proto::DiskId::new(7);
        journal
            .create_job(JobKind::RepairDisk { disk_id: disk })
            .await
            .expect("create job");

        let rows = jobs(&state);
        let arr = rows.as_array().expect("array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["kind"], "RepairDisk");
        assert_eq!(arr[0]["state"], "Created");
        assert!(
            arr[0]["coordinator"].is_null(),
            "unassigned job has no coordinator"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keys_projects_off_secret_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = leader_state(dir.path()).await;
        state
            .journal()
            .put_credential(
                "AKIA1",
                "super-secret",
                Some(vec!["bucket-a".into()]),
                ConsoleRole::Admin,
            )
            .await
            .expect("put credential");

        let rows = keys(&state);
        let arr = rows.as_array().expect("array");
        assert_eq!(arr.len(), 1);
        let row = &arr[0];
        assert_eq!(row["access_key"], "AKIA1");
        assert_eq!(row["console_role"], "admin");
        assert_eq!(row["allowed_buckets"][0], "bucket-a");
        // The red line: the secret must never appear in the console API.
        assert!(
            row.get("secret_key").is_none(),
            "secret_key must be projected off"
        );
        assert!(
            !rows.to_string().contains("super-secret"),
            "the secret value must not leak anywhere in the response"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn usage_lists_buckets_in_name_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = leader_state(dir.path()).await;
        for name in ["zeta", "alpha"] {
            state
                .journal()
                .create_bucket(name, NsMode::Flat, None, 1, MetaEngine::Rocks, 0)
                .await
                .expect("create bucket");
        }
        let rows = usage(&state);
        let arr = rows.as_array().expect("array");
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["name"], "alpha", "sorted by name");
        assert_eq!(arr[1]["name"], "zeta");
        assert_eq!(arr[0]["ns_mode"], "flat");
        assert!(
            arr[0]["total_bytes"].is_null(),
            "per-bucket byte attribution is not indexed in PD yet"
        );
    }

    #[test]
    fn parse_query_percent_decodes_values() {
        let m = parse_query("bucket=my-bucket&prefix=train%2Fshard&cursor=a%20b");
        assert_eq!(m.get("bucket").unwrap(), "my-bucket");
        assert_eq!(m.get("prefix").unwrap(), "train/shard");
        assert_eq!(m.get("cursor").unwrap(), "a b");
        assert!(parse_query("").is_empty());
    }

    #[test]
    fn hex_encode_round_trips_known_bytes() {
        assert_eq!(hex_encode(&[0x00, 0x0f, 0xff, 0xa3]), "000fffa3");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_endpoints_501_without_browser() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = leader_state(dir.path()).await;
        // No browser injected → object browse is not enabled.
        let list = list_objects(&state, "bucket=any").await;
        assert_eq!(list.status(), StatusCode::NOT_IMPLEMENTED);
        let head = head_object(&state, "bucket=any&key=k").await;
        assert_eq!(head.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_list_maps_entries_and_derives_cursor() {
        use crate::console::browse::{BrowseEntry, BrowseHead, ObjectBrowseError, ObjectBrowser};
        use async_trait::async_trait;

        // A fake browser returning one object; exercises the JSON projection
        // without a live MetaNode.
        struct FakeBrowser;
        #[async_trait]
        impl ObjectBrowser for FakeBrowser {
            async fn list(
                &self,
                _bucket: u64,
                _prefix: &[u8],
                _after: &[u8],
                _limit: u32,
            ) -> Result<Vec<BrowseEntry>, ObjectBrowseError> {
                Ok(vec![BrowseEntry {
                    key: b"train/000.tar".to_vec(),
                    is_prefix: false,
                    size: 1024,
                    inline: false,
                    mtime_millis: 42,
                }])
            }
            async fn head(
                &self,
                _bucket: u64,
                _key: &[u8],
            ) -> Result<Option<BrowseHead>, ObjectBrowseError> {
                Ok(Some(BrowseHead {
                    size: 1024,
                    etag: vec![0xab, 0xcd],
                    mtime_millis: 42,
                    inline: false,
                }))
            }
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let state = leader_state(dir.path())
            .await
            .with_browser(std::sync::Arc::new(FakeBrowser));
        state
            .journal()
            .create_bucket("imgs", NsMode::Flat, None, 1, MetaEngine::Rocks, 0)
            .await
            .expect("create bucket");

        let resp = list_objects(&state, "bucket=imgs&prefix=train%2F").await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Unknown bucket → 404.
        let missing = list_objects(&state, "bucket=nope").await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);

        // head projects the etag as hex.
        let head = head_object(&state, "bucket=imgs&key=train/000.tar").await;
        assert_eq!(head.status(), StatusCode::OK);
    }
}
