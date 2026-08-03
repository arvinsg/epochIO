//! Console end-to-end (08 §4/§5, M9 首轮): drives the PD-hosted Web Console over
//! real HTTP against an in-process PD replica. Covers the auth + read pipeline:
//! unauthenticated reads are refused, login issues a session cookie, cookie'd
//! reads return the leader view, and the overview counts reconcile with the
//! cluster's actual PD state. The static login page loads without a session.
//!
//! Role gating (readonly → 403 on admin writes) is not exercised: the M9 first
//! round is read-only (all write endpoints are deferred, 08 §5/§8), so there is
//! no admin-only endpoint to reject yet. The session already carries the role;
//! the 403 test lands with the first write endpoint.
//!
//! 307 (follower → leader) is likewise not exercised here: a single-replica PD
//! is always leader. The redirect logic has unit coverage; a multi-replica 307
//! e2e is a follow-up when the harness can pin a non-leader replica.

use std::time::Duration;

use epoch_client::PdClient;
use epoch_node::roles;
use epoch_node::{
    ChunkSpec, ClusterConfig, CodeSpec, GcSpec, MetaSpec, NodeSpec, PdSpec, QosSpec, SchedulerSpec,
    WriterSpec,
};
use epoch_proto::grpc::pd::{ConsoleRole, MetaEngine, NsMode};
use tempfile::TempDir;
use tokio::task::JoinHandle;

const CLUSTER: &str = "00c0ffee";

fn free_port() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
}

/// The console HTTP port = the PD gRPC port + 2000 (mirrors
/// `epoch_pd::console::CONSOLE_PORT_OFFSET`; kept as a literal so the test fails
/// loudly if the offset ever changes without updating the e2e).
const CONSOLE_PORT_OFFSET: u16 = 2000;

fn console_base(pd_addr: &str) -> String {
    let addr: std::net::SocketAddr = pd_addr.parse().expect("pd addr");
    // Mirrors `console::console_addr`: the offset applies downward when adding
    // would leave the u16 range, so a high OS-assigned port still yields a
    // bindable console port (plain wrapping produced a privileged one).
    let port = addr
        .port()
        .checked_add(CONSOLE_PORT_OFFSET)
        .unwrap_or_else(|| addr.port() - CONSOLE_PORT_OFFSET);
    format!("http://127.0.0.1:{port}")
}

struct PdOnly {
    tasks: Vec<JoinHandle<()>>,
    pd_endpoints: Vec<String>,
    _dirs: Vec<TempDir>,
}

impl PdOnly {
    /// Starts a single-replica PD (always leader) with an auto-assigned port.
    async fn start() -> Self {
        let _ = epoch_telemetry::logging::init(epoch_telemetry::logging::LogFormat::Pretty, "info");
        let addr = free_port().to_string();
        let dir = tempfile::tempdir().expect("pd dir");
        let pdnodes = vec![PdSpec {
            id: 1,
            addr: addr.clone(),
            dir: dir.path().to_path_buf(),
        }];
        let config = ClusterConfig {
            cluster_id: CLUSTER.to_string(),
            extent_size: 8 * 1024 * 1024,
            pd: vec![addr.clone()],
            pdnodes,
            nodes: Vec::<NodeSpec>::new(),
            code: CodeSpec {
                data: 2,
                parity: 1,
                stripe_size: 4096,
                blob_size: 64 * 1024,
            },
            writer: WriterSpec { token: 1 },
            chunks: Vec::<ChunkSpec>::new(),
            meta: MetaSpec::default(),
            scheduler: SchedulerSpec::default(),
            qos: QosSpec::default(),
            gc: GcSpec::default(),
            maintenance: Default::default(),
        };
        let mut tasks = Vec::new();
        let cfg = config.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(err) = roles::pd::run(&cfg, 1, std::future::pending()).await {
                tracing::error!(error = %err, "pd role exited");
            }
        }));
        // Let the replica bind + bootstrap + the console server come up.
        tokio::time::sleep(Duration::from_millis(800)).await;
        Self {
            tasks,
            pd_endpoints: vec![addr],
            _dirs: vec![dir],
        }
    }

    fn pd_client(&self) -> PdClient {
        PdClient::connect(&self.pd_endpoints).expect("pd client")
    }

    fn console_base(&self) -> String {
        console_base(&self.pd_endpoints[0])
    }

    fn stop(self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// A reqwest client that does NOT follow redirects (so a 307 is observable) and
/// does NOT auto-manage cookies (the test sets the session cookie by hand).
fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("http client")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_login_cookie_and_leader_reads() {
    let cluster = PdOnly::start().await;
    let base = cluster.console_base();
    let http = http();

    // Seed an admin credential + a couple of buckets to reconcile counts against.
    let pd = cluster.pd_client();
    pd.put_credential("AKIAADMIN", "s3cr3t", None, ConsoleRole::Admin)
        .await
        .expect("put admin credential");
    pd.create_bucket("alpha", NsMode::Flat, None, 1, MetaEngine::Rocks)
        .await
        .expect("bucket alpha");
    pd.create_bucket("beta", NsMode::Hier, None, 1, MetaEngine::Rocks)
        .await
        .expect("bucket beta");

    // 1. The static login page loads without any session.
    let index = http
        .get(format!("{base}/console"))
        .send()
        .await
        .expect("GET /console");
    assert_eq!(index.status(), 200, "login page must load unauthenticated");
    let body = index.text().await.expect("index body");
    assert!(
        body.contains("epochio_console") || body.contains("login"),
        "index should carry the console app"
    );

    // 2. An API read without a session is 401.
    let unauth = http
        .get(format!("{base}/api/v1/overview"))
        .send()
        .await
        .expect("GET overview unauth");
    assert_eq!(unauth.status(), 401, "reads require a session");

    // 3. Login with a wrong secret is rejected.
    let bad = http
        .post(format!("{base}/api/v1/login"))
        .body("AKIAADMIN\nwrong")
        .send()
        .await
        .expect("POST login bad");
    assert_eq!(bad.status(), 401, "wrong secret is rejected");

    // 4. Login with the right AK/SK sets a session cookie.
    let ok = http
        .post(format!("{base}/api/v1/login"))
        .body("AKIAADMIN\ns3cr3t")
        .send()
        .await
        .expect("POST login ok");
    assert_eq!(ok.status(), 200, "valid credentials log in");
    let cookie = ok
        .headers()
        .get(reqwest::header::SET_COOKIE)
        .expect("set-cookie present")
        .to_str()
        .expect("cookie str")
        .split(';')
        .next()
        .expect("cookie pair")
        .to_string();
    assert!(cookie.starts_with("epochio_console="), "session cookie set");

    // 5. A cookie'd overview read returns the leader view; counts reconcile with
    //    the actual PD state (2 buckets: 1 flat + 1 hier).
    let overview_body = http
        .get(format!("{base}/api/v1/overview"))
        .header(reqwest::header::COOKIE, &cookie)
        .send()
        .await
        .expect("GET overview")
        .text()
        .await
        .expect("overview body");
    let overview: serde_json::Value = serde_json::from_str(&overview_body).expect("overview json");
    assert_eq!(overview["buckets"]["total"], 2, "two buckets created");
    assert_eq!(overview["buckets"]["flat"], 1);
    assert_eq!(overview["buckets"]["hier"], 1);

    // 6. The keys endpoint lists the credential WITHOUT its secret (red line).
    let keys_resp = http
        .get(format!("{base}/api/v1/keys"))
        .header(reqwest::header::COOKIE, &cookie)
        .send()
        .await
        .expect("GET keys");
    assert_eq!(keys_resp.status(), 200);
    let keys_body = keys_resp.text().await.expect("keys body");
    assert!(keys_body.contains("AKIAADMIN"), "access key is listed");
    assert!(
        !keys_body.contains("s3cr3t"),
        "the secret must never appear in the console API (08 §4.1 red line)"
    );

    // 7. Logout revokes the session; the next read is 401 again.
    let logout = http
        .post(format!("{base}/api/v1/logout"))
        .header(reqwest::header::COOKIE, &cookie)
        .send()
        .await
        .expect("POST logout");
    assert_eq!(logout.status(), 200);
    let after = http
        .get(format!("{base}/api/v1/overview"))
        .header(reqwest::header::COOKIE, &cookie)
        .send()
        .await
        .expect("GET overview after logout");
    assert_eq!(after.status(), 401, "session revoked by logout");

    cluster.stop();
}
