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

//! M6 end-to-end acceptance (07 §M6): a real S3 client that signs SigV4 and
//! speaks HTTP to the *served* gateway — proving the head is genuinely
//! S3-compatible, not just the in-process backend.
//!
//! The harness assembles a full dev cluster in one process — one PD replica,
//! three data nodes (EC 2+1), three MetaNodes, and one gateway serving S3 on a
//! loopback port — then provisions a flat bucket, a hier bucket, a full-range
//! partition per namespace, and a root credential. It then drives the gateway
//! with `reqwest` requests signed by `aws-sigv4`, exercising:
//!
//! - **flat bucket**: PUT (inline small + EC large) / GET / HEAD / LIST /
//!   DELETE round-trip, with S3 MD5 ETag verification;
//! - **hier bucket**: implicit-mkdir deep-path PUT + GET round-trip;
//! - **auth**: an unsigned request is rejected (SigV4 is enforced).
//!
//! Timing: data placement convergence (heartbeat 5s + placement sweep 5s) and
//! partition-leader election (partition heartbeat 5s + PD liveness 5s) each
//! take tens of seconds, so every wait polls with a generous deadline.

use std::time::{Duration, Instant, SystemTime};

use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use epoch_client::PdClient;
use epoch_node::roles;
use epoch_node::{
    ClusterConfig, CodeSpec, GcSpec, MetaSpec, NodeSpec, PdSpec, QosSpec, SchedulerSpec, WriterSpec,
};
use epoch_proto::grpc::pd::pd_control_client::PdControlClient;
use epoch_proto::grpc::pd::{CreatePartitionRequest, NsMode};
use epoch_proto::{CodeModeId, NodeId};
use tempfile::TempDir;
use tokio::task::JoinHandle;

const CLUSTER: &str = "00c0ffee";
const EXTENT_SIZE: u64 = 8 * 1024 * 1024;
const CODE_MODE_ID: u16 = 1;
const STRIPE: usize = 4096;
const BLOB: usize = 64 * 1024;
const REGION: &str = "us-east-1";
const ACCESS_KEY: &str = "epochroot";
const SECRET_KEY: &str = "epochsecret0123456789";

fn free_port() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
}

/// A running in-process dev cluster (PD + data + meta + gateway) plus the S3
/// endpoint the signed client dials.
struct S3Cluster {
    tasks: Vec<JoinHandle<()>>,
    pd_endpoints: Vec<String>,
    s3_addr: std::net::SocketAddr,
    _dirs: Vec<TempDir>,
}

impl S3Cluster {
    /// Boots one PD replica, three data nodes, three MetaNodes, and a gateway,
    /// all on auto-assigned loopback ports and temp dirs.
    async fn start() -> Self {
        let _ = epoch_telemetry::logging::init(epoch_telemetry::logging::LogFormat::Pretty, "info");
        let mut dirs = Vec::new();
        let mut pdnodes = Vec::new();
        let mut pd_endpoints = Vec::new();
        for id in 1..=1u64 {
            let addr = free_port().to_string();
            let dir = tempfile::tempdir().expect("pd dir");
            pd_endpoints.push(addr.clone());
            pdnodes.push(PdSpec {
                id,
                addr,
                dir: dir.path().to_path_buf(),
            });
            dirs.push(dir);
        }

        // Data nodes (ids 1..=3) host disks for EC; each also runs a MetaNode on
        // its derived meta port (data port + 1000).
        let mut nodes = Vec::new();
        for id in 1..=3u32 {
            let dir = tempfile::tempdir().expect("data dir");
            nodes.push(NodeSpec {
                id,
                addr: free_port().to_string(),
                meta_addr: Some(free_port().to_string()),
                disk: dir.path().to_path_buf(),
                az: "az1".to_string(),
                rack: "r1".to_string(),
            });
            dirs.push(dir);
        }

        let config = ClusterConfig {
            cluster_id: CLUSTER.to_string(),
            extent_size: EXTENT_SIZE,
            pd: pd_endpoints.clone(),
            pdnodes,
            nodes,
            code: CodeSpec {
                data: 2,
                parity: 1,
                stripe_size: STRIPE,
                blob_size: BLOB,
            },
            writer: WriterSpec { token: 1 },
            chunks: Vec::new(),
            meta: MetaSpec::default(),
            scheduler: SchedulerSpec::default(),
            qos: QosSpec::default(),
            gc: GcSpec::default(),
            maintenance: Default::default(),
        };

        let mut tasks = Vec::new();
        for spec in &config.pdnodes {
            let config = config.clone();
            let id = spec.id;
            tasks.push(tokio::spawn(async move {
                if let Err(err) = roles::pd::run(&config, id, std::future::pending()).await {
                    tracing::error!(replica = id, error = %err, "pd role exited");
                }
            }));
        }
        for spec in &config.nodes {
            let data_config = config.clone();
            let meta_config = config.clone();
            let id = NodeId::new(spec.id);
            tasks.push(tokio::spawn(async move {
                if let Err(err) = roles::data::run(&data_config, id, std::future::pending()).await {
                    tracing::error!(node = id.get(), error = %err, "data role exited");
                }
            }));
            tasks.push(tokio::spawn(async move {
                if let Err(err) = roles::meta::run(&meta_config, id, std::future::pending()).await {
                    tracing::error!(node = id.get(), error = %err, "meta role exited");
                }
            }));
        }

        let s3_addr = free_port();
        tasks.push(tokio::spawn(async move {
            if let Err(err) = roles::gateway::run(&config, s3_addr, std::future::pending()).await {
                tracing::error!(error = %err, "gateway role exited");
            }
        }));

        let cluster = Self {
            tasks,
            pd_endpoints,
            s3_addr,
            _dirs: dirs,
        };
        tokio::time::sleep(Duration::from_millis(500)).await;
        cluster
    }

    fn pd_client(&self) -> PdClient {
        PdClient::connect(&self.pd_endpoints).expect("pd client")
    }

    fn pd_control(&self) -> PdControlClient<tonic::transport::Channel> {
        let channel =
            tonic::transport::Endpoint::from_shared(format!("http://{}", self.pd_endpoints[0]))
                .expect("endpoint")
                .connect_lazy();
        PdControlClient::new(channel)
    }

    fn stop(self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// Polls `check` until it returns true or the deadline passes.
async fn wait_for<F, Fut>(what: &str, timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if check().await {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Creates a full-range partition in namespace `ns` (mirrors PD's
/// `CreatePartition` control call) and waits for a leader to converge in the
/// route table for `bucket_id`.
async fn provision_partition(cluster: &S3Cluster, ns: NsMode, bucket_id: u64) {
    let mut ctrl = cluster.pd_control();
    ctrl.create_partition(CreatePartitionRequest {
        ns: ns as i32,
        start_bucket: 0,
        start_key: Vec::new(),
        start_unbounded: true,
        end_bucket: 0,
        end_key: Vec::new(),
        end_unbounded: true,
    })
    .await
    .expect("create partition");

    let pd = cluster.pd_client();
    wait_for("partition leader", Duration::from_secs(60), || {
        let pd = pd.clone();
        async move {
            pd.get_route(ns, bucket_id, b"a".to_vec())
                .await
                .map(|view| view.is_some_and(|v| !v.leader_addr.is_empty()))
                .unwrap_or(false)
        }
    })
    .await;
}

/// Signs an S3 request with SigV4 and returns the reqwest request ready to send.
fn signed_request(method: reqwest::Method, url: &str, body: &[u8]) -> reqwest::RequestBuilder {
    signed_request_as(ACCESS_KEY, SECRET_KEY, method, url, body)
}

/// As [`signed_request`], signed by a specific credential (so authorization —
/// not just authentication — can be exercised).
fn signed_request_as(
    access_key: &str,
    secret_key: &str,
    method: reqwest::Method,
    url: &str,
    body: &[u8],
) -> reqwest::RequestBuilder {
    let identity =
        aws_credential_types::Credentials::new(access_key, secret_key, None, None, "e2e").into();
    // s3s requires the `x-amz-content-sha256` header (S3 signs the payload
    // hash), which the signer only emits under `XAmzSha256`.
    let mut settings = SigningSettings::default();
    settings.payload_checksum_kind = aws_sigv4::http_request::PayloadChecksumKind::XAmzSha256;
    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(REGION)
        .name("s3")
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .expect("signing params")
        .into();

    let host = url_host(url);
    let signable = SignableRequest::new(
        method.as_str(),
        url,
        std::iter::once(("host", host.as_str())),
        SignableBody::Bytes(body),
    )
    .expect("signable");
    let (instructions, _sig) = sign(signable, &signing_params).expect("sign").into_parts();

    let client = reqwest::Client::new();
    let mut builder = client
        .request(method, url)
        .header("host", host)
        .body(body.to_vec());
    let (signed_headers, signed_params) = instructions.into_parts();
    for header in signed_headers {
        builder = builder.header(header.name(), header.value());
    }
    debug_assert!(signed_params.is_empty(), "header-based signing only");
    builder
}

/// The `host[:port]` component of a URL (SigV4 signs the Host header).
fn url_host(url: &str) -> String {
    let rest = url.split("://").nth(1).unwrap_or(url);
    rest.split('/').next().unwrap_or(rest).to_string()
}

/// S3 MD5 ETag body (unquoted lowercase hex) for a single-part object.
fn md5_hex(body: &[u8]) -> String {
    use md5::{Digest, Md5};
    let digest = Md5::digest(body);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn s3_client_round_trips_flat_and_hier_buckets() {
    let cluster = S3Cluster::start().await;
    let pd = cluster.pd_client();

    // Data plane converges to PD-driven writable chunks (EC needs them).
    wait_for("writable chunks", Duration::from_secs(60), || {
        let pd = pd.clone();
        async move {
            pd.get_writable_chunks(CodeModeId::new(CODE_MODE_ID))
                .await
                .map(|c| !c.is_empty())
                .unwrap_or(false)
        }
    })
    .await;

    // Provision: a flat bucket (id 1) and a hier bucket (id 2), a full-range
    // partition per namespace, and a root credential the gateway will verify.
    let inline_threshold = 4096u64;
    let flat_id = pd
        .create_bucket(
            "flatb",
            NsMode::Flat,
            Some(inline_threshold),
            CODE_MODE_ID,
            epoch_proto::grpc::pd::MetaEngine::Rocks,
        )
        .await
        .expect("create flat bucket")
        .get();
    let hier_id = pd
        .create_bucket(
            "hierb",
            NsMode::Hier,
            Some(inline_threshold),
            CODE_MODE_ID,
            epoch_proto::grpc::pd::MetaEngine::Rocks,
        )
        .await
        .expect("create hier bucket")
        .get();
    pd.put_credential(
        ACCESS_KEY,
        SECRET_KEY,
        None,
        epoch_proto::grpc::pd::ConsoleRole::Admin,
    )
    .await
    .expect("put credential");
    provision_partition(&cluster, NsMode::Flat, flat_id).await;
    provision_partition(&cluster, NsMode::Hier, hier_id).await;

    let base = format!("http://{}", cluster.s3_addr);

    // --- Auth: an unsigned PUT is rejected (SigV4 enforced). ---
    let unsigned = reqwest::Client::new()
        .put(format!("{base}/flatb/nope"))
        .body(b"x".to_vec())
        .send()
        .await
        .expect("send unsigned");
    assert!(
        unsigned.status() == reqwest::StatusCode::FORBIDDEN
            || unsigned.status() == reqwest::StatusCode::BAD_REQUEST,
        "unsigned request must be rejected, got {}",
        unsigned.status()
    );

    // --- Flat: inline small object round-trip. ---
    let small = b"hello epochIO".to_vec();
    let put = signed_request(
        reqwest::Method::PUT,
        &format!("{base}/flatb/small.txt"),
        &small,
    )
    .send()
    .await
    .expect("put small");
    assert_eq!(put.status(), 200, "put small: {:?}", put.text().await);
    let etag = put_etag(&put);
    assert_eq!(etag, md5_hex(&small), "flat inline etag is S3 MD5");

    let got = signed_request(
        reqwest::Method::GET,
        &format!("{base}/flatb/small.txt"),
        &[],
    )
    .send()
    .await
    .expect("get small");
    assert_eq!(got.status(), 200);
    assert_eq!(got.bytes().await.expect("body").as_ref(), small.as_slice());

    // --- Flat: EC large object (above inline threshold) round-trip. ---
    let large: Vec<u8> = (0..(BLOB + STRIPE)).map(|i| (i % 251) as u8).collect();
    let put = signed_request(
        reqwest::Method::PUT,
        &format!("{base}/flatb/large.bin"),
        &large,
    )
    .send()
    .await
    .expect("put large");
    assert_eq!(put.status(), 200, "put large: {:?}", put.text().await);
    assert_eq!(put_etag(&put), md5_hex(&large), "flat EC etag is S3 MD5");

    let got = signed_request(
        reqwest::Method::GET,
        &format!("{base}/flatb/large.bin"),
        &[],
    )
    .send()
    .await
    .expect("get large");
    assert_eq!(got.status(), 200);
    assert_eq!(got.bytes().await.expect("body").as_ref(), large.as_slice());

    // --- Flat: HEAD reports size + etag. ---
    let head = signed_request(
        reqwest::Method::HEAD,
        &format!("{base}/flatb/large.bin"),
        &[],
    )
    .send()
    .await
    .expect("head large");
    assert_eq!(head.status(), 200);
    assert_eq!(
        head.headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok()),
        Some(large.len()),
        "HEAD content-length"
    );

    // --- Flat: LIST V2 sees both keys. ---
    let list = signed_request(
        reqwest::Method::GET,
        &format!("{base}/flatb?list-type=2"),
        &[],
    )
    .send()
    .await
    .expect("list");
    assert_eq!(list.status(), 200);
    let body = list.text().await.expect("list body");
    assert!(body.contains("small.txt"), "list body: {body}");
    assert!(body.contains("large.bin"), "list body: {body}");

    // --- Flat: DELETE then GET is NoSuchKey. ---
    let del = signed_request(
        reqwest::Method::DELETE,
        &format!("{base}/flatb/small.txt"),
        &[],
    )
    .send()
    .await
    .expect("delete small");
    assert!(del.status().is_success(), "delete small: {}", del.status());
    let gone = signed_request(
        reqwest::Method::GET,
        &format!("{base}/flatb/small.txt"),
        &[],
    )
    .send()
    .await
    .expect("get deleted");
    assert_eq!(gone.status(), 404, "deleted key must be 404");

    // --- Hier: deep-path PUT implicitly makes directories; GET round-trips. ---
    let doc = b"nested checkpoint".to_vec();
    let put = signed_request(
        reqwest::Method::PUT,
        &format!("{base}/hierb/models/v1/ckpt.bin"),
        &doc,
    )
    .send()
    .await
    .expect("put hier");
    assert_eq!(put.status(), 200, "put hier: {:?}", put.text().await);
    let got = signed_request(
        reqwest::Method::GET,
        &format!("{base}/hierb/models/v1/ckpt.bin"),
        &[],
    )
    .send()
    .await
    .expect("get hier");
    assert_eq!(got.status(), 200);
    assert_eq!(got.bytes().await.expect("body").as_ref(), doc.as_slice());

    // --- Hier LIST: readdir renders files as keys, subdirs as CommonPrefixes. ---
    for key in ["models/v1/b.bin", "models/v2/c.bin", "models/top.txt"] {
        let put = signed_request(reqwest::Method::PUT, &format!("{base}/hierb/{key}"), b"x")
            .send()
            .await
            .expect("put hier tree");
        assert_eq!(put.status(), 200, "put {key}");
    }
    let listed = signed_request(
        reqwest::Method::GET,
        &format!("{base}/hierb?list-type=2&prefix=models/&delimiter=/"),
        &[],
    )
    .send()
    .await
    .expect("list hier");
    assert_eq!(listed.status(), 200, "hier LIST must not 503");
    let body = listed.text().await.expect("list body");
    assert!(
        body.contains("<Prefix>models/v1/</Prefix>") || body.contains("models/v1/</Prefix>"),
        "subdirectories collapse into CommonPrefixes: {body}"
    );
    assert!(
        body.contains("models/top.txt"),
        "files in the directory are keys: {body}"
    );
    assert!(
        !body.contains("models/v1/ckpt.bin"),
        "keys *inside* a subdirectory must not be listed at this level: {body}"
    );

    // --- Flat LIST with a delimiter: the directory view `aws s3 ls` relies on. ---
    for key in ["d/1", "d/2", "e/1", "flat-top"] {
        let put = signed_request(reqwest::Method::PUT, &format!("{base}/flatb/{key}"), b"y")
            .send()
            .await
            .expect("put flat tree");
        assert_eq!(put.status(), 200, "put {key}");
    }
    let listed = signed_request(
        reqwest::Method::GET,
        &format!("{base}/flatb?list-type=2&delimiter=/"),
        &[],
    )
    .send()
    .await
    .expect("list flat");
    assert_eq!(listed.status(), 200);
    let body = listed.text().await.expect("list body");
    assert!(body.contains("d/</Prefix>"), "`d/` collapses: {body}");
    assert!(body.contains("e/</Prefix>"), "`e/` collapses: {body}");
    assert!(body.contains("flat-top"), "a bare key stays a key: {body}");
    assert!(
        !body.contains("<Key>d/1</Key>"),
        "keys under a collapsed prefix are not listed individually: {body}"
    );

    // --- IAM-lite: a credential restricted to one bucket cannot touch another. ---
    //
    // This is authorization, not authentication: the request below is correctly
    // signed and authenticates fine. The defect this guards was a check that
    // existed but had no production caller — `allowed_buckets` was configured,
    // looked enforced, and allowed everything.
    const SUB_KEY: &str = "epochsub";
    const SUB_SECRET: &str = "epochsubsecret0123456";
    pd.put_credential(
        SUB_KEY,
        SUB_SECRET,
        Some(vec!["flatb".to_string()]),
        epoch_proto::grpc::pd::ConsoleRole::Readonly,
    )
    .await
    .expect("put sub credential");

    // Its allowed bucket works.
    let allowed = signed_request_as(
        SUB_KEY,
        SUB_SECRET,
        reqwest::Method::GET,
        &format!("{base}/flatb/flat-top"),
        &[],
    )
    .send()
    .await
    .expect("get allowed bucket");
    let allowed_status = allowed.status();
    assert_eq!(
        allowed_status,
        200,
        "the credential's own bucket stays readable: {:?}",
        allowed.text().await
    );

    // The other bucket is denied — 403, not 200 and not 404.
    let denied = signed_request_as(
        SUB_KEY,
        SUB_SECRET,
        reqwest::Method::GET,
        &format!("{base}/hierb/models/top.txt"),
        &[],
    )
    .send()
    .await
    .expect("get denied bucket");
    assert_eq!(
        denied.status(),
        403,
        "a bucket outside the allow-list must be AccessDenied, got {:?}",
        denied.text().await
    );
    // A write is denied too (the gate is on bucket resolution, not on the verb).
    let denied_put = signed_request_as(
        SUB_KEY,
        SUB_SECRET,
        reqwest::Method::PUT,
        &format!("{base}/hierb/sneak.bin"),
        b"nope",
    )
    .send()
    .await
    .expect("put denied bucket");
    assert_eq!(denied_put.status(), 403, "writes are gated as well");

    cluster.stop();
}

/// Extracts the unquoted ETag body from a response's `ETag` header.
fn put_etag(resp: &reqwest::Response) -> String {
    resp.headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim_matches('"').to_string())
        .unwrap_or_default()
}
