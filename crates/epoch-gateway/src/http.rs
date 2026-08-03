//! The S3 HTTP head (M6): implements the [`s3s::S3`] trait over the
//! [`ObjectService`] orchestrator, so a real S3 client (`aws s3`, s3cmd) drives
//! the EC + MetaNode pipeline. `s3s` owns HTTP/XML parsing, SigV4 verification
//! (via [`auth`](crate::auth)), and streaming; this maps each S3 operation to
//! the object service and back.
//!
//! ## Namespace dispatch
//!
//! The bucket's namespace mode (from the [`BucketCache`]) selects the path:
//! flat buckets go straight to [`ObjectService`]; hier buckets route through
//! [`s3compat`](crate::s3compat) (implicit mkdir / dir-file conflict / DFS
//! list, 03 §6.4). M6-3 wires the flat path and the shared verbs; the hier
//! dispatch is added in M6-4.
//!
//! ## Bodies
//!
//! Request bodies stream in bounded by the EC-admission pool (M6-2); a PUT
//! collects up to that bound before EC encoding. Object bodies larger than the
//! pool are a documented follow-up. **Ranged GET is honoured** ([`crate::range`]):
//! only the blobs the window touches are fetched, and the reply is 206 with a
//! matching `Content-Range` — answering a range request with the whole body under
//! 200 silently corrupts every parallel downloader.
//!
//! Design: docs/design/00-overview.md §5; docs/design/03-metanode.md §6.4

use std::time::{SystemTime, UNIX_EPOCH};

use epoch_client::{BucketCache, TokenSource};
use s3s::dto;
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};

use crate::error::to_s3_error;
use crate::object::ObjectService;

/// The maximum object body a single PUT buffers (MVP bound; equals the EC
/// admission unit ceiling — streaming beyond the pool is a follow-up). 5 GiB is
/// the S3 single-PUT maximum, a natural cap.
const MAX_PUT_BYTES: usize = 5 << 30;

/// The S3 backend: the object service plus the bucket cache for namespace and
/// inline-threshold resolution. Generic over the token source so tests can
/// inject a fake; production uses `PdClient`.
pub struct S3Backend<S: TokenSource> {
    objects: ObjectService<S>,
    buckets: BucketCache,
    creds: epoch_client::CredentialCache,
}

impl<S: TokenSource> S3Backend<S> {
    /// Assembles the backend over the object service, bucket cache, and the
    /// credential cache used for bucket-level authorization.
    #[must_use]
    pub fn new(
        objects: ObjectService<S>,
        buckets: BucketCache,
        creds: epoch_client::CredentialCache,
    ) -> Self {
        Self {
            objects,
            buckets,
            creds,
        }
    }

    /// Resolves a bucket **and authorizes the caller for it** (01 §6 IAM-lite).
    ///
    /// INVARIANT(design 01 §6): every operation that names a bucket passes
    /// through here, so authorization cannot be forgotten at a call site. The
    /// defect this replaces was exactly that: `Credential::allows_bucket` existed,
    /// was correct, and had zero production callers — so a sub-account's
    /// `allowed_buckets` list was configured, appeared to work, and enforced
    /// nothing. A silently-inert access check is worse than none, because the
    /// operator believes the isolation is real.
    ///
    /// Resolution and authorization are deliberately one function rather than two
    /// (a separate `authorize()` would be forgettable in the same way).
    async fn bucket(
        &self,
        creds: Option<&s3s::auth::Credentials>,
        name: &str,
    ) -> S3Result<epoch_client::BucketInfo> {
        self.authorize(creds, name).await?;
        match self.buckets.resolve(name).await {
            Ok(info) => Ok(info),
            Err(epoch_client::ClientError::NotFound(_)) => {
                Err(s3_error!(NoSuchBucket, "no such bucket: {name}"))
            }
            Err(e) => Err(s3_error!(InternalError, "bucket lookup: {e}")),
        }
    }

    /// Checks the signed caller may touch `bucket`, per its credential's
    /// allow-list (`None` = all buckets, the root account).
    ///
    /// An unsigned request cannot reach here — `s3s` rejects it during SigV4
    /// verification — but if the auth layer is ever configured away, no
    /// credentials means no authorization, so this denies rather than allows.
    async fn authorize(
        &self,
        creds: Option<&s3s::auth::Credentials>,
        bucket: &str,
    ) -> S3Result<()> {
        let Some(creds) = creds else {
            return Err(s3_error!(AccessDenied, "unauthenticated request"));
        };
        match self.creds.resolve(&creds.access_key).await {
            Ok(Some(cred)) if cred.allows_bucket(bucket) => Ok(()),
            Ok(Some(_)) => Err(s3_error!(
                AccessDenied,
                "credential is not allowed to access bucket {bucket}"
            )),
            // The key verified its signature moments ago, so a miss here means the
            // credential was just revoked: deny.
            Ok(None) => Err(s3_error!(AccessDenied, "unknown access key")),
            Err(e) => Err(s3_error!(InternalError, "authorization lookup: {e}")),
        }
    }

    /// One page of a flat-bucket LIST, with `delimiter` folded into
    /// `CommonPrefixes` ([`crate::list`]).
    ///
    /// Scans in batches until the row budget fills or the key space is exhausted.
    /// A folded group is *skipped* rather than paged through — the fold hands back
    /// the group's upper bound as the next `start_after` — so a prefix holding a
    /// million keys costs one batch, not a million rows (see the `list` module's
    /// INVARIANT).
    async fn list_flat(
        &self,
        bucket_id: u64,
        prefix: &[u8],
        delimiter: &[u8],
        start_after: &[u8],
        limit: u32,
    ) -> S3Result<crate::list::ListPage> {
        // Bounds the work one request can cause when folding collapses a lot of
        // keys into few rows (each batch is one MetaNode round trip).
        const MAX_BATCHES: usize = 16;

        let mut cursor = start_after.to_vec();
        let mut rows: Vec<crate::list::ListRow> = Vec::new();
        let mut next_token: Option<Vec<u8>> = None;
        for _ in 0..MAX_BATCHES {
            let remaining = limit as usize - rows.len();
            let entries = self
                .objects
                .meta()
                .list_objects(bucket_id, prefix, &cursor, limit)
                .await
                .map_err(|e| to_s3_error(crate::error::GatewayError::Meta(e.to_string())))?;
            if entries.is_empty() {
                next_token = None;
                break;
            }
            let batch_exhausted = (entries.len() as u32) < limit;
            let keys: Vec<Vec<u8>> = entries.iter().map(|e| e.key.clone()).collect();
            let folded = crate::list::fold(&keys, prefix, delimiter, remaining);
            for row in folded.rows {
                match row {
                    crate::list::Row::Key(key) => {
                        // Re-attach the entry so size/etag survive the fold.
                        if let Some(entry) = entries.iter().find(|e| e.key == key) {
                            rows.push(crate::list::ListRow::Object(entry.clone()));
                        }
                    }
                    crate::list::Row::CommonPrefix(p) => {
                        rows.push(crate::list::ListRow::Prefix(p));
                    }
                }
            }
            // Where to continue: the fold's skip point when it gave one, else
            // after the last key this batch scanned.
            cursor = folded
                .resume_after
                .unwrap_or_else(|| keys.last().cloned().unwrap_or_default());
            if rows.len() >= limit as usize {
                next_token = Some(cursor);
                break;
            }
            if batch_exhausted {
                // The scan reached the end of the prefix's key space.
                next_token = None;
                break;
            }
            next_token = Some(cursor.clone());
        }
        Ok(crate::list::ListPage { rows, next_token })
    }
}

/// Wall-clock millis for proposer timestamps (the MetaNode replicates these;
/// apply never reads a clock, 03 §8).
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// A `Timestamp` for the current time (S3 creation-date, and the fallback when a
/// record carries no usable mtime).
fn now_ts() -> dto::Timestamp {
    dto::Timestamp::from(SystemTime::now())
}

/// The stored wall-clock millis as an S3 `Timestamp`.
///
/// A non-positive value means "never recorded" (an old or internally-written
/// record) and falls back to now, since S3 requires a last-modified.
fn stored_ts(millis: i64) -> dto::Timestamp {
    if millis <= 0 {
        return now_ts();
    }
    let secs = millis / 1000;
    let nanos = u32::try_from((millis % 1000) * 1_000_000).unwrap_or(0);
    match u64::try_from(secs) {
        Ok(secs) => dto::Timestamp::from(UNIX_EPOCH + std::time::Duration::new(secs, nanos)),
        Err(_) => now_ts(),
    }
}

/// Whether a bucket uses the hierarchical namespace (S3 keys are paths, 03 §6.4).
fn is_hier(bucket: &epoch_client::BucketInfo) -> bool {
    bucket.ns_mode == epoch_proto::grpc::pd::NsMode::Hier
}

/// Collects the S3 metadata headers a PUT carries, to persist on the head and
/// replay on GET/HEAD.
///
/// Silently discarding these is the worst failure mode available: the client gets
/// a 200 and its metadata is gone permanently, with nothing to retry against.
fn http_meta_of(input: &dto::PutObjectInput) -> epoch_client::HttpMeta {
    epoch_client::HttpMeta {
        content_type: input.content_type.clone(),
        content_encoding: input.content_encoding.clone(),
        cache_control: input.cache_control.clone(),
        user: input
            .metadata
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect(),
    }
}

/// The user-metadata map for a response, or `None` when nothing was stored.
fn user_metadata(stored: &std::collections::HashMap<String, String>) -> Option<dto::Metadata> {
    (!stored.is_empty()).then(|| stored.clone().into_iter().collect())
}

#[async_trait::async_trait]
impl<S: TokenSource + 'static> S3 for S3Backend<S> {
    async fn put_object(
        &self,
        req: S3Request<dto::PutObjectInput>,
    ) -> S3Result<S3Response<dto::PutObjectOutput>> {
        let input = req.input;
        let bucket = self.bucket(req.credentials.as_ref(), &input.bucket).await?;
        // Capture the metadata headers before the body is moved out of `input`.
        let http = http_meta_of(&input);
        // Collect the streaming body (bounded; MVP buffers before EC encode).
        let body = match input.body {
            Some(blob) => {
                let mut b: s3s::Body = blob.into();
                b.store_all_limited(MAX_PUT_BYTES)
                    .await
                    .map_err(|e| s3_error!(IncompleteBody, "read body: {e}"))?
            }
            None => bytes::Bytes::new(),
        };
        // Hierarchical buckets translate the key as a path (03 §6.4): implicit
        // mkdir + dir/file conflict. Flat buckets go straight to the object
        // service (inline vs EC).
        let etag = if is_hier(&bucket) {
            crate::s3compat::put(
                self.objects.meta(),
                bucket.bucket_id,
                input.key.as_bytes(),
                body.as_ref(),
                now_millis(),
                http,
            )
            .await
            .map_err(to_s3_error)?
        } else {
            self.objects
                .put_object(
                    bucket.bucket_id,
                    input.key.as_bytes(),
                    body.as_ref(),
                    bucket.inline_threshold,
                    now_millis(),
                    http,
                )
                .await
                .map_err(to_s3_error)?
                .etag
        };
        Ok(S3Response::new(dto::PutObjectOutput {
            e_tag: Some(dto::ETag::Strong(hex16(&etag))),
            ..Default::default()
        }))
    }

    async fn get_object(
        &self,
        req: S3Request<dto::GetObjectInput>,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        let input = req.input;
        let bucket = self.bucket(req.credentials.as_ref(), &input.bucket).await?;
        if is_hier(&bucket) {
            let Some(obj) =
                crate::s3compat::get(self.objects.meta(), bucket.bucket_id, input.key.as_bytes())
                    .await
                    .map_err(to_s3_error)?
            else {
                return Err(s3_error!(NoSuchKey, "no such key: {}", input.key));
            };
            // hier bodies are inline (03 §6.4), so a window is a slice of bytes
            // already in hand — but it must still be honoured, or a ranged read
            // gets the whole object under 200 (see `crate::range` INVARIANT).
            let window = match input.range {
                Some(range) => {
                    let spec = crate::object::RangeSpec::from(range);
                    Some(
                        crate::range::resolve(obj.size, spec.first, spec.last, spec.suffix)
                            .map_err(|_| {
                                to_s3_error(crate::error::GatewayError::RangeNotSatisfiable(
                                    obj.size,
                                ))
                            })?,
                    )
                }
                None => None,
            };
            let bytes = match window {
                Some(w) => {
                    let start = usize::try_from(w.start).unwrap_or(usize::MAX);
                    let end = usize::try_from(w.end).unwrap_or(usize::MAX);
                    obj.body
                        .get(start..=end)
                        .ok_or_else(|| {
                            to_s3_error(crate::error::GatewayError::RangeNotSatisfiable(obj.size))
                        })?
                        .to_vec()
                }
                None => obj.body,
            };
            let body = dto::StreamingBlob::from(s3s::Body::from(bytes));
            let mut response = S3Response::new(dto::GetObjectOutput {
                body: Some(body),
                content_length: Some(window.map_or(obj.size, |w| w.byte_count()) as i64),
                content_range: window.map(|w| w.content_range(obj.size)),
                accept_ranges: Some("bytes".to_string()),
                e_tag: Some(dto::ETag::Strong(hex16(&obj.etag))),
                last_modified: Some(stored_ts(obj.mtime)),
                content_type: obj.http.content_type.clone(),
                content_encoding: obj.http.content_encoding.clone(),
                cache_control: obj.http.cache_control.clone(),
                metadata: user_metadata(&obj.http.user),
                ..Default::default()
            });
            if window.is_some() {
                response.status = Some(::http::StatusCode::PARTIAL_CONTENT);
            }
            return Ok(response);
        }
        let Some(fetched) = self
            .objects
            .get_object_maybe_ranged(bucket.bucket_id, input.key.as_bytes(), input.range)
            .await
            .map_err(to_s3_error)?
        else {
            return Err(s3_error!(NoSuchKey, "no such key: {}", input.key));
        };
        let body = s3s::dto::StreamingBlob::from(s3s::Body::from(fetched.body));
        // A ranged read answers 206 with `Content-Range`; a whole-object read 200.
        // Returning the full body under 200 for a range request would silently
        // corrupt every parallel downloader (see `crate::range` INVARIANT).
        let (status, content_range) = match fetched.range {
            Some(window) => (
                Some(::http::StatusCode::PARTIAL_CONTENT),
                Some(window.content_range(fetched.size)),
            ),
            None => (None, None),
        };
        let content_length = fetched.range.map_or(fetched.size, |w| w.byte_count());
        let mut response = S3Response::new(dto::GetObjectOutput {
            body: Some(body),
            content_length: Some(content_length as i64),
            content_range,
            // Advertise range support so clients probe instead of guessing.
            accept_ranges: Some("bytes".to_string()),
            e_tag: Some(dto::ETag::Strong(hex16(&fetched.etag))),
            last_modified: Some(stored_ts(fetched.mtime)),
            // Replay what the PUT stored (absent stays absent).
            content_type: fetched.http.content_type.clone(),
            content_encoding: fetched.http.content_encoding.clone(),
            cache_control: fetched.http.cache_control.clone(),
            metadata: user_metadata(&fetched.http.user),
            ..Default::default()
        });
        if let Some(status) = status {
            response.status = Some(status);
        }
        Ok(response)
    }

    async fn head_object(
        &self,
        req: S3Request<dto::HeadObjectInput>,
    ) -> S3Result<S3Response<dto::HeadObjectOutput>> {
        let input = req.input;
        let bucket = self.bucket(req.credentials.as_ref(), &input.bucket).await?;
        let head = if is_hier(&bucket) {
            crate::s3compat::head(self.objects.meta(), bucket.bucket_id, input.key.as_bytes())
                .await
                .map_err(to_s3_error)?
        } else {
            self.objects
                .meta()
                .head_object(bucket.bucket_id, input.key.as_bytes())
                .await
                .map_err(|e| to_s3_error(crate::error::GatewayError::Meta(e.to_string())))?
        };
        let Some(head) = head else {
            return Err(s3_error!(NoSuchKey, "no such key: {}", input.key));
        };
        let http = epoch_client::HttpMeta::from_view(&head);
        Ok(S3Response::new(dto::HeadObjectOutput {
            content_length: Some(head.size as i64),
            e_tag: Some(dto::ETag::Strong(hex16_slice(&head.etag))),
            last_modified: Some(stored_ts(head.mtime)),
            // Clients probe HEAD before deciding to range-download.
            accept_ranges: Some("bytes".to_string()),
            content_type: http.content_type,
            content_encoding: http.content_encoding,
            cache_control: http.cache_control,
            metadata: user_metadata(&http.user),
            ..Default::default()
        }))
    }

    async fn delete_object(
        &self,
        req: S3Request<dto::DeleteObjectInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectOutput>> {
        let input = req.input;
        let bucket = self.bucket(req.credentials.as_ref(), &input.bucket).await?;
        if is_hier(&bucket) {
            crate::s3compat::delete(
                self.objects.meta(),
                bucket.bucket_id,
                input.key.as_bytes(),
                now_millis(),
            )
            .await
            .map_err(to_s3_error)?;
        } else {
            self.objects
                .delete_object(bucket.bucket_id, input.key.as_bytes(), now_millis())
                .await
                .map_err(to_s3_error)?;
        }
        Ok(S3Response::new(dto::DeleteObjectOutput::default()))
    }

    async fn delete_objects(
        &self,
        req: S3Request<dto::DeleteObjectsInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectsOutput>> {
        let input = req.input;
        let bucket = self.bucket(req.credentials.as_ref(), &input.bucket).await?;
        let hier = is_hier(&bucket);
        let quiet = input.delete.quiet.unwrap_or(false);
        let ts = now_millis();

        // No batch metadata RPC exists (keys route to independent partitions, so
        // there is no single-partition atomicity to exploit); delete per key. A
        // delete is idempotent — an absent key still reports as deleted (S3
        // semantics). Keys are processed in order; a bounded fan-out is a
        // follow-up if batch latency matters.
        let mut deleted: Vec<dto::DeletedObject> = Vec::new();
        let mut errors: Vec<dto::Error> = Vec::new();
        for obj in input.delete.objects {
            let key = obj.key;
            let result = if hier {
                crate::s3compat::delete(self.objects.meta(), bucket.bucket_id, key.as_bytes(), ts)
                    .await
            } else {
                self.objects
                    .delete_object(bucket.bucket_id, key.as_bytes(), ts)
                    .await
            };
            match result {
                Ok(()) => {
                    // In quiet mode S3 returns only errors, not successes.
                    if !quiet {
                        deleted.push(dto::DeletedObject {
                            key: Some(key),
                            ..Default::default()
                        });
                    }
                }
                Err(e) => errors.push(dto::Error {
                    key: Some(key),
                    code: Some("InternalError".to_string()),
                    message: Some(e.to_string()),
                    ..Default::default()
                }),
            }
        }
        Ok(S3Response::new(dto::DeleteObjectsOutput {
            deleted: (!deleted.is_empty()).then_some(deleted),
            errors: (!errors.is_empty()).then_some(errors),
            ..Default::default()
        }))
    }

    async fn copy_object(
        &self,
        req: S3Request<dto::CopyObjectInput>,
    ) -> S3Result<S3Response<dto::CopyObjectOutput>> {
        let input = req.input;
        let (src_bucket_name, src_key) = match &input.copy_source {
            dto::CopySource::Bucket { bucket, key, .. } => (bucket.to_string(), key.to_string()),
            _ => {
                return Err(s3_error!(
                    NotImplemented,
                    "only bucket/key copy sources are supported"
                ));
            }
        };
        let src = self
            .bucket(req.credentials.as_ref(), &src_bucket_name)
            .await?;
        let dst = self.bucket(req.credentials.as_ref(), &input.bucket).await?;
        // Physical re-encode copy; hier namespaces (implicit-mkdir / dir-file
        // semantics) are a follow-up, both endpoints must be flat.
        if is_hier(&src) || is_hier(&dst) {
            return Err(s3_error!(
                NotImplemented,
                "copy within a hierarchical bucket is not yet supported"
            ));
        }
        let result = self
            .objects
            .copy_object(
                src.bucket_id,
                src_key.as_bytes(),
                dst.bucket_id,
                input.key.as_bytes(),
                dst.inline_threshold,
                now_millis(),
            )
            .await
            .map_err(to_s3_error)?
            .ok_or_else(|| s3_error!(NoSuchKey, "copy source not found: {src_key}"))?;
        Ok(S3Response::new(dto::CopyObjectOutput {
            copy_object_result: Some(dto::CopyObjectResult {
                e_tag: Some(dto::ETag::Strong(hex16(&result.etag))),
                last_modified: Some(now_ts()),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<dto::ListObjectsV2Input>,
    ) -> S3Result<S3Response<dto::ListObjectsV2Output>> {
        let input = req.input;
        let bucket = self.bucket(req.credentials.as_ref(), &input.bucket).await?;
        let prefix = input.prefix.clone().unwrap_or_default();
        let delimiter = input.delimiter.clone().unwrap_or_default();
        // `continuation_token` is our opaque `start_after` (the last key of the
        // prior page); `start_after` seeds the first page (S3 semantics).
        let start_after = input
            .continuation_token
            .clone()
            .or_else(|| input.start_after.clone())
            .unwrap_or_default();
        let limit = input.max_keys.unwrap_or(1000).clamp(1, 1000) as u32;

        let page = if is_hier(&bucket) {
            // A hier bucket's keys *are* paths: LIST is a readdir of the
            // directory the prefix names, which yields the same
            // keys + CommonPrefixes shape (03 §6.4).
            crate::s3compat::list(
                self.objects.meta(),
                bucket.bucket_id,
                prefix.as_bytes(),
                start_after.as_bytes(),
                limit,
            )
            .await
            .map_err(to_s3_error)?
        } else {
            self.list_flat(
                bucket.bucket_id,
                prefix.as_bytes(),
                delimiter.as_bytes(),
                start_after.as_bytes(),
                limit,
            )
            .await?
        };

        let mut contents: Vec<dto::Object> = Vec::new();
        let mut common: Vec<dto::CommonPrefix> = Vec::new();
        for row in &page.rows {
            match row {
                crate::list::ListRow::Object(entry) => contents.push(dto::Object {
                    key: Some(String::from_utf8_lossy(&entry.key).into_owned()),
                    size: entry.head.as_ref().map(|h| h.size as i64),
                    e_tag: entry
                        .head
                        .as_ref()
                        .map(|h| dto::ETag::Strong(hex16_slice(&h.etag))),
                    last_modified: Some(
                        entry
                            .head
                            .as_ref()
                            .map_or_else(now_ts, |h| stored_ts(h.mtime)),
                    ),
                    ..Default::default()
                }),
                crate::list::ListRow::Prefix(p) => common.push(dto::CommonPrefix {
                    prefix: Some(String::from_utf8_lossy(p).into_owned()),
                }),
            }
        }
        // `key_count` counts both forms (S3: keys + collapsed prefixes).
        let key_count = (contents.len() + common.len()) as i32;
        Ok(S3Response::new(dto::ListObjectsV2Output {
            name: Some(input.bucket.clone()),
            prefix: input.prefix,
            delimiter: input.delimiter,
            max_keys: Some(limit as i32),
            key_count: Some(key_count),
            is_truncated: Some(page.next_token.is_some()),
            contents: Some(contents),
            common_prefixes: Some(common),
            continuation_token: input.continuation_token,
            next_continuation_token: page
                .next_token
                .as_ref()
                .map(|t| String::from_utf8_lossy(t).into_owned()),
            ..Default::default()
        }))
    }

    async fn head_bucket(
        &self,
        req: S3Request<dto::HeadBucketInput>,
    ) -> S3Result<S3Response<dto::HeadBucketOutput>> {
        self.bucket(req.credentials.as_ref(), &req.input.bucket)
            .await?;
        Ok(S3Response::new(dto::HeadBucketOutput::default()))
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<dto::CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CreateMultipartUploadOutput>> {
        let input = req.input;
        let bucket = self.bucket(req.credentials.as_ref(), &input.bucket).await?;
        reject_hier_multipart(&bucket)?;
        // Mint a fresh upload id (a random-free monotonic-ish value from the
        // clock; uniqueness within a key is all the metadata layer needs — a
        // duplicate would collide on the (key, upload_id) session key and the
        // create is idempotent).
        let upload_id = now_millis() as u128;
        self.objects
            .create_multipart(
                bucket.bucket_id,
                input.key.as_bytes(),
                upload_id,
                now_millis(),
            )
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(dto::CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(encode_upload_id(upload_id)),
            ..Default::default()
        }))
    }

    async fn upload_part(
        &self,
        req: S3Request<dto::UploadPartInput>,
    ) -> S3Result<S3Response<dto::UploadPartOutput>> {
        let input = req.input;
        let bucket = self.bucket(req.credentials.as_ref(), &input.bucket).await?;
        reject_hier_multipart(&bucket)?;
        let upload_id = decode_upload_id(&input.upload_id)?;
        let part_no = u32::try_from(input.part_number)
            .map_err(|_| s3_error!(InvalidArgument, "part number out of range"))?;
        let body = match input.body {
            Some(blob) => {
                let mut b: s3s::Body = blob.into();
                b.store_all_limited(MAX_PUT_BYTES)
                    .await
                    .map_err(|e| s3_error!(IncompleteBody, "read part body: {e}"))?
            }
            None => bytes::Bytes::new(),
        };
        let result = self
            .objects
            .put_part(
                bucket.bucket_id,
                input.key.as_bytes(),
                upload_id,
                part_no,
                body.as_ref(),
                now_millis(),
            )
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(dto::UploadPartOutput {
            e_tag: Some(dto::ETag::Strong(hex16(&result.etag))),
            ..Default::default()
        }))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<dto::CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CompleteMultipartUploadOutput>> {
        let input = req.input;
        let bucket = self.bucket(req.credentials.as_ref(), &input.bucket).await?;
        reject_hier_multipart(&bucket)?;
        let upload_id = decode_upload_id(&input.upload_id)?;
        // Map the client's listed parts to the MetaNode's `(part_no, etag)` refs;
        // the MetaNode validates each against the stored part (03 multipart).
        let parts = input
            .multipart_upload
            .and_then(|mu| mu.parts)
            .unwrap_or_default();
        let refs = parts
            .into_iter()
            .map(|p| {
                let part_no = p
                    .part_number
                    .and_then(|n| u32::try_from(n).ok())
                    .ok_or_else(|| s3_error!(InvalidPart, "missing/invalid part number"))?;
                let etag = p.e_tag.as_ref().map(render_etag_bytes).unwrap_or_default();
                Ok(epoch_proto::grpc::meta::PartRef { part_no, etag })
            })
            .collect::<S3Result<Vec<_>>>()?;
        self.objects
            .complete_multipart(
                bucket.bucket_id,
                input.key.as_bytes(),
                upload_id,
                refs,
                now_millis(),
            )
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(dto::CompleteMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            ..Default::default()
        }))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<dto::AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::AbortMultipartUploadOutput>> {
        let input = req.input;
        let bucket = self.bucket(req.credentials.as_ref(), &input.bucket).await?;
        reject_hier_multipart(&bucket)?;
        let upload_id = decode_upload_id(&input.upload_id)?;
        self.objects
            .abort_multipart(
                bucket.bucket_id,
                input.key.as_bytes(),
                upload_id,
                now_millis(),
            )
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(dto::AbortMultipartUploadOutput::default()))
    }
}

/// Rejects multipart on a hierarchical bucket (flat-only; hier multipart is a
/// follow-up).
fn reject_hier_multipart(bucket: &epoch_client::BucketInfo) -> S3Result<()> {
    if is_hier(bucket) {
        return Err(s3_error!(
            NotImplemented,
            "multipart upload on a hierarchical bucket is not yet supported"
        ));
    }
    Ok(())
}

/// Renders the S3 `upload_id` string from the internal `u128` (decimal).
fn encode_upload_id(id: u128) -> String {
    id.to_string()
}

/// Parses the internal `u128` upload id from the S3 string.
fn decode_upload_id(s: &str) -> S3Result<u128> {
    s.parse::<u128>()
        .map_err(|_| s3_error!(NoSuchUpload, "malformed upload id"))
}

/// Extracts the 16-byte part digest from a client-supplied ETag string
/// (hex, optionally quoted). A non-16-byte value yields zeros — the MetaNode's
/// etag comparison then rejects the part, which is the correct S3 behavior.
fn render_etag_bytes(etag: &dto::ETag) -> Vec<u8> {
    let raw = match etag {
        dto::ETag::Strong(s) | dto::ETag::Weak(s) => s.as_str(),
    };
    let s = raw.trim_matches('"');
    let mut out = Vec::with_capacity(16);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i + 1 < bytes.len() && out.len() < 16 {
        let hi = (bytes[i] as char).to_digit(16);
        let lo = (bytes[i + 1] as char).to_digit(16);
        match (hi, lo) {
            (Some(h), Some(l)) => out.push((h * 16 + l) as u8),
            _ => break,
        }
        i += 2;
    }
    out
}

/// Lowercase hex of a 16-byte digest (S3 ETag body; `s3s` adds the quotes).
fn hex16(digest: &[u8; 16]) -> String {
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// Same, from a slice (MetaNode wire etag; non-16 slices render what's there).
fn hex16_slice(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use crate::etag;

    use super::{hex16, stored_ts, user_metadata};

    #[test]
    fn hex16_matches_etag_render_body() {
        let d = etag::object_etag(b"abc");
        // hex16 is the unquoted body; render_etag wraps it in quotes.
        assert_eq!(format!("\"{}\"", hex16(&d)), etag::render_etag(&d, None));
    }

    /// `last_modified` must reflect the *stored* mtime. Serving read-time
    /// instead makes `aws s3 sync` and every backup tool re-transfer everything
    /// on each run, because they compare mtimes to detect change.
    #[test]
    fn stored_ts_reflects_the_recorded_millis() {
        // Compare against the SystemTime the millis denote (Timestamp converts
        // one way only, so build the expected value the same way).
        let millis = 1_700_000_000_123u64;
        let expected = super::dto::Timestamp::from(
            std::time::UNIX_EPOCH
                + std::time::Duration::new(millis / 1000, ((millis % 1000) * 1_000_000) as u32),
        );
        assert_eq!(stored_ts(millis as i64), expected);
        // A different instant must not compare equal (guards a stubbed-out impl).
        assert_ne!(stored_ts(millis as i64 + 5_000), expected);
    }

    #[test]
    fn stored_ts_falls_back_for_an_unrecorded_mtime() {
        // A record with no usable mtime still needs *a* last-modified (S3
        // requires it); it must not render as the epoch.
        let epoch = super::dto::Timestamp::from(std::time::UNIX_EPOCH);
        assert_ne!(stored_ts(0), epoch, "0 falls back to now, not the epoch");
        assert_ne!(stored_ts(-1), epoch, "a negative mtime also falls back");
    }

    #[test]
    fn user_metadata_omits_an_empty_map() {
        // An object stored without user metadata must reply with no metadata at
        // all, not an empty map.
        assert!(user_metadata(&std::collections::HashMap::new()).is_none());
        let mut stored = std::collections::HashMap::new();
        stored.insert("owner".to_string(), "trainer".to_string());
        let rendered = user_metadata(&stored).expect("present");
        assert_eq!(rendered.get("owner").map(String::as_str), Some("trainer"));
    }
}
