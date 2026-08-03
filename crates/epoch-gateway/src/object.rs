//! The S3 object orchestrator (M6): ties the EC write/read [`Gateway`] to the
//! MetaNode metadata client, with the inline-vs-EC decision and the EC-buffer
//! admission gate. This is the layer the S3 HTTP head (M6-3) drives, one method
//! per S3 verb.
//!
//! ## PUT (00 §5)
//!
//! - **inline**: `size ≤ bucket.inline_threshold` → one MetaNode `PutObject`
//!   carrying the bytes (a single raft round, no EC). If the MetaNode's guard
//!   rejects it (`resource_exhausted`, 03 §4.3 partition inline share), the
//!   write **transparently downgrades** to the EC path.
//! - **EC**: acquire buffer budget → `Gateway::put_object` (quorum + rewrite)
//!   → map the layout to slices → MetaNode `PutObject{slices}`.
//!
//! The S3 ETag (MD5, [`crate::etag`]) is computed over the body and stored in
//! the head either way.
//!
//! ## GET / HEAD / DELETE / LIST
//!
//! GET fetches the full slice list (`GetObjectMeta`), rebuilds the layout via
//! the chunk map, and reconstructs through the EC read path; inline objects
//! return their bytes directly. HEAD/DELETE/LIST are thin MetaNode passthroughs.
//!
//! Design: docs/design/00-overview.md §5; docs/design/03-metanode.md §4.3/§5

use std::sync::Arc;

use epoch_client::{ChunkMap, ClientError, MetaClient, TokenSource};

use crate::admission::Admission;
use crate::code::CodeMode;
use crate::error::GatewayError;
use crate::etag;
use crate::gateway::Gateway;
use crate::range;
use crate::slices::{layout_to_slices, slices_to_layout};

/// A stored object read back for a GET: its bytes and S3 ETag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedObject {
    /// The object's bytes (the requested window when `range` is set).
    pub body: Vec<u8>,
    /// The S3 ETag digest (16 bytes) recorded at write time.
    pub etag: [u8; 16],
    /// Full object size in bytes (not the window's length).
    pub size: u64,
    /// The resolved byte window for a ranged read; `None` for a whole-object GET.
    /// Present ⇒ the caller must answer 206 with a matching `Content-Range`.
    pub range: Option<range::ByteRange>,
    /// The HTTP metadata stored at write time, replayed on the response.
    pub http: epoch_client::HttpMeta,
    /// Last-modified, wall-clock millis as recorded by the write.
    ///
    /// The response must carry *this*, not the read's own clock: `aws s3 sync`
    /// and every backup tool compares mtimes to decide what changed, so a
    /// last-modified that tracks read time makes each run re-transfer everything.
    pub mtime: i64,
}

/// A client's requested byte range, before resolution against the object size.
///
/// Mirrors RFC 9110's two forms so the HTTP layer can hand over the parsed header
/// without this crate depending on the S3 protocol types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeSpec {
    /// First byte offset, or the suffix length when `suffix` is set.
    pub first: u64,
    /// Last byte offset (inclusive); `None` means "to the end".
    pub last: Option<u64>,
    /// Whether `first` is a suffix length (`bytes=-N`) rather than an offset.
    pub suffix: bool,
}

impl From<s3s::dto::Range> for RangeSpec {
    fn from(range: s3s::dto::Range) -> Self {
        match range {
            s3s::dto::Range::Int { first, last } => Self {
                first,
                last,
                suffix: false,
            },
            s3s::dto::Range::Suffix { length } => Self {
                first: length,
                last: None,
                suffix: true,
            },
        }
    }
}

/// The result of a PUT: the object's S3 ETag digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutResult {
    /// The S3 ETag digest (16 bytes).
    pub etag: [u8; 16],
}

/// The S3 object service: EC gateway + MetaNode client + admission, per bucket.
///
/// Cheap to clone (all state is shared). One instance serves all buckets that
/// share a code mode; the inline threshold is supplied per call from the
/// bucket cache (M6 HTTP layer).
pub struct ObjectService<S: TokenSource> {
    gateway: Arc<Gateway<S>>,
    meta: MetaClient,
    chunk_map: ChunkMap,
    admission: Admission,
    code: CodeMode,
}

impl<S: TokenSource> Clone for ObjectService<S> {
    fn clone(&self) -> Self {
        Self {
            gateway: Arc::clone(&self.gateway),
            meta: self.meta.clone(),
            chunk_map: self.chunk_map.clone(),
            admission: self.admission.clone(),
            code: self.code,
        }
    }
}

impl<S: TokenSource> ObjectService<S> {
    /// Assembles the service over its pieces. `code` is the cluster/bucket EC
    /// code mode (the shape a GET reconstructs with).
    #[must_use]
    pub fn new(
        gateway: Arc<Gateway<S>>,
        meta: MetaClient,
        chunk_map: ChunkMap,
        admission: Admission,
        code: CodeMode,
    ) -> Self {
        Self {
            gateway,
            meta,
            chunk_map,
            admission,
            code,
        }
    }

    /// Writes an object (00 §5): inline when `size ≤ inline_threshold` (0 =
    /// disabled), else EC; a guard-rejected inline write downgrades to EC.
    /// Returns the object's S3 ETag.
    ///
    /// `http` carries the S3 headers to persist (Content-Type, `x-amz-meta-*`);
    /// they are stored on the head and replayed on GET/HEAD.
    ///
    /// # Errors
    ///
    /// [`GatewayError`] for EC/quorum/admission failures; [`GatewayError::Meta`]
    /// for a metadata commit failure that is not an inline-guard downgrade.
    pub async fn put_object(
        &self,
        bucket: u64,
        key: &[u8],
        body: &[u8],
        inline_threshold: u64,
        ts_millis: i64,
        http: epoch_client::HttpMeta,
    ) -> Result<PutResult, GatewayError> {
        let etag = etag::object_etag(body);
        let size = body.len() as u64;

        if inline_threshold > 0 && size <= inline_threshold {
            match self
                .meta
                .put_object(epoch_client::PutObject {
                    bucket,
                    key,
                    size,
                    etag,
                    inline_data: body.to_vec(),
                    slices: Vec::new(),
                    ts_millis,
                    http: http.clone(),
                })
                .await
            {
                Ok(()) => return Ok(PutResult { etag }),
                // The partition's inline guard refused it (03 §4.3): fall
                // through to the EC path (transparent downgrade).
                Err(e) if is_inline_downgrade(&e) => {
                    tracing::debug!("inline PUT rejected by guard; downgrading to EC");
                }
                Err(e) => return Err(GatewayError::Meta(e.to_string())),
            }
        }

        // EC path: reserve buffer budget, encode+write, commit the slice list.
        let _permit = self.admission.acquire(body.len()).await?;
        let layout = self.gateway.put_object(body).await?;
        let slices = layout_to_slices(&layout);
        self.meta
            .put_object(epoch_client::PutObject {
                bucket,
                key,
                size,
                etag,
                inline_data: Vec::new(),
                slices,
                ts_millis,
                http,
            })
            .await
            .map_err(|e| GatewayError::Meta(e.to_string()))?;
        // The metadata now references these blobs: resolve them off the writer's
        // in-flight set so the GC watermark advances past them (Q27).
        self.gateway.resolve_layout(&layout);
        Ok(PutResult { etag })
    }

    /// Reads a whole object, or `None` if absent (00 §5 read path). Inline
    /// objects return their stored bytes; EC objects are reconstructed from the
    /// resolved slice list.
    ///
    /// # Errors
    ///
    /// [`GatewayError::Meta`] for a metadata read failure; [`GatewayError`] for
    /// EC reconstruction failure.
    pub async fn get_object(
        &self,
        bucket: u64,
        key: &[u8],
    ) -> Result<Option<FetchedObject>, GatewayError> {
        self.fetch(bucket, key, None).await
    }

    /// Reads one byte window of an object (RFC 9110 ranged GET), or `None` if the
    /// object is absent. `range` is the client's parsed header; the returned
    /// [`FetchedObject::range`] carries the resolved window for `Content-Range`.
    ///
    /// Only the blobs the window touches are fetched and decoded — a 1 MiB read of
    /// a 10 GiB object costs one blob, not the whole object.
    ///
    /// # Errors
    ///
    /// [`GatewayError::RangeNotSatisfiable`] when the window lies outside the
    /// object; otherwise as [`get_object`](Self::get_object).
    pub async fn get_object_range(
        &self,
        bucket: u64,
        key: &[u8],
        range: RangeSpec,
    ) -> Result<Option<FetchedObject>, GatewayError> {
        self.fetch(bucket, key, Some(range)).await
    }

    /// GET with an optional S3 `Range` header: ranged when present, whole-object
    /// otherwise. The one entry point the HTTP layer needs, so it never has to
    /// branch (and never has to *remember* to branch — the defect this replaces
    /// was a missing branch that answered ranges with the full body).
    ///
    /// # Errors
    ///
    /// As [`get_object_range`](Self::get_object_range).
    pub async fn get_object_maybe_ranged(
        &self,
        bucket: u64,
        key: &[u8],
        range: Option<s3s::dto::Range>,
    ) -> Result<Option<FetchedObject>, GatewayError> {
        self.fetch(bucket, key, range.map(RangeSpec::from)).await
    }

    /// The single read path: resolve metadata, then serve the whole object or the
    /// requested window. Kept as one function so a ranged and a full GET can never
    /// disagree about integrity checks or heal reporting.
    async fn fetch(
        &self,
        bucket: u64,
        key: &[u8],
        want: Option<RangeSpec>,
    ) -> Result<Option<FetchedObject>, GatewayError> {
        let Some(meta) = self
            .meta
            .get_object_meta(bucket, key)
            .await
            .map_err(|e| GatewayError::Meta(e.to_string()))?
        else {
            return Ok(None);
        };
        let head = meta.head.unwrap_or_default();
        let etag = to_etag(&head.etag);
        let http = epoch_client::HttpMeta::from_view(&head);

        // Resolve the window (if any) against the recorded size.
        let window = match want {
            Some(spec) => Some(
                range::resolve(head.size, spec.first, spec.last, spec.suffix)
                    .map_err(|_| GatewayError::RangeNotSatisfiable(head.size))?,
            ),
            None => None,
        };

        if head.inline {
            let body = match window {
                // Inline bodies are already in memory: slice directly.
                Some(w) => {
                    let start = usize::try_from(w.start).unwrap_or(usize::MAX);
                    let end = usize::try_from(w.end).unwrap_or(usize::MAX);
                    meta.inline_data
                        .get(start..=end)
                        .ok_or(GatewayError::RangeNotSatisfiable(head.size))?
                        .to_vec()
                }
                None => meta.inline_data,
            };
            return Ok(Some(FetchedObject {
                body,
                etag,
                size: head.size,
                range: window,
                http,
                mtime: head.mtime,
            }));
        }

        let layout = slices_to_layout(&meta.slices, self.code, head.size, &self.chunk_map).await?;
        // A ranged read narrows the layout to the overlapping blobs; a full read
        // takes it as-is.
        let (read_layout, trim) = match window {
            Some(w) => {
                let plan =
                    range::plan(&layout, w).ok_or(GatewayError::RangeNotSatisfiable(head.size))?;
                (
                    crate::code::ObjectLayout {
                        // The narrowed layout's `size` is the bytes it spans, so the
                        // read path's own length check stays meaningful.
                        size: plan.blobs.iter().map(|b| b.len as u64).sum(),
                        code: layout.code,
                        blobs: plan.blobs,
                    },
                    Some((plan.skip, plan.take)),
                )
            }
            None => (layout, None),
        };
        let read = self.gateway.get_object(&read_layout).await?;
        // heal-on-read (04 §4): the bytes are ready to return; any missing/corrupt
        // shard is reported to PD asynchronously so a repair never blocks the GET
        // (best-effort — a lost report is caught by the InspectRound backstop).
        if !read.heals.is_empty() {
            self.spawn_heal_reports(read.heals);
        }
        let body = match trim {
            Some((skip, take)) => read
                .bytes
                .get(skip..skip + take)
                .ok_or(GatewayError::RangeNotSatisfiable(head.size))?
                .to_vec(),
            None => read.bytes,
        };
        Ok(Some(FetchedObject {
            body,
            etag,
            size: head.size,
            range: window,
            http,
            mtime: head.mtime,
        }))
    }

    /// Fires the heal-on-read reports to PD in a detached task so they never
    /// delay or fail the GET response (01 §6.4 Q5 fast path, best-effort).
    fn spawn_heal_reports(&self, heals: Vec<crate::get::HealReport>) {
        let chunk_map = self.chunk_map.clone();
        tokio::spawn(async move {
            for heal in heals {
                if let Err(err) = chunk_map
                    .report_shard_repair(heal.chunk_id, heal.index)
                    .await
                {
                    tracing::debug!(
                        chunk = heal.chunk_id.get(),
                        index = heal.index,
                        error = %err,
                        "heal-on-read report failed (best-effort; backstop will catch it)"
                    );
                }
            }
        });
    }

    /// Deletes an object (idempotent).
    ///
    /// # Errors
    /// [`GatewayError::Meta`] on a metadata failure.
    pub async fn delete_object(
        &self,
        bucket: u64,
        key: &[u8],
        ts_millis: i64,
    ) -> Result<(), GatewayError> {
        self.meta
            .delete_object(bucket, key, ts_millis)
            .await
            .map_err(|e| GatewayError::Meta(e.to_string()))
    }

    /// Server-side copy (S3 `CopyObject`): reads the source object and rewrites
    /// its bytes under the destination key. This is a **physical** copy —
    /// re-reading (reconstructing) and re-EC-encoding the bytes — so the two
    /// keys own independent blobs. A metadata-only copy that shared blobs would
    /// be unsafe under the single-phase delete model (03 §5: `apply_delete`
    /// unconditionally captures a key's slices into the delete queue, with no
    /// blob refcount, so deleting either key would reclaim the other's data).
    ///
    /// Returns the destination object's ETag, or `None` if the source is absent.
    ///
    /// # Errors
    ///
    /// [`GatewayError`] for a read/reconstruct or write/commit failure.
    pub async fn copy_object(
        &self,
        src_bucket: u64,
        src_key: &[u8],
        dst_bucket: u64,
        dst_key: &[u8],
        inline_threshold: u64,
        ts_millis: i64,
    ) -> Result<Option<PutResult>, GatewayError> {
        let Some(src) = self.get_object(src_bucket, src_key).await? else {
            return Ok(None);
        };
        // S3's default metadata directive is COPY: the destination inherits the
        // source's Content-Type and user metadata.
        let result = self
            .put_object(
                dst_bucket,
                dst_key,
                &src.body,
                inline_threshold,
                ts_millis,
                src.http,
            )
            .await?;
        Ok(Some(result))
    }

    /// The MetaNode client (for the HTTP layer's HEAD/LIST/multipart passthroughs).
    #[must_use]
    pub fn meta(&self) -> &MetaClient {
        &self.meta
    }

    /// Begins a multipart upload (S3 `CreateMultipartUpload`): records the
    /// session under `upload_id`.
    ///
    /// # Errors
    /// [`GatewayError::Meta`] on a metadata failure.
    pub async fn create_multipart(
        &self,
        bucket: u64,
        key: &[u8],
        upload_id: u128,
        ts_millis: i64,
    ) -> Result<(), GatewayError> {
        self.meta
            .create_multipart(bucket, key, upload_id, ts_millis)
            .await
            .map_err(|e| GatewayError::Meta(e.to_string()))
    }

    /// Uploads one part (S3 `UploadPart`): EC-encodes the part's bytes (a part is
    /// always EC — there is no inline part form) and records its slice list +
    /// ETag under `(upload_id, part_no)`. Returns the part's ETag. Reuses the
    /// object write path but writes no head — the head is assembled at complete.
    ///
    /// # Errors
    /// [`GatewayError`] for an EC/admission failure; [`GatewayError::Meta`] for a
    /// metadata failure (including a rejected unknown upload id).
    pub async fn put_part(
        &self,
        bucket: u64,
        key: &[u8],
        upload_id: u128,
        part_no: u32,
        body: &[u8],
        ts_millis: i64,
    ) -> Result<PutResult, GatewayError> {
        let etag = etag::object_etag(body);
        let _permit = self.admission.acquire(body.len()).await?;
        let layout = self.gateway.put_object(body).await?;
        let slices = layout_to_slices(&layout);
        if let Some(rejected) = self
            .meta
            .put_part(
                bucket,
                key,
                upload_id,
                part_no,
                body.len() as u64,
                etag,
                slices,
                ts_millis,
            )
            .await
            .map_err(|e| GatewayError::Meta(e.to_string()))?
        {
            return Err(GatewayError::Meta(rejected));
        }
        // The part metadata now references these blobs (Q27): resolve them.
        self.gateway.resolve_layout(&layout);
        Ok(PutResult { etag })
    }

    /// Completes a multipart upload (S3 `CompleteMultipartUpload`): assembles the
    /// listed parts into one object head. Returns the object's ETag (the
    /// MetaNode's multipart digest — not the S3 `md5(...)-N` form, a documented
    /// limitation, 03 multipart).
    ///
    /// # Errors
    /// [`GatewayError::Meta`] on a metadata failure or a rejected completion
    /// (unknown upload / missing part / etag mismatch / empty list).
    pub async fn complete_multipart(
        &self,
        bucket: u64,
        key: &[u8],
        upload_id: u128,
        parts: Vec<epoch_proto::grpc::meta::PartRef>,
        ts_millis: i64,
    ) -> Result<(), GatewayError> {
        match self
            .meta
            .complete_multipart(bucket, key, upload_id, parts, ts_millis)
            .await
            .map_err(|e| GatewayError::Meta(e.to_string()))?
        {
            Some(rejected) => Err(GatewayError::Meta(rejected)),
            None => Ok(()),
        }
    }

    /// Aborts a multipart upload (S3 `AbortMultipartUpload`; idempotent).
    ///
    /// # Errors
    /// [`GatewayError::Meta`] on a metadata failure.
    pub async fn abort_multipart(
        &self,
        bucket: u64,
        key: &[u8],
        upload_id: u128,
        ts_millis: i64,
    ) -> Result<(), GatewayError> {
        self.meta
            .abort_multipart(bucket, key, upload_id, ts_millis)
            .await
            .map_err(|e| GatewayError::Meta(e.to_string()))
    }
}

/// Whether a metadata error is the inline-guard downgrade signal
/// (`resource_exhausted`, 03 §4.3) — the gateway retries the write as EC.
fn is_inline_downgrade(err: &ClientError) -> bool {
    matches!(err, ClientError::Rpc { code, .. } if code == "ResourceExhausted")
}

/// Decodes a wire etag field (16 bytes; a malformed/absent value → zeros).
fn to_etag(bytes: &[u8]) -> [u8; 16] {
    <[u8; 16]>::try_from(bytes).unwrap_or([0; 16])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_downgrade_detects_resource_exhausted_only() {
        assert!(is_inline_downgrade(&ClientError::Rpc {
            code: "ResourceExhausted".to_string(),
            message: "inline share".to_string(),
        }));
        assert!(!is_inline_downgrade(&ClientError::Rpc {
            code: "Internal".to_string(),
            message: String::new(),
        }));
        assert!(!is_inline_downgrade(&ClientError::NotFound(
            "x".to_string()
        )));
    }

    #[test]
    fn to_etag_tolerates_bad_lengths() {
        assert_eq!(to_etag(&[7; 16]), [7; 16]);
        assert_eq!(to_etag(&[1, 2, 3]), [0; 16]);
        assert_eq!(to_etag(&[]), [0; 16]);
    }
}
