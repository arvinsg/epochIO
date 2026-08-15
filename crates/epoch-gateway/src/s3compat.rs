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

//! S3 compatibility for hierarchical buckets (03 §6.4): translate flat S3 keys
//! (`a/b/c`) into tree operations over the hier MetaNode ops. Only hier buckets
//! use this; flat buckets pass straight through [`ObjectService`].
//!
//! - **PUT `a/b/c`**: path-walk from the root, implicitly `mkdir` each missing
//!   intermediate directory (the idempotent two-step, 03 §6.2), then write the
//!   file `c` under the final parent. A component that is already a *file* is a
//!   dir/file conflict → `InvalidObjectName` (400).
//! - **GET / HEAD / DELETE**: path-walk to the leaf file; a missing component
//!   or a directory-as-file is `NoSuchKey`.
//! - **LIST**: `readdir` the directory named by the prefix; entries become keys
//!   (a shallow, delimiter-`/` listing — the DFS-merge full form is a
//!   follow-up, this covers the `aws s3 ls s3://bucket/dir/` path).
//!
//! `ROOT_INO` (=1) is the path-walk origin (03 §6.5). This module owns the
//! *namespace* half of a hier operation only — the path walk, implicit mkdir,
//! and conflict rules. The *body* half (inline vs EC, admission, reconstruction,
//! ranged reads) is the same machinery flat buckets use and lives in
//! [`ObjectService`](crate::object::ObjectService), which drives the resolvers
//! here. A hier file body is therefore inline or EC on exactly the flat rule
//! (03 §6.4), not inline-only.
//!
//! Design: docs/design/03-metanode.md §6.4/§6.5

use epoch_client::{ClientError, MetaClient};
use epoch_proto::consts::ROOT_INO;
use epoch_proto::grpc::meta::{self, HierEntryKind, SliceRef};

use crate::error::GatewayError;

/// A resolved path: the leaf's parent directory inode and the leaf name.
pub struct LeafPath {
    /// The inode of the directory holding the leaf.
    pub parent_ino: u64,
    /// The leaf's name within that directory.
    pub name: Vec<u8>,
}

/// Splits an S3 key into non-empty `/`-separated components. A trailing slash
/// (a "directory marker") drops the empty final component.
fn components(key: &[u8]) -> Vec<&[u8]> {
    key.split(|&b| b == b'/')
        .filter(|c| !c.is_empty())
        .collect()
}

/// A hier file resolved by a path walk: its head plus its content reference
/// (inline bytes, or the full EC slice list assembled by the MetaNode).
pub struct HierFile {
    /// The file's head view (size, etag, mtime, HTTP metadata).
    pub head: meta::ObjectHeadView,
    /// The inline bytes, when the head is inline.
    pub inline_data: Vec<u8>,
    /// The full EC slice list, when the head is not inline.
    pub slices: Vec<SliceRef>,
}

/// Walks the path to the leaf's parent, creating intermediate directories
/// (implicit mkdir, 03 §6.4). Returns the leaf's `(parent_ino, name)`.
///
/// `ensure_dirs` distinguishes PUT (create missing dirs) from read/delete
/// (missing dir → not found).
async fn walk_to_leaf(
    meta: &MetaClient,
    bucket: u64,
    key: &[u8],
    ensure_dirs: bool,
    ts_millis: i64,
) -> Result<Option<LeafPath>, GatewayError> {
    let comps = components(key);
    let Some((leaf, dirs)) = comps.split_last() else {
        // Empty key (or only slashes): not a valid object name.
        return Err(GatewayError::Meta("empty object key".to_string()));
    };
    let mut parent_ino = ROOT_INO;
    for dir in dirs {
        let looked = meta
            .hier_lookup(bucket, parent_ino, dir)
            .await
            .map_err(meta_err)?;
        match HierEntryKind::try_from(looked.kind).unwrap_or(HierEntryKind::HierEntryUnspecified) {
            HierEntryKind::HierEntryDir => parent_ino = looked.dir_ino,
            HierEntryKind::HierEntryFile => {
                // A path component is a file → dir/file conflict (03 §6.4).
                return Err(GatewayError::DirFileConflict);
            }
            HierEntryKind::HierEntryUnspecified => {
                if !ensure_dirs {
                    return Ok(None);
                }
                parent_ino = mkdir(meta, bucket, parent_ino, dir, ts_millis).await?;
            }
        }
    }
    Ok(Some(LeafPath {
        parent_ino,
        name: leaf.to_vec(),
    }))
}

/// The idempotent two-step mkdir (03 §6.2): step 1 mints the child sentinel,
/// step 2 links it into the parent. If the name already links a directory, the
/// existing child is returned (idempotent); a file at the name is a conflict.
async fn mkdir(
    meta: &MetaClient,
    bucket: u64,
    parent_ino: u64,
    name: &[u8],
    ts_millis: i64,
) -> Result<u64, GatewayError> {
    // Re-check under the (possible) race: another PUT may have created it.
    let looked = meta
        .hier_lookup(bucket, parent_ino, name)
        .await
        .map_err(meta_err)?;
    match HierEntryKind::try_from(looked.kind).unwrap_or(HierEntryKind::HierEntryUnspecified) {
        HierEntryKind::HierEntryDir => return Ok(looked.dir_ino),
        HierEntryKind::HierEntryFile => return Err(GatewayError::DirFileConflict),
        HierEntryKind::HierEntryUnspecified => {}
    }
    // Step 1: mint the child sentinel. Step 2: link it (commit point). If the
    // link is rejected as a conflict, surface it; a concurrent link of the same
    // name is idempotent server-side.
    let child_ino = meta
        .hier_mkdir_sentinel(bucket, parent_ino, name, ts_millis)
        .await
        .map_err(meta_err)?;
    if let Some(reason) = meta
        .hier_mkdir_link(bucket, parent_ino, name, child_ino, ts_millis)
        .await
        .map_err(meta_err)?
    {
        // A file appeared at the name between our check and link (03 §6.4).
        return Err(GatewayError::Meta(format!("mkdir {name:?}: {reason}")));
    }
    Ok(child_ino)
}

/// Resolves a hier PUT's target: ensures every intermediate directory exists
/// (implicit mkdir, 03 §6.4) and rejects a leaf name already held by a
/// directory. The caller writes the body through
/// [`commit_write`] once it has an inline body or an EC slice list.
///
/// Splitting resolve from commit is what lets a hier body take the *same*
/// inline-vs-EC path a flat object takes: the body machinery never has to know
/// about path walks, and the path walk never has to know about EC.
///
/// # Errors
/// [`GatewayError::DirFileConflict`] (→ 400) when a path component is a file or
/// the leaf name is a directory; [`GatewayError::Meta`] on a metadata failure.
pub async fn resolve_for_write(
    meta: &MetaClient,
    bucket: u64,
    key: &[u8],
    ts_millis: i64,
) -> Result<LeafPath, GatewayError> {
    let Some(leaf) = walk_to_leaf(meta, bucket, key, true, ts_millis).await? else {
        return Err(GatewayError::Meta("path walk failed".to_string()));
    };
    // A leaf whose name is an existing directory is a dir/file conflict.
    let looked = meta
        .hier_lookup(bucket, leaf.parent_ino, &leaf.name)
        .await
        .map_err(meta_err)?;
    if HierEntryKind::try_from(looked.kind) == Ok(HierEntryKind::HierEntryDir) {
        return Err(GatewayError::DirFileConflict);
    }
    Ok(leaf)
}

/// Commits a resolved hier write: one `HierWrite` carrying either inline bytes
/// or an EC slice list (exactly the two `PutObject` forms, 03 §6.4). An
/// overwrite captures the old file's slices at apply time (INVARIANT 03 §5), so
/// replacing an EC file never leaks its blobs.
///
/// # Errors
/// [`GatewayError::Meta`] on a metadata failure or a rejected write.
#[allow(clippy::too_many_arguments)]
pub async fn commit_write(
    meta: &MetaClient,
    bucket: u64,
    leaf: &LeafPath,
    size: u64,
    etag: [u8; 16],
    inline_data: Vec<u8>,
    slices: Vec<SliceRef>,
    ts_millis: i64,
    http: epoch_client::HttpMeta,
) -> Result<(), GatewayError> {
    let written = meta
        .hier_write(epoch_client::HierWrite {
            bucket,
            parent_ino: leaf.parent_ino,
            name: &leaf.name,
            size,
            etag,
            inline_data,
            slices,
            ts_millis,
            http,
        })
        .await
        .map_err(write_err)?;
    if let Some(reason) = written {
        return Err(GatewayError::Meta(format!("hier write: {reason}")));
    }
    Ok(())
}

/// Resolves a hier read: path-walk to the leaf and return the file's head and
/// content reference. `None` when a component is missing or the leaf is a
/// directory (→ `NoSuchKey`).
///
/// The MetaNode assembles the *full* slice list (head-embedded + overflow
/// segments, 03 §4.2), so a large EC file resolves in one round trip.
///
/// # Errors
/// [`GatewayError::Meta`] on a metadata failure.
pub async fn resolve_for_read(
    meta: &MetaClient,
    bucket: u64,
    key: &[u8],
) -> Result<Option<HierFile>, GatewayError> {
    let Some(leaf) = walk_to_leaf(meta, bucket, key, false, 0).await? else {
        return Ok(None);
    };
    let looked = meta
        .hier_lookup(bucket, leaf.parent_ino, &leaf.name)
        .await
        .map_err(meta_err)?;
    if HierEntryKind::try_from(looked.kind).unwrap_or(HierEntryKind::HierEntryUnspecified)
        != HierEntryKind::HierEntryFile
    {
        return Ok(None);
    }
    Ok(Some(HierFile {
        head: looked.file.unwrap_or_default(),
        inline_data: looked.inline_data,
        slices: looked.slices,
    }))
}

/// HEAD from a hier bucket: the leaf file's head view, or `None` if absent.
///
/// # Errors
/// [`GatewayError::Meta`] on a metadata failure.
pub async fn head(
    meta: &MetaClient,
    bucket: u64,
    key: &[u8],
) -> Result<Option<meta::ObjectHeadView>, GatewayError> {
    Ok(resolve_for_read(meta, bucket, key).await?.map(|f| f.head))
}

/// LIST a hier bucket: `readdir` the directory the prefix names (03 §6.4).
///
/// A hier bucket's keys *are* paths, so S3's flat-prefix listing maps onto one
/// directory read: files become object rows, subdirectories become
/// `CommonPrefixes`. The delimiter is implicitly `/` — a hier bucket has no other
/// meaningful grouping, and S3 clients always send `/` for a directory view.
///
/// `prefix` names the directory to read (with or without a trailing slash); a
/// prefix that resolves to a file or to nothing lists empty rather than erroring,
/// matching S3's "a prefix matching nothing is not an error".
///
/// # Errors
/// [`GatewayError::Meta`] on a metadata failure.
pub async fn list(
    meta: &MetaClient,
    bucket: u64,
    prefix: &[u8],
    start_after: &[u8],
    limit: u32,
) -> Result<crate::list::ListPage, GatewayError> {
    // Resolve the prefix to a directory inode. An empty prefix is the root.
    let mut dir_ino = ROOT_INO;
    let mut base: Vec<u8> = Vec::new();
    for comp in components(prefix) {
        let looked = meta
            .hier_lookup(bucket, dir_ino, comp)
            .await
            .map_err(meta_err)?;
        match HierEntryKind::try_from(looked.kind).unwrap_or(HierEntryKind::HierEntryUnspecified) {
            HierEntryKind::HierEntryDir => {
                dir_ino = looked.dir_ino;
                base.extend_from_slice(comp);
                base.push(b'/');
            }
            // A prefix naming a file, or naming nothing, matches no listing.
            HierEntryKind::HierEntryFile | HierEntryKind::HierEntryUnspecified => {
                return Ok(crate::list::ListPage::default());
            }
        }
    }

    // `start_after` arrives as a full key; readdir wants the name within the
    // directory, so strip the resolved base path.
    let cursor = start_after.strip_prefix(base.as_slice()).unwrap_or(b"");
    let entries = meta
        .hier_readdir(bucket, dir_ino, cursor, limit)
        .await
        .map_err(meta_err)?;

    let truncated = entries.len() as u32 == limit;
    let mut rows = Vec::with_capacity(entries.len());
    let mut last_name: Vec<u8> = Vec::new();
    for entry in entries {
        last_name = entry.name.clone();
        let mut key = base.clone();
        key.extend_from_slice(&entry.name);
        match HierEntryKind::try_from(entry.kind).unwrap_or(HierEntryKind::HierEntryUnspecified) {
            HierEntryKind::HierEntryFile => {
                rows.push(crate::list::ListRow::Object(
                    epoch_proto::grpc::meta::ObjectEntry {
                        key,
                        head: entry.file,
                    },
                ));
            }
            HierEntryKind::HierEntryDir => {
                // A subdirectory is a collapsed prefix, delimiter-terminated.
                key.push(b'/');
                rows.push(crate::list::ListRow::Prefix(key));
            }
            HierEntryKind::HierEntryUnspecified => {}
        }
    }
    let next_token = truncated.then(|| {
        let mut token = base.clone();
        token.extend_from_slice(&last_name);
        token
    });
    Ok(crate::list::ListPage { rows, next_token })
}

/// DELETE from a hier bucket: path-walk + unlink the file (idempotent). Empty
/// directories are left (03 §6.4: DELETE 不删空目录).
///
/// # Errors
/// [`GatewayError::Meta`] on a metadata failure.
pub async fn delete(
    meta: &MetaClient,
    bucket: u64,
    key: &[u8],
    ts_millis: i64,
) -> Result<(), GatewayError> {
    let Some(leaf) = walk_to_leaf(meta, bucket, key, false, ts_millis).await? else {
        return Ok(()); // a missing path is a no-op delete
    };
    meta.hier_unlink(bucket, leaf.parent_ino, &leaf.name, ts_millis)
        .await
        .map_err(meta_err)?;
    Ok(())
}

/// Maps a client error to the gateway's metadata error.
fn meta_err(e: ClientError) -> GatewayError {
    GatewayError::Meta(e.to_string())
}

/// Maps a write's client error, keeping the inline guard's `RESOURCE_EXHAUSTED`
/// (03 §4.3) as its own typed variant so the caller can downgrade to EC.
fn write_err(e: ClientError) -> GatewayError {
    match &e {
        ClientError::Rpc { code, .. } if code == "ResourceExhausted" => {
            GatewayError::MetaInlineGuard(e.to_string())
        }
        _ => GatewayError::Meta(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn components_splits_and_drops_empties() {
        assert_eq!(components(b"a/b/c"), vec![&b"a"[..], b"b", b"c"]);
        assert_eq!(components(b"/a//b/"), vec![&b"a"[..], b"b"]);
        assert_eq!(components(b"single"), vec![&b"single"[..]]);
        assert!(components(b"").is_empty());
        assert!(components(b"///").is_empty());
    }
}
