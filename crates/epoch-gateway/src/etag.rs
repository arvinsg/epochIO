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

//! S3 ETag computation (M6). S3 mandates MD5-based ETags — distinct from the
//! internal integrity digests (per-blob CRC32C frames + blake3, already in the
//! store/EC layers). This module computes only the *S3-visible* ETag the
//! gateway returns and stores in the object head's 16-byte etag field:
//!
//! - **single PUT**: `MD5(body)` — the raw 16-byte digest (S3 renders it hex).
//! - **multipart Complete**: `MD5(concat(part MD5s))` with a `-<n>` part-count
//!   suffix in the S3 string form (03 §5 note; the count rides in the response
//!   layer, the head stores the 16-byte digest).
//!
//! Design: docs/design/03-metanode.md §4.2 (etag field); AWS S3 ETag semantics

use md5::{Digest, Md5};

/// The S3 ETag of a whole object body: `MD5(body)` (16 bytes).
#[must_use]
pub fn object_etag(body: &[u8]) -> [u8; 16] {
    let mut hasher = Md5::new();
    hasher.update(body);
    hasher.finalize().into()
}

/// The S3 multipart ETag digest: `MD5(concat(part MD5s))` (16 bytes). The
/// `-<part_count>` suffix is an S3 string-form concern (rendered by the HTTP
/// layer); the head stores this digest.
#[must_use]
pub fn multipart_etag(part_etags: &[[u8; 16]]) -> [u8; 16] {
    let mut hasher = Md5::new();
    for etag in part_etags {
        hasher.update(etag);
    }
    hasher.finalize().into()
}

/// Renders an ETag digest as the quoted lowercase-hex string S3 clients expect,
/// with the multipart `-<n>` suffix when `part_count` is `Some` (`n ≥ 1`).
#[must_use]
pub fn render_etag(digest: &[u8; 16], part_count: Option<u32>) -> String {
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    match part_count {
        Some(n) => format!("\"{hex}-{n}\""),
        None => format!("\"{hex}\""),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_etag_matches_known_md5() {
        // MD5("") = d41d8cd98f00b204e9800998ecf8427e (RFC 1321 test vector).
        assert_eq!(
            render_etag(&object_etag(b""), None),
            "\"d41d8cd98f00b204e9800998ecf8427e\""
        );
        // MD5("abc") = 900150983cd24fb0d6963f7d28e17f72.
        assert_eq!(
            render_etag(&object_etag(b"abc"), None),
            "\"900150983cd24fb0d6963f7d28e17f72\""
        );
    }

    #[test]
    fn multipart_etag_hashes_concatenated_part_digests_with_suffix() {
        let p1 = object_etag(b"part-one");
        let p2 = object_etag(b"part-two");
        let combined = multipart_etag(&[p1, p2]);
        // Deterministic: same as MD5 of the two 16-byte digests concatenated.
        let mut expected = Md5::new();
        expected.update(p1);
        expected.update(p2);
        let expected: [u8; 16] = expected.finalize().into();
        assert_eq!(combined, expected);
        // The string form carries the part count.
        assert!(render_etag(&combined, Some(2)).ends_with("-2\""));
    }
}
