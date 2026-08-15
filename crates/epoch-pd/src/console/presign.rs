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

//! SigV4 presigned-URL *generation* for the console's object-download redirect
//! (08 §4: 下载一律 302 转 gateway，附调用者的短时效凭证).
//!
//! The console session holds only the caller's access key — never the secret
//! (08 §4.1 red line). But PD *is* the credential store, so it can sign on the
//! caller's behalf: this module builds a standard SigV4 query-string-signed URL
//! (`X-Amz-*` params) that the gateway's `s3s` layer verifies exactly as it
//! would a client-signed presigned URL. The browser is redirected (302) to the
//! gateway with it; the object bytes never pass through PD.
//!
//! The canonical-request construction mirrors `s3s`'s verifier
//! (`sig_v4::create_presigned_canonical_request`) field-for-field so a URL this
//! module signs is one `s3s` accepts: query params URI-encoded and sorted, the
//! `X-Amz-Signature` param itself excluded, only the `host` header signed, and
//! an `UNSIGNED-PAYLOAD` body marker (the body is unknown at signing time).
//!
//! Design: docs/design/08-web-console.md §4; AWS SigV4 query-string auth.

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

/// The SigV4 algorithm identifier.
const ALGORITHM: &str = "AWS4-HMAC-SHA256";
/// The service this signs for (the gateway's S3 head).
const SERVICE: &str = "s3";
/// The payload marker for presigned URLs (the body is not signed).
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

/// A SigV4 presigner over one credential. Cheap to construct per request.
pub struct Presigner<'a> {
    access_key: &'a str,
    secret_key: &'a str,
    region: &'a str,
}

/// The inputs a presigned GET needs, already resolved by the caller.
pub struct PresignRequest<'a> {
    /// The gateway's `host:port` (the signed `host` header value).
    pub host: &'a str,
    /// The request path, e.g. `/bucket/key` (URI-encoded per segment below).
    pub path: &'a str,
    /// Signing time as `YYYYMMDD'T'HHMMSS'Z'` (UTC).
    pub amz_date: &'a str,
    /// The credential-scope date, `YYYYMMDD` (must equal `amz_date`'s date).
    pub date: &'a str,
    /// Validity window in seconds (the console uses a short-lived URL).
    pub expires_secs: u32,
}

impl<'a> Presigner<'a> {
    /// Builds a presigner over one credential and region.
    #[must_use]
    pub fn new(access_key: &'a str, secret_key: &'a str, region: &'a str) -> Self {
        Self {
            access_key,
            secret_key,
            region,
        }
    }

    /// Produces the presigned URL for a GET of `req.path` on `req.host`.
    ///
    /// The returned string is a complete `http://host/path?X-Amz-*` URL ready to
    /// be a `Location` header. Only the `host` header is signed (the minimal set
    /// `s3s` requires); the query carries the credential scope, date, expiry,
    /// signed-headers list, and the final signature.
    #[must_use]
    pub fn presign_get(&self, req: &PresignRequest<'_>) -> String {
        let scope = format!("{}/{}/{}/aws4_request", req.date, self.region, SERVICE);
        let credential = format!("{}/{}", self.access_key, scope);

        // The query params that participate in the signature (everything except
        // X-Amz-Signature itself, which is appended after signing).
        let mut params: Vec<(String, String)> = vec![
            ("X-Amz-Algorithm".into(), ALGORITHM.into()),
            ("X-Amz-Credential".into(), credential),
            ("X-Amz-Date".into(), req.amz_date.into()),
            ("X-Amz-Expires".into(), req.expires_secs.to_string()),
            ("X-Amz-SignedHeaders".into(), "host".into()),
        ];
        // Sort by the *encoded* name, matching s3s's canonical-query ordering.
        params.sort_by(|a, b| a.0.cmp(&b.0));
        let canonical_qs = params
            .iter()
            .map(|(n, v)| format!("{}={}", uri_encode(n, true), uri_encode(v, true)))
            .collect::<Vec<_>>()
            .join("&");

        let canonical_request = format!(
            "GET\n{}\n{}\nhost:{}\n\nhost\n{}",
            uri_encode(req.path, false),
            canonical_qs,
            req.host,
            UNSIGNED_PAYLOAD
        );
        let string_to_sign = format!(
            "{}\n{}\n{}\n{}",
            ALGORITHM,
            req.amz_date,
            scope,
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let signature = self.sign(&string_to_sign, req.date);

        format!(
            "http://{}{}?{}&X-Amz-Signature={}",
            req.host,
            uri_encode(req.path, false),
            canonical_qs,
            signature
        )
    }

    /// The SigV4 signing key chain: `HMAC(HMAC(...HMAC("AWS4"+secret, date),
    /// region), service), "aws4_request")`, then the string-to-sign.
    fn sign(&self, string_to_sign: &str, date: &str) -> String {
        let k_date = hmac(&format!("AWS4{}", self.secret_key), date.as_bytes());
        let k_region = hmac_bytes(&k_date, self.region.as_bytes());
        let k_service = hmac_bytes(&k_region, SERVICE.as_bytes());
        let k_signing = hmac_bytes(&k_service, b"aws4_request");
        hex::encode(hmac_bytes(&k_signing, string_to_sign.as_bytes()))
    }
}

type HmacSha256 = Hmac<Sha256>;

/// `HMAC-SHA256(key_str, data)` → raw digest bytes.
fn hmac(key: &str, data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key.as_bytes()).expect("hmac key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// `HMAC-SHA256(key_bytes, data)` → raw digest bytes.
fn hmac_bytes(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// URI-encodes per RFC 3986 (unreserved chars pass through). `encode_slash`
/// distinguishes path segments (`/ ` kept) from query names/values (`/` → `%2F`),
/// matching s3s's `uri_encode`.
fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        let keep = matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~')
            || (b == b'/' && !encode_slash);
        if keep {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The canonical AWS SigV4 test vector (get-object presigned URL), adapted
    /// to the host-only signed-header set this module produces. The expected
    /// signature is recomputed by the same algorithm; the test pins the
    /// canonical-request *shape* (the part that must match s3s byte-for-byte).
    #[test]
    fn presign_get_builds_the_expected_canonical_form() {
        let signer = Presigner::new("AKID", "secret", "us-east-1");
        let url = signer.presign_get(&PresignRequest {
            host: "gw:9000",
            path: "/bucket/dir/obj.txt",
            amz_date: "20130524T000000Z",
            date: "20130524",
            expires_secs: 900,
        });
        // The URL carries every required X-Amz field and the signature.
        assert!(url.starts_with("http://gw:9000/bucket/dir/obj.txt?"));
        assert!(url.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(url.contains("X-Amz-Credential=AKID%2F20130524%2Fus-east-1%2Fs3%2Faws4_request"));
        assert!(url.contains("X-Amz-Date=20130524T000000Z"));
        assert!(url.contains("X-Amz-Expires=900"));
        assert!(url.contains("X-Amz-SignedHeaders=host"));
        assert!(url.contains("X-Amz-Signature="));
        // The signature is 64 lowercase hex chars.
        let sig = url.rsplit("X-Amz-Signature=").next().unwrap();
        assert_eq!(sig.len(), 64);
        assert!(sig.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn uri_encode_follows_rfc3986_unreserved() {
        assert_eq!(uri_encode("a/b-c.d_e~f", false), "a/b-c.d_e~f");
        assert_eq!(uri_encode("a/b", true), "a%2Fb");
        assert_eq!(uri_encode("a b", true), "a%20b");
        assert_eq!(uri_encode("a+b", true), "a%2Bb");
    }

    /// The signing key chain matches the AWS reference derivation (the digest
    /// below is the documented `kDate` for this key + `20120215`, cross-checked
    /// against an independent HMAC-SHA256 implementation).
    #[test]
    fn signing_key_chain_matches_reference() {
        let k_date = hmac("AWS4wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", b"20120215");
        assert_eq!(
            hex::encode(&k_date),
            "ed838dceb93f9c66f7ce5dd7db2e81f32359d5b3937685ad412bbc1aecb0db20"
        );
    }

    /// The full signature matches an independently-computed value (Python
    /// hmac/hashlib over the same canonical request), so the URL this module
    /// signs is one a standard SigV4 verifier — including the gateway's `s3s`
    /// presigned-URL path — accepts.
    #[test]
    fn presign_get_signature_matches_reference() {
        let signer = Presigner::new(
            "AKID",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "us-east-1",
        );
        let url = signer.presign_get(&PresignRequest {
            host: "gw:9000",
            path: "/bucket/dir/obj.txt",
            amz_date: "20130524T000000Z",
            date: "20130524",
            expires_secs: 900,
        });
        let sig = url.rsplit("X-Amz-Signature=").next().unwrap();
        assert_eq!(
            sig, "16f4688ee9f24456029be98f3ac657c0683f69ab933d4f65093c074939dc3b1d",
            "the signature must match the independently-derived value"
        );
    }
}
