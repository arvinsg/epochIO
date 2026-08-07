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

//! Embedded console frontend assets (08 §1/§7): the built frontend product is
//! compiled into the PD binary via `include_dir!` so `--role pd` serves
//! `/console` with zero external files — consistent with the "three services,
//! zero external dependencies" promise.
//!
//! The directory `crates/epoch-pd/console/` holds the shippable product
//! (`index.html` + logo SVGs); it is embedded at build time. Adding a file to
//! that directory makes it servable without touching this module.
//!
//! Design: docs/design/08-web-console.md §1, §7

use include_dir::{Dir, include_dir};

/// The embedded console frontend (compiled into the binary).
static CONSOLE_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/console");

/// One servable asset: its bytes and Content-Type.
pub struct Asset {
    /// The file bytes (borrowed from the embedded image; `'static`).
    pub bytes: &'static [u8],
    /// The MIME type for the `Content-Type` header.
    pub content_type: &'static str,
}

/// Looks up an embedded asset by its console-relative path (e.g. `index.html`,
/// `logo.svg`). Returns `None` for an unknown path. `""` and `index.html` both
/// resolve to the SPA entry point.
#[must_use]
pub fn get(path: &str) -> Option<Asset> {
    let rel = if path.is_empty() { "index.html" } else { path };
    let file = CONSOLE_DIR.get_file(rel)?;
    Some(Asset {
        bytes: file.contents(),
        content_type: content_type_for(rel),
    })
}

/// The SPA entry point (`index.html`) — served for the app root and any
/// client-routed path the SPA owns (deep links fall back to it).
#[must_use]
pub fn index() -> Asset {
    get("index.html").expect("console index.html is embedded")
}

/// Maps a file extension to its Content-Type. Small fixed table — the console
/// ships only HTML/SVG/CSS/JS.
fn content_type_for(path: &str) -> &'static str {
    match path.rsplit('.').next() {
        Some("html") => "text/html; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("json") => "application/json",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embeds_index_and_logos() {
        assert!(get("index.html").is_some(), "index.html is embedded");
        assert!(get("").is_some(), "empty path resolves to index");
        assert!(get("logo.svg").is_some());
        assert!(get("logo-compact.svg").is_some());
        assert!(get("nope.txt").is_none(), "unknown path is None");
    }

    #[test]
    fn content_types_match_extension() {
        assert_eq!(
            get("index.html").unwrap().content_type,
            "text/html; charset=utf-8"
        );
        assert_eq!(get("logo.svg").unwrap().content_type, "image/svg+xml");
    }
}
