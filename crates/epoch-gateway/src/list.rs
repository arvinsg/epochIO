//! S3 LIST folding: `delimiter` → `CommonPrefixes` (RFC-free, S3-specific).
//!
//! Pure and side-effect free. A flat namespace stores keys as an ordered
//! dictionary, and S3's `delimiter` presents that ordering as a pseudo-hierarchy:
//! keys sharing a prefix up to the first delimiter *after* the requested prefix
//! collapse into one `CommonPrefixes` entry instead of appearing individually.
//!
//! Without this, `aws s3 ls s3://bucket/` returns every key in the bucket flat —
//! the directory view every S3 browser, `s3fs`, and rclone's traversal relies on
//! simply does not exist.
//!
//! ## Why folding needs a skip, not just a filter
//!
//! Folding cannot be "scan a page, group it": a prefix holding a million keys
//! would need a million rows scanned to emit one line, and every page after the
//! first would re-emit the same group. So a fold produces, along with its rows,
//! the key to **resume after** — for a folded group that is the group's upper
//! bound (its prefix with the last byte incremented), which the next page's
//! `start_after` jumps straight to. Cost then tracks the number of *groups*, not
//! the number of keys.
//!
//! Design: docs/design/03-metanode.md §5 (LIST 分页)

/// One row of a rendered LIST page: an object entry or a collapsed prefix.
///
/// Distinct from [`Row`] (which carries only keys, since folding is pure): a
/// rendered row keeps the object's head so the HTTP layer can emit size/etag.
#[derive(Debug, Clone)]
pub enum ListRow {
    /// An object, with its metadata entry.
    Object(epoch_proto::grpc::meta::ObjectEntry),
    /// A collapsed prefix (delimiter-terminated).
    Prefix(Vec<u8>),
}

/// A rendered LIST page: its rows plus the opaque continuation token.
#[derive(Debug, Clone, Default)]
pub struct ListPage {
    /// Rows in key order.
    pub rows: Vec<ListRow>,
    /// The `start_after` for the next page; `None` = this was the last page.
    pub next_token: Option<Vec<u8>>,
}

/// One row of a folded LIST page: either an object key or a collapsed prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Row {
    /// An object whose key holds no delimiter past the requested prefix.
    Key(Vec<u8>),
    /// A collapsed group: every key starting with this (delimiter-terminated)
    /// prefix, presented as one line.
    CommonPrefix(Vec<u8>),
}

/// The outcome of folding one scanned batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Folded {
    /// The page's rows, in key order, deduplicated.
    pub rows: Vec<Row>,
    /// The key to resume after on the next page, or `None` when the batch was
    /// consumed entirely (the caller then continues after the last scanned key).
    ///
    /// Set when folding stopped early — either the row budget filled, or a group
    /// was collapsed and the scan should skip past it.
    pub resume_after: Option<Vec<u8>>,
}

/// Folds `keys` (ascending, all sharing `prefix`) into at most `limit` rows.
///
/// `delimiter` is the S3 grouping separator; an empty delimiter means no folding
/// (every key is its own row). Returns the rows plus where to resume.
///
/// INVARIANT(design 03 §5): a folded group must be skippable. When a group is
/// emitted, `resume_after` is its exclusive upper bound, so the next page starts
/// *past* the whole group rather than at its second key. Otherwise a prefix with
/// a million keys costs a million scanned rows to render one line, and each page
/// re-emits the same `CommonPrefixes` entry forever.
#[must_use]
pub fn fold(keys: &[Vec<u8>], prefix: &[u8], delimiter: &[u8], limit: usize) -> Folded {
    if delimiter.is_empty() {
        // No folding: keys pass through, bounded by the budget. The last emitted
        // key is itself the resume point (the scan is exclusive).
        let rows: Vec<Row> = keys.iter().take(limit).cloned().map(Row::Key).collect();
        let resume_after = (keys.len() > limit)
            .then(|| keys.get(limit - 1).cloned())
            .flatten();
        return Folded { rows, resume_after };
    }

    let mut rows: Vec<Row> = Vec::new();
    let mut last_group: Option<Vec<u8>> = None;
    // The last key actually consumed into a row — the resume point when the row
    // budget cuts the page short (the store scan is `> start_after`).
    let mut last_consumed: Option<Vec<u8>> = None;
    for key in keys {
        // Only the part after the requested prefix is searched for a delimiter
        // (S3: the prefix itself may well contain the delimiter).
        let Some(rest) = key.strip_prefix(prefix) else {
            continue;
        };
        match find(rest, delimiter) {
            Some(at) => {
                // The group is prefix + rest-up-to-and-including the delimiter.
                let mut group = prefix.to_vec();
                group.extend_from_slice(&rest[..at + delimiter.len()]);
                if last_group.as_deref() == Some(group.as_slice()) {
                    continue; // already emitted; this key is inside it
                }
                if rows.len() == limit {
                    return Folded {
                        rows,
                        resume_after: last_consumed,
                    };
                }
                last_group = Some(group.clone());
                rows.push(Row::CommonPrefix(group));
                last_consumed = Some(key.clone());
            }
            None => {
                if rows.len() == limit {
                    return Folded {
                        rows,
                        resume_after: last_consumed,
                    };
                }
                rows.push(Row::Key(key.clone()));
                last_consumed = Some(key.clone());
            }
        }
    }
    // The batch is exhausted. If the final row is a collapsed group, resume past
    // the whole group so the next page skips its remaining keys; otherwise the
    // caller continues after the last scanned key on its own.
    let resume_after = match rows.last() {
        Some(Row::CommonPrefix(group)) => Some(upper_bound(group)),
        _ => None,
    };
    Folded { rows, resume_after }
}

/// The exclusive upper bound of everything starting with `group`: the prefix with
/// its final byte incremented (`a/` → `a0`), so `start_after` lands past the last
/// key of the group.
///
/// A trailing `0xFF` cannot be incremented in place, so its byte is dropped and
/// the carry applied to the previous one; an all-`0xFF` prefix has no bound below
/// the key space's end and yields the prefix itself (the next page then walks the
/// remainder key by key — correct, just not skipped).
#[must_use]
fn upper_bound(group: &[u8]) -> Vec<u8> {
    let mut out = group.to_vec();
    while let Some(last) = out.pop() {
        if last < 0xFF {
            out.push(last + 1);
            return out;
        }
    }
    group.to_vec()
}

/// Byte-substring search (no `str` assumption — keys are arbitrary bytes).
fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(list: &[&str]) -> Vec<Vec<u8>> {
        list.iter().map(|k| k.as_bytes().to_vec()).collect()
    }

    fn key_rows(folded: &Folded) -> Vec<String> {
        folded
            .rows
            .iter()
            .map(|r| match r {
                Row::Key(k) => format!("K:{}", String::from_utf8_lossy(k)),
                Row::CommonPrefix(p) => format!("P:{}", String::from_utf8_lossy(p)),
            })
            .collect()
    }

    #[test]
    fn no_delimiter_passes_keys_through() {
        let k = keys(&["a/1", "a/2", "b"]);
        let folded = fold(&k, b"", b"", 10);
        assert_eq!(key_rows(&folded), vec!["K:a/1", "K:a/2", "K:b"]);
        assert_eq!(folded.resume_after, None);
    }

    #[test]
    fn folds_keys_under_a_directory_into_one_prefix() {
        // The canonical `aws s3 ls s3://bucket/` case.
        let k = keys(&["a/1", "a/2", "a/3", "b/1", "top"]);
        let folded = fold(&k, b"", b"/", 10);
        assert_eq!(
            key_rows(&folded),
            vec!["P:a/", "P:b/", "K:top"],
            "each directory collapses to one line; a bare key stays a key"
        );
    }

    #[test]
    fn folding_is_relative_to_the_requested_prefix() {
        // Inside `a/`, the group is `a/x/` — the prefix's own delimiter does not
        // fold everything into one row.
        let k = keys(&["a/x/1", "a/x/2", "a/y/1", "a/z"]);
        let folded = fold(&k, b"a/", b"/", 10);
        assert_eq!(key_rows(&folded), vec!["P:a/x/", "P:a/y/", "K:a/z"]);
    }

    /// INVARIANT(design 03 §5): a folded group must be skippable, else a prefix
    /// with a million keys costs a million scanned rows per page.
    #[test]
    fn a_folded_group_resumes_past_the_whole_group() {
        let k = keys(&["a/1", "a/2", "a/3"]);
        let folded = fold(&k, b"", b"/", 10);
        assert_eq!(key_rows(&folded), vec!["P:a/"]);
        let resume = folded.resume_after.expect("resume past the group");
        assert_eq!(
            resume,
            b"a0".to_vec(),
            "`a/` + 1 = `a0`, which sorts above every `a/...` key"
        );
        // Sanity: the bound really is above every key of the group.
        for key in &k {
            assert!(
                key.as_slice() < resume.as_slice(),
                "{} must sort below the resume bound",
                String::from_utf8_lossy(key)
            );
        }
    }

    #[test]
    fn upper_bound_carries_over_trailing_max_bytes() {
        assert_eq!(upper_bound(b"a/"), b"a0".to_vec());
        assert_eq!(upper_bound(&[b'a', 0xFF]), b"b".to_vec());
        // All-0xFF has no bound below the end of the key space: return as-is
        // (the next page walks the remainder, which is correct if slower).
        assert_eq!(upper_bound(&[0xFF, 0xFF]), vec![0xFF, 0xFF]);
    }

    #[test]
    fn the_row_budget_stops_the_page_and_reports_where_to_resume() {
        let k = keys(&["a/1", "b/1", "c/1", "d/1"]);
        let folded = fold(&k, b"", b"/", 2);
        assert_eq!(key_rows(&folded), vec!["P:a/", "P:b/"]);
        assert!(
            folded.resume_after.is_some(),
            "a truncated page must say where to continue"
        );
    }

    #[test]
    fn keys_outside_the_prefix_are_ignored() {
        // The store scan is prefix-bounded, but folding must not mis-handle a
        // stray key (a truncated scan bound would otherwise fold wrongly).
        let k = keys(&["other", "a/1"]);
        let folded = fold(&k, b"a/", b"/", 10);
        assert_eq!(key_rows(&folded), vec!["K:a/1"]);
    }

    #[test]
    fn a_multibyte_delimiter_folds_too() {
        let k = keys(&["a::1", "a::2", "b"]);
        let folded = fold(&k, b"", b"::", 10);
        assert_eq!(key_rows(&folded), vec!["P:a::", "K:b"]);
    }

    #[test]
    fn an_empty_batch_folds_to_nothing() {
        let folded = fold(&[], b"", b"/", 10);
        assert!(folded.rows.is_empty());
        assert_eq!(folded.resume_after, None);
    }
}
