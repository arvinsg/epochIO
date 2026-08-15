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

//! Skill-registry consistency.
//!
//! A project skill's name is written in three places — the directory name, the
//! `SKILL.md` frontmatter, and the registry block in `AGENTS.md` — and an agent
//! that cannot find a listed skill, or that never sees an unlisted one, fails
//! silently. The reference harness this was modelled on had already drifted in
//! exactly this way, so the agreement is checked rather than remembered.

use std::path::PathBuf;

use crate::report::Outcome;
use crate::source;

const SKILLS_DIR: &str = ".agents/skills";
const REGISTRY_FILE: &str = "AGENTS.md";
const REGISTRY_START: &str = "<!-- SKILLS_TABLE_START -->";
const REGISTRY_END: &str = "<!-- SKILLS_TABLE_END -->";
const SKILL_PREFIX: &str = "ep-";

/// Every description must state its triggers with this phrase, or the skill is never
/// selected for the situations it exists for.
const TRIGGER_PHRASE: &str = "Use when";

pub fn run() -> std::process::ExitCode {
    crate::report::report("registry", check())
}

pub fn check() -> Result<Outcome, String> {
    let root = source::workspace_root();
    let dirs = skill_dirs(&root.join(SKILLS_DIR))?;
    let registered = registered_entries(&root.join(REGISTRY_FILE))?;
    let mut violations = Vec::new();

    for dir in &dirs {
        let name = dir
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| format!("non-UTF-8 skill directory: {}", dir.display()))?;

        if !name.starts_with(SKILL_PREFIX) {
            violations.push(format!(
                "{SKILLS_DIR}/{name}: project skills are prefixed '{SKILL_PREFIX}'"
            ));
        }

        let manifest = dir.join("SKILL.md");
        if !manifest.is_file() {
            violations.push(format!("{SKILLS_DIR}/{name}: no SKILL.md"));
            continue;
        }
        let content = std::fs::read_to_string(&manifest)
            .map_err(|e| format!("read {}: {e}", manifest.display()))?;

        match frontmatter_field(&content, "name") {
            Some(declared) if declared == name => {}
            Some(declared) => violations.push(format!(
                "{SKILLS_DIR}/{name}/SKILL.md: frontmatter name '{declared}' \
                 does not match the directory"
            )),
            None => violations.push(format!(
                "{SKILLS_DIR}/{name}/SKILL.md: no 'name:' in the frontmatter"
            )),
        }

        let described = frontmatter_field(&content, "description");
        if described.is_none() {
            violations.push(format!(
                "{SKILLS_DIR}/{name}/SKILL.md: no 'description:' in the frontmatter"
            ));
        } else if !described.is_some_and(|d| d.contains(TRIGGER_PHRASE)) {
            violations.push(format!(
                "{SKILLS_DIR}/{name}/SKILL.md: description must say '{TRIGGER_PHRASE}' — \
                 without triggers it is never selected"
            ));
        }

        match registered.iter().find(|entry| entry.name == name) {
            None => violations.push(format!(
                "{name} is not registered in {REGISTRY_FILE} — an unlisted skill is \
                 never invoked"
            )),
            Some(entry) if Some(entry.description.as_str()) != described => {
                violations.push(format!(
                    "{name}: the {REGISTRY_FILE} description differs from the SKILL.md \
                     frontmatter — the registry is what an agent reads first, so the two \
                     drifting means it is selected on stale triggers"
                ));
            }
            Some(_) => {}
        }
    }

    for entry in &registered {
        if !dirs
            .iter()
            .any(|d| d.file_name().is_some_and(|n| n == entry.name.as_str()))
        {
            violations.push(format!(
                "{REGISTRY_FILE} lists '{}' but {SKILLS_DIR}/{}/ does not exist",
                entry.name, entry.name
            ));
        }
    }

    Ok(Outcome::new(dirs.len(), "skills", violations))
}

/// Skill directories, sorted. A missing skills directory is not a violation —
/// the harness is allowed to carry no project skills.
fn skill_dirs(dir: &std::path::Path) -> Result<Vec<PathBuf>, String> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(|e| format!("read {}: {e}", dir.display()))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| p.is_dir())
        .collect();
    dirs.sort();
    Ok(dirs)
}

/// One `<skill>` entry from the registry block.
struct Entry {
    name: String,
    description: String,
}

/// A frontmatter scalar field from a leading `---` block.
fn frontmatter_field<'a>(content: &'a str, field: &str) -> Option<&'a str> {
    let prefix = format!("{field}:");
    let mut lines = content.lines();
    if lines.next()?.trim() != "---" {
        return None;
    }
    for line in lines {
        let trimmed = line.trim();
        if trimmed == "---" {
            return None;
        }
        if let Some(value) = trimmed.strip_prefix(&prefix) {
            return Some(value.trim().trim_matches('"').trim_matches('\''));
        }
    }
    None
}

/// `<skill>` entries inside the registry block, in document order.
fn registered_entries(file: &std::path::Path) -> Result<Vec<Entry>, String> {
    let content =
        std::fs::read_to_string(file).map_err(|e| format!("read {}: {e}", file.display()))?;
    let start = content
        .find(REGISTRY_START)
        .ok_or_else(|| format!("{REGISTRY_FILE}: missing {REGISTRY_START}"))?;
    let end = content
        .find(REGISTRY_END)
        .ok_or_else(|| format!("{REGISTRY_FILE}: missing {REGISTRY_END}"))?;
    if end < start {
        return Err(format!(
            "{REGISTRY_FILE}: registry markers are out of order"
        ));
    }
    Ok(skill_entries(&content[start..end]))
}

/// Pairs each `<name>` with the `<description>` from the same `<skill>` block, so a
/// block missing one of the two cannot silently borrow its neighbour's.
fn skill_entries(block: &str) -> Vec<Entry> {
    let mut entries = Vec::new();
    for chunk in block.split("<skill>").skip(1) {
        let body = chunk.split("</skill>").next().unwrap_or(chunk);
        let names = tagged_values(body, "name");
        let descriptions = tagged_values(body, "description");
        if let (Some(name), Some(description)) = (names.first(), descriptions.first()) {
            entries.push(Entry {
                name: name.clone(),
                description: description.clone(),
            });
        }
    }
    entries
}

fn tagged_values(block: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut values = Vec::new();
    let mut rest = block;

    while let Some(at) = rest.find(&open) {
        let after = &rest[at + open.len()..];
        let Some(end) = after.find(&close) else { break };
        values.push(after[..end].trim().to_string());
        rest = &after[end + close.len()..];
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frontmatter_name_is_read_from_the_leading_block() {
        let doc = "---\nname: ep-plan\ndescription: x. Use when y.\n---\n\n# ep-plan\n";
        assert_eq!(frontmatter_field(doc, "name"), Some("ep-plan"));
        assert_eq!(
            frontmatter_field(doc, "description"),
            Some("x. Use when y.")
        );
    }

    #[test]
    fn quoted_and_padded_names_are_unwrapped() {
        assert_eq!(
            frontmatter_field("---\nname:  \"ep-review\" \n---\n", "name"),
            Some("ep-review")
        );
        assert_eq!(
            frontmatter_field("---\nname: 'ep-commit'\n---\n", "name"),
            Some("ep-commit")
        );
    }

    #[test]
    fn a_name_after_the_frontmatter_does_not_count() {
        let doc = "---\ndescription: x\n---\n\nname: ep-fake\n";
        assert_eq!(frontmatter_field(doc, "name"), None);
    }

    #[test]
    fn missing_frontmatter_is_not_a_name() {
        assert_eq!(
            frontmatter_field("# ep-plan\n\nname: ep-plan\n", "name"),
            None
        );
    }

    #[test]
    fn tagged_values_reads_every_entry() {
        let block = "<skill>\n<name>ep-plan</name>\n</skill>\n<skill>\n<name> ep-review </name>\n";
        assert_eq!(tagged_values(block, "name"), ["ep-plan", "ep-review"]);
    }

    #[test]
    fn tagged_values_ignores_an_unclosed_tag() {
        assert_eq!(tagged_values("<name>ep-plan", "name"), Vec::<String>::new());
    }

    #[test]
    fn entries_pair_name_with_its_own_description() {
        let block = "\
<skill>
<name>ep-a</name>
<description>first. Use when a.</description>
</skill>
<skill>
<name>ep-b</name>
<description>second. Use when b.</description>
</skill>
";
        let entries = skill_entries(block);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "ep-a");
        assert_eq!(entries[0].description, "first. Use when a.");
        assert_eq!(entries[1].description, "second. Use when b.");
    }

    #[test]
    fn a_block_missing_a_description_does_not_borrow_the_next_one() {
        let block = "\
<skill>
<name>ep-a</name>
</skill>
<skill>
<name>ep-b</name>
<description>second. Use when b.</description>
</skill>
";
        let entries = skill_entries(block);
        assert_eq!(
            entries.len(),
            1,
            "the incomplete block is dropped, not merged"
        );
        assert_eq!(entries[0].name, "ep-b");
    }

    #[test]
    fn registry_and_skill_directories_agree() {
        let outcome = check().expect("check should run");
        assert!(
            outcome.is_clean(),
            "registry drift: {:?}",
            outcome.violations()
        );
    }

    #[test]
    fn every_registered_skill_states_its_triggers() {
        let entries = registered_entries(&source::workspace_root().join(REGISTRY_FILE))
            .expect("registry should parse");
        assert!(!entries.is_empty(), "the registry lists no skills");
        for entry in &entries {
            assert!(
                entry.description.contains(TRIGGER_PHRASE),
                "{}: description must say '{TRIGGER_PHRASE}'",
                entry.name
            );
        }
    }
}
