use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum LangKind {
    Rust,
    Python,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphMember {
    pub kind: LangKind,
    pub name: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestTarget {
    pub member: String,
    pub command: String,
    pub path: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectGraph {
    pub members: Vec<GraphMember>,
    pub tests: Vec<TestTarget>,
}

impl ProjectGraph {
    /// Discover a basic project graph from a workspace root.
    /// Heuristics (MVP):
    /// - Rust: any immediate subfolder containing a Cargo.toml is a member
    /// - Python: any immediate subfolder containing a pyproject.toml or tests/ directory is a member
    pub async fn from_workspace(root: &Path) -> anyhow::Result<Self> {
        let mut members: Vec<GraphMember> = Vec::new();
        let mut tests: Vec<TestTarget> = Vec::new();

        let root = root.to_path_buf();
        if !root.exists() {
            return Ok(ProjectGraph { members, tests });
        }

        // include the root itself for Rust if has Cargo.toml
        let root_cargo = root.join("Cargo.toml");
        if root_cargo.exists() {
            let name = root
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "workspace".into());
            members.push(GraphMember { kind: LangKind::Rust, name: name.clone(), path: root.to_string_lossy().to_string() });
            tests.push(TestTarget { member: name.clone(), command: "cargo test".into(), path: Some(root.to_string_lossy().to_string()) });
        }

        // scan first-level subdirectories
        if let Ok(entries) = fs::read_dir(&root) {
            for e in entries.flatten() {
                let p = e.path();
                if !p.is_dir() { continue; }
                let cargo = p.join("Cargo.toml");
                let pyproject = p.join("pyproject.toml");
                let tests_dir = p.join("tests");
                // Rust crate
                if cargo.exists() {
                    let name = p.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_else(|| "crate".into());
                    let path_s = p.to_string_lossy().to_string();
                    members.push(GraphMember { kind: LangKind::Rust, name: name.clone(), path: path_s.clone() });
                    tests.push(TestTarget { member: name, command: "cargo test".into(), path: Some(path_s) });
                    continue;
                }
                // Python package
                if pyproject.exists() || tests_dir.exists() {
                    let name = p.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_else(|| "python".into());
                    let path_s = p.to_string_lossy().to_string();
                    members.push(GraphMember { kind: LangKind::Python, name: name.clone(), path: path_s.clone() });
                    tests.push(TestTarget { member: name, command: "pytest -q".into(), path: Some(path_s) });
                }
            }
        }

        Ok(ProjectGraph { members, tests })
    }

    /// Return a naive set of impacted tests given changed files.
    /// MVP: if a file path contains a member.path substring, include that member's test target; otherwise, return all tests when unsure.
    pub fn impacted_tests(&self, changed_files: &[PathBuf]) -> Vec<TestTarget> {
        if changed_files.is_empty() {
            return self.tests.clone();
        }
        let mut out: Vec<TestTarget> = Vec::new();
        'outer: for t in &self.tests {
            if let Some(member) = self.members.iter().find(|m| m.name == t.member) {
                for cf in changed_files {
                    let cf_s = cf.to_string_lossy();
                    if cf_s.contains(&member.path) {
                        out.push(t.clone());
                        continue 'outer;
                    }
                }
            }
        }
        if out.is_empty() { self.tests.clone() } else { out }
    }
}
