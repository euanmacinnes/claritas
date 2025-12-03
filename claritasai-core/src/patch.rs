use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PatchFormat {
    UnifiedDiff,
    Structured,
}

impl Default for PatchFormat {
    fn default() -> Self { PatchFormat::UnifiedDiff }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Patch {
    pub format: PatchFormat,
    /// Human-readable description of the change
    #[serde(default)]
    pub description: String,
    /// When `UnifiedDiff`, store one or more diff hunks; when `Structured`, store serialized ops
    #[serde(default)]
    pub diffs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApplyMode { DryRun, Commit }

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DiffPreview {
    pub files_changed: usize,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ApplyOutcome {
    pub files_changed: usize,
    pub checks: Vec<PreCommitCheck>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PreCommitCheck { Build, Tests, Lints }
