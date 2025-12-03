use crate::{ApplyMode, ApplyOutcome, DiffPreview, Patch};

/// Safe filesystem wrapper with root-anchored operations and simple allow/deny controls.
#[derive(Debug, Clone, Default)]
pub struct SafeFs {
    pub root: std::path::PathBuf,
    pub allow: Vec<String>,
    pub deny: Vec<String>,
}

impl SafeFs {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self { Self { root: root.into(), ..Default::default() } }

    fn is_path_allowed(&self, _rel: &std::path::Path) -> bool {
        // Minimal placeholder: later implement proper allow/deny checks
        true
    }

    /// Dry-run application of a patch to produce a summary diff preview.
    pub async fn diff(&self, patch: &Patch) -> anyhow::Result<DiffPreview> {
        let summary = match patch.format {
            crate::PatchFormat::UnifiedDiff => format!("{} diff hunk(s)", patch.diffs.len()),
            crate::PatchFormat::Structured => "structured ops".into(),
        };
        Ok(DiffPreview { files_changed: 0, summary })
    }

    /// Apply a patch either as a dry-run or commit. Guarded checks will be handled by PatchPipeline.
    pub async fn apply(&self, patch: &Patch, mode: ApplyMode) -> anyhow::Result<ApplyOutcome> {
        match mode {
            ApplyMode::DryRun => Ok(ApplyOutcome { files_changed: 0, checks: vec![] }),
            ApplyMode::Commit => {
                // Placeholder: no-op write until full implementation
                Ok(ApplyOutcome { files_changed: 0, checks: vec![] })
            }
        }
    }
}
