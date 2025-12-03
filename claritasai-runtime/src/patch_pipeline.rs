use claritasai_core::{ApplyMode, ApplyOutcome, Patch, PreCommitCheck};

#[derive(Debug, Default, Clone)]
pub struct ValidationReport {
    pub ok: bool,
    pub checks: Vec<PreCommitCheck>,
    pub details: String,
}

#[derive(Debug, Default, Clone)]
pub struct CommitReport {
    pub applied: bool,
    pub outcome: ApplyOutcome,
    pub details: String,
}

/// PatchPipeline coordinates pre-commit checks (build/tests/lints) and guarded apply+rollback.
pub struct PatchPipeline;

impl PatchPipeline {
    pub fn new() -> Self { Self }

    /// Run pre-commit validation (stub: returns success and lists checks).
    pub async fn validate(&self, _patch: &Patch) -> anyhow::Result<ValidationReport> {
        Ok(ValidationReport {
            ok: true,
            checks: vec![PreCommitCheck::Build, PreCommitCheck::Tests, PreCommitCheck::Lints],
            details: "validation stub".into(),
        })
    }

    /// Apply with rollback on failure (stub: no-op apply outcome).
    pub async fn apply_with_rollback(&self, _patch: &Patch) -> anyhow::Result<CommitReport> {
        Ok(CommitReport {
            applied: true,
            outcome: ApplyOutcome { files_changed: 0, checks: vec![] },
            details: "apply stub".into(),
        })
    }
}
