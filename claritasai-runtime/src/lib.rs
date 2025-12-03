use claritasai_core::{Context, Plan, PlanVerdict};

pub mod roles;
pub mod patch_pipeline;
pub mod orchestrator;

pub use roles::{Role, RunState};
pub use patch_pipeline::{PatchPipeline, ValidationReport, CommitReport};
pub use orchestrator::{Orchestrator, OrchestratorConfig, RunId, Approval, ApprovalStatus};

pub struct Runtime;

impl Runtime {
    pub fn new() -> Self { Self }
    pub async fn plan(&self, _ctx: &mut Context, _objective: &str) -> anyhow::Result<Plan> {
        Ok(Plan { objective: _objective.to_string(), steps: vec![], project_id: None, parent_plan_id: None })
    }
    pub async fn verify(&self, _ctx: &mut Context, _plan: &Plan) -> anyhow::Result<PlanVerdict> {
        Ok(PlanVerdict { status: "Approved".into(), rationale: "stub".into(), required_changes: None, notes: None })
    }
    pub async fn execute(&self, _ctx: &mut Context, _plan: &Plan) -> anyhow::Result<()> { Ok(()) }
}
