use claritasai_core::{Context, Plan, PlanVerdict};

pub struct Runtime;

impl Runtime {
    pub fn new() -> Self { Self }
    pub async fn plan(&self, _ctx: &mut Context, _objective: &str) -> anyhow::Result<Plan> {
        Ok(Plan { objective: _objective.to_string(), steps: vec![] })
    }
    pub async fn verify(&self, _ctx: &mut Context, _plan: &Plan) -> anyhow::Result<PlanVerdict> {
        Ok(PlanVerdict { status: "Approved".into(), rationale: "stub".into() })
    }
    pub async fn execute(&self, _ctx: &mut Context, _plan: &Plan) -> anyhow::Result<()> { Ok(()) }
}
