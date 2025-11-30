use claritasai_core::{Plan, PlanVerdict};

pub struct Verifier;

impl Verifier {
    pub fn new() -> Self { Self }
    pub fn review(&self, _plan: &Plan) -> PlanVerdict {
        PlanVerdict { status: "Approved".into(), rationale: "stub".into(), required_changes: None, notes: None }
    }
}
