use claritasai_core::{Plan, PlanVerdict};
use serde::{Deserialize, Serialize};

pub struct Verifier;

impl Verifier {
    pub fn new() -> Self { Self }
    pub fn review(&self, _plan: &Plan) -> PlanVerdict {
        PlanVerdict { status: "Approved".into(), rationale: "stub".into(), required_changes: None, notes: None }
    }
}

// ----- V2 verdict schema with strong typing -----
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum VerdictStatus { Approved, NeedsChanges }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanVerdictV2 {
    pub status: VerdictStatus,
    pub rationale: String,
    #[serde(default)]
    pub required_changes: Vec<String>,
}

impl From<PlanVerdictV2> for PlanVerdict {
    fn from(v: PlanVerdictV2) -> Self {
        let status = match v.status { VerdictStatus::Approved => "Approved", VerdictStatus::NeedsChanges => "NeedsChanges" }.to_string();
        PlanVerdict { status, rationale: v.rationale, required_changes: if v.required_changes.is_empty() { None } else { Some(v.required_changes) }, notes: None }
    }
}

impl From<PlanVerdict> for PlanVerdictV2 {
    fn from(v: PlanVerdict) -> Self {
        let status = match v.status.as_str() { "Approved" => VerdictStatus::Approved, _ => VerdictStatus::NeedsChanges };
        PlanVerdictV2 { status, rationale: v.rationale, required_changes: v.required_changes.unwrap_or_default() }
    }
}
