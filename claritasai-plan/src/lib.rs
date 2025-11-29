use claritasai_core::{Plan, PlanStep};

pub struct Planner;

impl Planner {
    pub fn new() -> Self { Self }
    pub fn draft_plan(&self, objective: &str) -> Plan {
        Plan { objective: objective.to_string(), steps: vec![
            PlanStep{ tool_ref: "meta.ping".into(), input: serde_json::json!({}) }
        ] }
    }
}
