use claritasai_core::{Plan, PlanStep};

pub struct Planner;

impl Planner {
    pub fn new() -> Self { Self }
    pub fn draft_plan(&self, objective: &str) -> Plan {
        // Simple multi-step plan to exercise the pipeline across domains.
        // Later, this will be replaced by an LLM/MCP-driven planner.
        let mut steps: Vec<PlanStep> = Vec::new();
        // 0) meta ping (prove toolchain)
        steps.push(PlanStep { tool_ref: "meta.ping".into(), input: serde_json::json!({}) });
        // 1) python lint (path configurable at runtime; default to ".")
        steps.push(PlanStep { tool_ref: "python.lint".into(), input: serde_json::json!({ "path": "." }) });
        // 2) rust cargo build debug (default ".")
        steps.push(PlanStep { tool_ref: "rust.cargo.build.debug".into(), input: serde_json::json!({ "path": "." }) });
        // 3) clarium validate simple SQL
        steps.push(PlanStep { tool_ref: "clarium.validate_sql".into(), input: serde_json::json!({ "sql": "SELECT 1" }) });

        Plan { objective: objective.to_string(), steps, project_id: None, parent_plan_id: None }
    }
}
