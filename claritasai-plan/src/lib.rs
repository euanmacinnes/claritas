use serde::{Deserialize, Serialize};
use serde_json::Value;
use claritasai_core::{Plan as CorePlan, PlanStep as CorePlanStep};

/// V2 Planning schema with governance fields. Backward compatible conversion to core Plan.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Plan {
    pub objective: String,
    #[serde(default)] pub assumptions: Vec<String>,
    #[serde(default)] pub constraints: Vec<String>,
    #[serde(default)] pub risks: Vec<String>,
    #[serde(default)] pub steps: Vec<PlanStep>,
    #[serde(default)] pub assertions: Vec<String>,
    #[serde(default)] pub rollback: Vec<PlanStep>,
    #[serde(skip_serializing_if = "Option::is_none")] pub project_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub parent_plan_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStep {
    pub id: String,
    pub tool: String,
    #[serde(default)] pub params: Value,
    #[serde(default)] pub expects: Vec<String>,
}

impl From<Plan> for CorePlan {
    fn from(p: Plan) -> Self {
        let steps: Vec<CorePlanStep> = p
            .steps
            .into_iter()
            .map(|s| CorePlanStep { tool_ref: s.tool, input: s.params })
            .collect();
        CorePlan {
            objective: p.objective,
            steps,
            project_id: p.project_id,
            parent_plan_id: p.parent_plan_id,
        }
    }
}

pub struct Planner;

impl Planner {
    pub fn new() -> Self { Self }

    /// Draft a simple cross-domain plan (placeholder until LLM-driven planner is wired).
    pub fn draft_plan(&self, objective: &str) -> Plan {
        let steps = vec![
            PlanStep { id: "meta_ping".into(), tool: "meta.ping".into(), params: serde_json::json!({}), expects: vec!["ok".into()] },
            PlanStep { id: "py_lint".into(), tool: "python.lint".into(), params: serde_json::json!({"path":"."}), expects: vec!["clean".into()] },
            PlanStep { id: "rs_build".into(), tool: "rust.cargo.build.debug".into(), params: serde_json::json!({"path":"."}), expects: vec!["build_succeeds".into()] },
            PlanStep { id: "db_sql_validate".into(), tool: "clarium.validate_sql".into(), params: serde_json::json!({"sql":"SELECT 1"}), expects: vec!["valid".into()] },
        ];
        Plan {
            objective: objective.to_string(),
            steps,
            ..Default::default()
        }
    }
}
