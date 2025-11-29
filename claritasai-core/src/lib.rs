use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub objective: String,
    pub steps: Vec<PlanStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStep {
    pub tool_ref: String,
    pub input: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanVerdict {
    pub status: String, // "Approved" | "NeedsChanges" | "Rejected"
    pub rationale: String,
}

#[derive(Debug, Default)]
pub struct Context {}

#[allow(async_fn_in_trait)]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    async fn call(&self, _ctx: &Context, _input: serde_json::Value) -> anyhow::Result<serde_json::Value>;
}
