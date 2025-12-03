use serde::{Deserialize, Serialize};

pub mod patch;
pub mod fs;
pub use patch::{Patch, PatchFormat, ApplyMode, ApplyOutcome, PreCommitCheck, DiffPreview};
pub use fs::SafeFs;
pub mod graph;
pub use graph::{ProjectGraph, TestTarget, LangKind, GraphMember};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub objective: String,
    pub steps: Vec<PlanStep>,
    #[serde(skip_serializing_if = "Option::is_none")] pub project_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub parent_plan_id: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")] pub required_changes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")] pub notes: Option<String>,
}

#[derive(Debug, Default)]
pub struct Context {}

#[allow(async_fn_in_trait)]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    async fn call(&self, _ctx: &Context, _input: serde_json::Value) -> anyhow::Result<serde_json::Value>;
}

// --------- New domain types for workspaces/projects and memory guidelines ---------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")] pub root_path: Option<String>,
    #[serde(default)] pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    /// Filesystem path to the project folder
    pub path: String,
    /// Optional VCS URL; repo may be initialized locally if empty
    #[serde(skip_serializing_if = "Option::is_none")] pub repo_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MemoryGuidelines {
    /// Free-form textual guidelines that should be considered during plan creation/execution.
    #[serde(default)] pub text: String,
    /// Arbitrary structured hints consumed by tools/agents.
    #[serde(default)] pub metadata: serde_json::Value,
}

impl MemoryGuidelines {
    /// Merge other into self, with other's fields taking precedence when set/non-empty.
    pub fn merged_with(&self, other: &MemoryGuidelines) -> MemoryGuidelines {
        let text = if !other.text.trim().is_empty() { other.text.clone() } else { self.text.clone() };
        let metadata = match (self.metadata.clone(), other.metadata.clone()) {
            (serde_json::Value::Null, m) => m,
            (m, serde_json::Value::Null) => m,
            (serde_json::Value::Object(mut a), serde_json::Value::Object(b)) => {
                for (k, v) in b { a.insert(k, v); }
                serde_json::Value::Object(a)
            }
            (_, m) => m, // prefer other when not both objects
        };
        MemoryGuidelines { text, metadata }
    }
}
