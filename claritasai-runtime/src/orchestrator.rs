use std::collections::HashMap;

// no serde derives needed on internal types here
use serde_json::json;
use tokio::sync::broadcast::Sender;

use crate::{Role, RunState};

pub type RunId = i64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalStatus { Approved, NeedsChanges }

#[derive(Debug, Clone)]
pub struct Approval {
    pub role: Role,
    pub status: ApprovalStatus,
    pub rationale: String,
    pub required_changes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    pub step_timeout_ms: u64,
}

impl Default for OrchestratorConfig {
    fn default() -> Self { Self { step_timeout_ms: 60_000 } }
}

#[derive(Debug, Default)]
pub struct Orchestrator {
    runs: HashMap<RunId, RunState>,
    tx: Option<Sender<String>>, // JSON-encoded SSE-like events
    cfg: OrchestratorConfig,
}

impl Orchestrator {
    pub fn new(cfg: OrchestratorConfig, tx: Option<Sender<String>>) -> Self {
        Self { runs: HashMap::new(), tx, cfg }
    }

    pub fn state_of(&self, run_id: RunId) -> Option<RunState> { self.runs.get(&run_id).copied() }

    pub fn start_run(&mut self, run_id: RunId) {
        self.runs.insert(run_id, RunState::Drafting);
        self.emit_state(run_id, Some("start"), None, None);
    }

    pub fn plan_saved(&mut self, run_id: RunId) {
        self.runs.insert(run_id, RunState::Reviewing);
        self.emit_state(run_id, Some("plan_saved"), None, None);
    }

    pub fn apply_qa_verdict(&mut self, run_id: RunId, approval: &Approval) {
        match approval.status {
            ApprovalStatus::Approved => {
                self.runs.insert(run_id, RunState::Approved);
            }
            ApprovalStatus::NeedsChanges => {
                self.runs.insert(run_id, RunState::Refining);
            }
        }
        self.emit_verdict(run_id, approval);
    }

    pub fn apply_manager_verdict(&mut self, run_id: RunId, approval: &Approval) {
        match approval.status {
            ApprovalStatus::Approved => {
                self.runs.insert(run_id, RunState::Executing);
            }
            ApprovalStatus::NeedsChanges => {
                self.runs.insert(run_id, RunState::Refining);
            }
        }
        self.emit_verdict(run_id, approval);
    }

    pub fn complete(&mut self, run_id: RunId) {
        self.runs.insert(run_id, RunState::Completed);
        self.emit_state(run_id, Some("completed"), None, None);
    }

    pub fn block(&mut self, run_id: RunId, reason: &str) {
        self.runs.insert(run_id, RunState::Blocked);
        self.emit_state(run_id, Some("blocked"), Some(reason), None);
    }

    pub fn resume(&mut self, run_id: RunId) {
        // On resume, return to Reviewing if we were refining/blocked, else keep current
        let next = match self.runs.get(&run_id).copied() {
            Some(RunState::Refining) | Some(RunState::Blocked) => RunState::Reviewing,
            Some(s) => s,
            None => RunState::Drafting,
        };
        self.runs.insert(run_id, next);
        self.emit_state(run_id, Some("resume"), None, None);
    }

    fn emit_verdict(&self, run_id: RunId, approval: &Approval) {
        let role_str = match approval.role { Role::Dev => "Dev", Role::QA => "QA", Role::Manager => "Manager" };
        let status_str = match approval.status { ApprovalStatus::Approved => "Approved", ApprovalStatus::NeedsChanges => "NeedsChanges" };
        self.emit_json(json!({
            "event": "orchestrator_verdict",
            "run_id": run_id,
            "role": role_str,
            "status": status_str,
            "rationale": approval.rationale,
            "required_changes": approval.required_changes,
        }));
        // Also emit a state snapshot
        if let Some(state) = self.runs.get(&run_id) {
            self.emit_state(run_id, Some("state_update"), None, Some(*state));
        }
    }

    fn emit_state(&self, run_id: RunId, stage: Option<&str>, message: Option<&str>, state: Option<RunState>) {
        let st = state.or_else(|| self.runs.get(&run_id).copied());
        let status = st.map(|s| format!("{:?}", s));
        self.emit_json(json!({
            "event": "orchestrator_state",
            "run_id": run_id,
            "stage": stage.unwrap_or(""),
            "status": status,
            "message": message,
        }));
    }

    fn emit_json(&self, v: serde_json::Value) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(v.to_string());
        }
    }
}
