use claritasai_core::{ApplyMode, ApplyOutcome, Patch, PreCommitCheck, SafeFs};
use claritasai_mcp::ToolRegistry;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::broadcast::Sender as BcSender;
use tokio::time::Instant;

#[derive(Debug, Default, Clone)]
pub struct ValidationReport {
    pub ok: bool,
    pub checks: Vec<PreCommitCheck>,
    pub details: String,
    /// Per-gate telemetry (start/finish/duration/status)
    pub gate_events: Vec<GateEvent>,
    /// Total validation duration in milliseconds
    pub duration_ms: u128,
}

#[derive(Debug, Default, Clone)]
pub struct CommitReport {
    pub applied: bool,
    pub outcome: ApplyOutcome,
    pub details: String,
    /// Per-gate telemetry captured during pre/post validation
    pub gate_events: Vec<GateEvent>,
    /// Total end-to-end duration in milliseconds
    pub duration_ms: u128,
    /// Optional structured summary (e.g., { pre: {...}, post: {...} })
    pub summary_json: serde_json::Value,
}

/// PatchPipeline coordinates pre-commit checks (build/tests/lints) and guarded apply+rollback.
pub struct PatchPipeline {
    registry: Arc<ToolRegistry>,
    /// Optional workspace roots for targeted checks
    pub rust_root: Option<String>,
    pub python_root: Option<String>,
    /// Optional SSE-like event emitter (JSON strings)
    pub event_tx: Option<BcSender<String>>,
}

impl PatchPipeline {
    pub fn new(registry: Arc<ToolRegistry>) -> Self {
        Self { registry, rust_root: None, python_root: None, event_tx: None }
    }

    pub fn with_rust_root(mut self, root: impl Into<String>) -> Self {
        self.rust_root = Some(root.into());
        self
    }

    pub fn with_python_root(mut self, root: impl Into<String>) -> Self {
        self.python_root = Some(root.into());
        self
    }

    pub fn with_event_tx(mut self, tx: Option<BcSender<String>>) -> Self {
        self.event_tx = tx;
        self
    }

    /// Run pre-commit validation (stub: returns success and lists checks).
    pub async fn validate(&self, _patch: &Patch) -> anyhow::Result<ValidationReport> {
        let started = Instant::now();
        let mut checks: Vec<PreCommitCheck> = Vec::new();
        let mut details: Vec<String> = Vec::new();
        let mut ok = true;
        let mut gate_events: Vec<GateEvent> = Vec::new();

        // Rust gates (best-effort)
        if let Some(root) = &self.rust_root {
            // Build
            gate_events.push(self.emit_gate_started("rust.cargo.build.debug", root));
            let g_start = Instant::now();
            let build_res = self.registry.call_with_timeout(
                "rust.cargo.build.debug",
                json!({"path": root}),
                120,
            ).await;
            match build_res {
                Ok(_) => {
                    checks.push(PreCommitCheck::Build);
                    details.push("rust build ok".into());
                    gate_events.push(self.emit_gate_finished("rust.cargo.build.debug", root, true, g_start.elapsed().as_millis(), None));
                }
                Err(e) => {
                    ok = false;
                    let msg = format!(
                        "rust build failed: {}",
                        friendly_mcp_error("rust", &e.to_string())
                    );
                    details.push(msg.clone());
                    gate_events.push(self.emit_gate_finished("rust.cargo.build.debug", root, false, g_start.elapsed().as_millis(), Some(msg)));
                }
            }
            // Tests
            gate_events.push(self.emit_gate_started("rust.cargo.test", root));
            let g_start = Instant::now();
            let test_res = self.registry.call_with_timeout(
                "rust.cargo.test",
                json!({"path": root}),
                120,
            ).await;
            match test_res {
                Ok(_) => {
                    checks.push(PreCommitCheck::Tests);
                    details.push("rust tests ok".into());
                    gate_events.push(self.emit_gate_finished("rust.cargo.test", root, true, g_start.elapsed().as_millis(), None));
                }
                Err(e) => {
                    ok = false;
                    let msg = format!("rust tests failed: {}", friendly_mcp_error("rust", &e.to_string()));
                    details.push(msg.clone());
                    gate_events.push(self.emit_gate_finished("rust.cargo.test", root, false, g_start.elapsed().as_millis(), Some(msg)));
                }
            }
            // Lints (clippy)
            gate_events.push(self.emit_gate_started("rust.clippy", root));
            let g_start = Instant::now();
            let clippy_res = self.registry.call_with_timeout(
                "rust.clippy",
                json!({"path": root}),
                60,
            ).await;
            match clippy_res {
                Ok(_) => {
                    checks.push(PreCommitCheck::Lints);
                    details.push("rust clippy ok".into());
                    gate_events.push(self.emit_gate_finished("rust.clippy", root, true, g_start.elapsed().as_millis(), None));
                }
                Err(e) => {
                    ok = false;
                    let msg = format!("rust clippy failed: {}", friendly_mcp_error("rust", &e.to_string()));
                    details.push(msg.clone());
                    gate_events.push(self.emit_gate_finished("rust.clippy", root, false, g_start.elapsed().as_millis(), Some(msg)));
                }
            }
        }

        // Python gates (best-effort)
        if let Some(root) = &self.python_root {
            // Lint
            gate_events.push(self.emit_gate_started("python.lint", root));
            let g_start = Instant::now();
            let pylint_res = self.registry.call_with_timeout("python.lint", json!({"path": root}), 30).await;
            match pylint_res {
                Ok(_) => {
                    if !checks.contains(&PreCommitCheck::Lints) { checks.push(PreCommitCheck::Lints); }
                    details.push("python lint ok".into());
                    gate_events.push(self.emit_gate_finished("python.lint", root, true, g_start.elapsed().as_millis(), None));
                }
                Err(e) => {
                    ok = false;
                    let msg = format!("python lint failed: {}", friendly_mcp_error("python", &e.to_string()));
                    details.push(msg.clone());
                    gate_events.push(self.emit_gate_finished("python.lint", root, false, g_start.elapsed().as_millis(), Some(msg)));
                }
            }
            // Unit tests
            gate_events.push(self.emit_gate_started("python.test.unit", root));
            let g_start = Instant::now();
            let pytest_res = self.registry.call_with_timeout(
                "python.test.unit",
                json!({"path": root}),
                90,
            ).await;
            match pytest_res {
                Ok(_) => {
                    if !checks.contains(&PreCommitCheck::Tests) { checks.push(PreCommitCheck::Tests); }
                    details.push("python unit tests ok".into());
                    gate_events.push(self.emit_gate_finished("python.test.unit", root, true, g_start.elapsed().as_millis(), None));
                }
                Err(e) => {
                    ok = false;
                    let msg = format!("python unit tests failed: {}", friendly_mcp_error("python", &e.to_string()));
                    details.push(msg.clone());
                    gate_events.push(self.emit_gate_finished("python.test.unit", root, false, g_start.elapsed().as_millis(), Some(msg)));
                }
            }
        }

        Ok(ValidationReport { ok, checks, details: details.join("; "), gate_events, duration_ms: started.elapsed().as_millis() })
    }

    /// Apply with rollback on failure (guarded by pre/post validation).
    pub async fn apply_with_rollback(&self, patch: &Patch, fs: &SafeFs) -> anyhow::Result<CommitReport> {
        let started = Instant::now();
        // Dry run
        let _ = fs.diff(patch).await?;
        // Pre-commit checks (before write)
        let pre = self.validate(patch).await?;
        if !pre.ok {
            return Ok(CommitReport {
                applied: false,
                outcome: ApplyOutcome::default(),
                details: format!("pre-commit checks failed: {}", pre.details),
                gate_events: pre.gate_events,
                duration_ms: started.elapsed().as_millis(),
                summary_json: json!({
                    "pre": {"ok": false, "details": pre.details}
                }),
            });
        }

        // Commit
        let outcome = fs.apply(patch, ApplyMode::Commit).await?;

        // Post-commit validation
        let post = self.validate(patch).await?;
        if !post.ok {
            // TODO: implement rollback when SafeFs supports transactional writes or VCS integration
            return Ok(CommitReport {
                applied: false,
                outcome,
                details: format!("post-commit checks failed; rollback not implemented: {}", post.details),
                gate_events: post.gate_events,
                duration_ms: started.elapsed().as_millis(),
                summary_json: json!({
                    "pre": {"ok": true},
                    "post": {"ok": false, "details": post.details}
                }),
            });
        }

        let mut combined = pre.gate_events;
        combined.extend(post.gate_events);
        Ok(CommitReport {
            applied: true,
            outcome,
            details: "apply ok".into(),
            gate_events: combined,
            duration_ms: started.elapsed().as_millis(),
            summary_json: json!({"pre": {"ok": true}, "post": {"ok": true}}),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct GateEvent {
    pub gate: String,
    pub root: String,
    pub ok: bool,
    pub duration_ms: u128,
    pub message: Option<String>,
}

impl GateEvent {
    fn started(gate: &str, root: &str) -> Self {
        Self { gate: gate.to_string(), root: root.to_string(), ok: false, duration_ms: 0, message: None }
    }
    fn finished(gate: &str, root: &str, ok: bool, duration_ms: u128, message: Option<String>) -> Self {
        Self { gate: gate.to_string(), root: root.to_string(), ok, duration_ms, message }
    }
}

impl PatchPipeline {
    fn emit_gate_started(&self, gate: &str, root: &str) -> GateEvent {
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(json!({
                "event": "gate_started",
                "tool": gate,
                "status": "started",
                "message": format!("root={}", root),
            }).to_string());
        }
        GateEvent::started(gate, root)
    }
    fn emit_gate_finished(&self, gate: &str, root: &str, ok: bool, duration_ms: u128, message: Option<String>) -> GateEvent {
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(json!({
                "event": "gate_finished",
                "tool": gate,
                "status": if ok { "ok" } else { "failed" },
                "message": message,
                "json": {"duration_ms": duration_ms, "root": root},
            }).to_string());
        }
        GateEvent::finished(gate, root, ok, duration_ms, message)
    }
}

fn friendly_mcp_error(domain: &str, err: &str) -> String {
    if err.contains("no server registered") {
        return format!(
            "{} MCP host not available. Ensure it is configured and running (see configs/claritasai.yaml) or start locally via claritasai-app.",
            domain
        );
    }
    if err.contains("timeout waiting for MCP response") {
        return format!(
            "{} tool timed out. Consider increasing timeouts or checking the host logs. Ensure the {} MCP host is healthy.",
            domain, domain
        );
    }
    err.to_string()
}
