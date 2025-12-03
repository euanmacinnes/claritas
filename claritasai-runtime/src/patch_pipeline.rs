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
            let build_timeout = 120u64;
            let build_res = self.registry.call_with_timeout(
                "rust.cargo.build.debug",
                json!({"path": root}),
                build_timeout,
            ).await;
            match build_res {
                Ok(val) => {
                    checks.push(PreCommitCheck::Build);
                    details.push("rust build ok".into());
                    let mut ev = self.emit_gate_finished("rust.cargo.build.debug", root, true, g_start.elapsed().as_millis(), None, None);
                    let (so, se) = extract_snips(&val);
                    ev.stdout_snip = so; ev.stderr_snip = se;
                    gate_events.push(ev);
                }
                Err(e) => {
                    ok = false;
                    let msg = format!(
                        "rust build failed: {}",
                        friendly_mcp_error("rust", &e.to_string())
                    );
                    details.push(msg.clone());
                    let timeout_ms = if e.to_string().contains("timeout waiting for MCP response") { Some(build_timeout * 1000) } else { None };
                    let ev = self.emit_gate_finished("rust.cargo.build.debug", root, false, g_start.elapsed().as_millis(), Some(msg), timeout_ms);
                    gate_events.push(ev);
                }
            }
            // Tests
            gate_events.push(self.emit_gate_started("rust.cargo.test", root));
            let g_start = Instant::now();
            let test_timeout = 120u64;
            let test_res = self.registry.call_with_timeout(
                "rust.cargo.test",
                json!({"path": root}),
                test_timeout,
            ).await;
            match test_res {
                Ok(val) => {
                    checks.push(PreCommitCheck::Tests);
                    details.push("rust tests ok".into());
                    let mut ev = self.emit_gate_finished("rust.cargo.test", root, true, g_start.elapsed().as_millis(), None, None);
                    let (so, se) = extract_snips(&val);
                    ev.stdout_snip = so; ev.stderr_snip = se;
                    gate_events.push(ev);
                }
                Err(e) => {
                    ok = false;
                    let msg = format!("rust tests failed: {}", friendly_mcp_error("rust", &e.to_string()));
                    details.push(msg.clone());
                    let timeout_ms = if e.to_string().contains("timeout waiting for MCP response") { Some(test_timeout * 1000) } else { None };
                    let ev = self.emit_gate_finished("rust.cargo.test", root, false, g_start.elapsed().as_millis(), Some(msg), timeout_ms);
                    gate_events.push(ev);
                }
            }
            // Lints (clippy)
            gate_events.push(self.emit_gate_started("rust.clippy", root));
            let g_start = Instant::now();
            let clippy_timeout = 60u64;
            let clippy_res = self.registry.call_with_timeout(
                "rust.clippy",
                json!({"path": root}),
                clippy_timeout,
            ).await;
            match clippy_res {
                Ok(val) => {
                    checks.push(PreCommitCheck::Lints);
                    details.push("rust clippy ok".into());
                    let mut ev = self.emit_gate_finished("rust.clippy", root, true, g_start.elapsed().as_millis(), None, None);
                    let (so, se) = extract_snips(&val);
                    ev.stdout_snip = so; ev.stderr_snip = se;
                    gate_events.push(ev);
                }
                Err(e) => {
                    ok = false;
                    let msg = format!("rust clippy failed: {}", friendly_mcp_error("rust", &e.to_string()));
                    details.push(msg.clone());
                    let timeout_ms = if e.to_string().contains("timeout waiting for MCP response") { Some(clippy_timeout * 1000) } else { None };
                    let ev = self.emit_gate_finished("rust.clippy", root, false, g_start.elapsed().as_millis(), Some(msg), timeout_ms);
                    gate_events.push(ev);
                }
            }
        }

        // Python gates (best-effort)
        if let Some(root) = &self.python_root {
            // Lint
            gate_events.push(self.emit_gate_started("python.lint", root));
            let g_start = Instant::now();
            let py_lint_timeout = 30u64;
            let pylint_res = self.registry.call_with_timeout("python.lint", json!({"path": root}), py_lint_timeout).await;
            match pylint_res {
                Ok(val) => {
                    if !checks.contains(&PreCommitCheck::Lints) { checks.push(PreCommitCheck::Lints); }
                    details.push("python lint ok".into());
                    let mut ev = self.emit_gate_finished("python.lint", root, true, g_start.elapsed().as_millis(), None, None);
                    let (so, se) = extract_snips(&val);
                    ev.stdout_snip = so; ev.stderr_snip = se;
                    gate_events.push(ev);
                }
                Err(e) => {
                    ok = false;
                    let msg = format!("python lint failed: {}", friendly_mcp_error("python", &e.to_string()));
                    details.push(msg.clone());
                    let timeout_ms = if e.to_string().contains("timeout waiting for MCP response") { Some(py_lint_timeout * 1000) } else { None };
                    let ev = self.emit_gate_finished("python.lint", root, false, g_start.elapsed().as_millis(), Some(msg), timeout_ms);
                    gate_events.push(ev);
                }
            }
            // Unit tests
            gate_events.push(self.emit_gate_started("python.test.unit", root));
            let g_start = Instant::now();
            let py_test_timeout = 90u64;
            let pytest_res = self.registry.call_with_timeout(
                "python.test.unit",
                json!({"path": root}),
                py_test_timeout,
            ).await;
            match pytest_res {
                Ok(val) => {
                    if !checks.contains(&PreCommitCheck::Tests) { checks.push(PreCommitCheck::Tests); }
                    details.push("python unit tests ok".into());
                    let mut ev = self.emit_gate_finished("python.test.unit", root, true, g_start.elapsed().as_millis(), None, None);
                    let (so, se) = extract_snips(&val);
                    ev.stdout_snip = so; ev.stderr_snip = se;
                    gate_events.push(ev);
                }
                Err(e) => {
                    ok = false;
                    let msg = format!("python unit tests failed: {}", friendly_mcp_error("python", &e.to_string()));
                    details.push(msg.clone());
                    let timeout_ms = if e.to_string().contains("timeout waiting for MCP response") { Some(py_test_timeout * 1000) } else { None };
                    let ev = self.emit_gate_finished("python.test.unit", root, false, g_start.elapsed().as_millis(), Some(msg), timeout_ms);
                    gate_events.push(ev);
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

fn extract_snips(val: &serde_json::Value) -> (Option<String>, Option<String>) {
    // Try common keys and produce trimmed snippets
    fn trim(s: &str, cap: usize) -> String {
        let mut out = s.to_string();
        if out.len() > cap { out.truncate(cap); }
        out
    }
    let cap: usize = 16 * 1024; // 16KB snippets
    let mut so: Option<String> = None;
    let mut se: Option<String> = None;
    if let Some(s) = val.get("stdout").and_then(|x| x.as_str()) {
        so = Some(trim(s, cap));
    }
    if let Some(s) = val.get("stderr").and_then(|x| x.as_str()) {
        se = Some(trim(s, cap));
    }
    if so.is_none() {
        if let Some(s) = val.get("out").and_then(|x| x.as_str()) {
            so = Some(trim(s, cap));
        }
    }
    if se.is_none() {
        if let Some(s) = val.get("err").and_then(|x| x.as_str()) {
            se = Some(trim(s, cap));
        }
    }
    // If value is a string, treat as stdout
    if so.is_none() && val.is_string() {
        if let Some(s) = val.as_str() { so = Some(trim(s, cap)); }
    }
    (so, se)
}

#[derive(Debug, Clone, Default)]
pub struct GateEvent {
    pub gate: String,
    pub root: String,
    pub ok: bool,
    pub duration_ms: u128,
    pub message: Option<String>,
    pub timeout_ms: Option<u64>,
    /// Optional short outputs for UI/log artifacts (when available from hosts)
    pub stdout_snip: Option<String>,
    pub stderr_snip: Option<String>,
}

impl GateEvent {
    fn started(gate: &str, root: &str) -> Self {
        Self { gate: gate.to_string(), root: root.to_string(), ok: false, duration_ms: 0, message: None, timeout_ms: None, stdout_snip: None, stderr_snip: None }
    }
    fn finished(gate: &str, root: &str, ok: bool, duration_ms: u128, message: Option<String>, timeout_ms: Option<u64>) -> Self {
        Self { gate: gate.to_string(), root: root.to_string(), ok, duration_ms, message, timeout_ms, stdout_snip: None, stderr_snip: None }
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
    fn emit_gate_finished(&self, gate: &str, root: &str, ok: bool, duration_ms: u128, message: Option<String>, timeout_ms: Option<u64>) -> GateEvent {
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(json!({
                "event": "gate_finished",
                "tool": gate,
                "status": if ok { "ok" } else { "failed" },
                "message": message,
                "json": {"duration_ms": duration_ms, "root": root, "timeout_ms": timeout_ms},
            }).to_string());
        }
        GateEvent::finished(gate, root, ok, duration_ms, message, timeout_ms)
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
