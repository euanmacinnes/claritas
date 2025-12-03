use axum::{
    extract::{Form, State, Path as AxPath, Query},
    response::{Html, IntoResponse, Sse},
    response::sse::Event,
    routing::get,
    Router,
};
use std::sync::Arc;
use std::time::Duration;
use claritasai_mcp::{StdioClient, ToolRegistry};
use claritasai_plan::Planner;
use claritasai_verify::Verifier;
use claritasai_agents::AgentsHarness;
// use claritasai_core types if needed later
use tokio_postgres as pg;
use serde_json::json;
use tokio::sync::broadcast;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tokio::time::Instant;
use tokio_stream::wrappers::BroadcastStream;
use claritasai_notify::{NotifierHub, NotifyMessage};
use claritasai_core::{Workspace, Project, MemoryGuidelines, Plan, Patch, PatchFormat, SafeFs};
use claritasai_runtime::{Orchestrator, OrchestratorConfig, Approval, ApprovalStatus, Role, PatchPipeline};
use serde::{Serialize, Deserialize};
use std::fs;
use std::path::{Path as FsPath, PathBuf};
use tokio::fs as tfs;
use axum::Json;
use axum::http::{HeaderMap, header, StatusCode};
use sha2::{Sha256, Digest};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::process::Command;

#[derive(Clone, Default)]
pub struct WebState {
    pub mcp_python_cmd: String,
    pub python_root: Option<String>,
    pub mcp_rust_cmd: String,
    pub rust_root: Option<String>,
    pub mcp_clarium_cmd: String,
    pub mcp_clarium_args: Vec<String>,
    pub event_tx: Option<broadcast::Sender<String>>, // for SSE streaming
    pub tool_registry: Option<ToolRegistry>,
    pub db_dsn: Option<String>,
    pub notifier_hub: Option<Arc<NotifierHub>>,
    // Streaming controls (can be configured via env/CLI upstream):
    pub partial_chunk_size: usize,   // default 1024
    pub partial_max_chunks: usize,   // default 3
    // Optional multi-agent harness (Ollama-backed)
    pub agents: Option<AgentsHarness>,
    // Agent config
    pub agents_max_attempts: Option<usize>,
    // Executor config
    pub step_timeout_ms: Option<u64>,
    // Orchestrator (in-memory governance state machine)
    pub orchestrator: Option<Arc<Mutex<Orchestrator>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SseEvent {
    event: String,
    #[serde(skip_serializing_if = "Option::is_none")] run_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")] plan_steps: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")] step_idx: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")] tool: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] chunk: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] stream: Option<String>,
    // Optional snippet sent on step_finished; persisted in DB
    #[serde(skip_serializing_if = "Option::is_none")] snippet: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] has_more: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")] stdout_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")] stderr_bytes: Option<u64>,
    // Agent timeline support
    #[serde(skip_serializing_if = "Option::is_none")] role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] stage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] json: Option<serde_json::Value>,
}

pub fn router(state: Arc<WebState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/chat", get(chat_get).post(chat_post))
        .route("/chat/stream", get(chat_stream))
        .route("/db/check", get(db_check))
        .route("/runs/:id/metrics", get(metrics_get))
        .route("/runs/:id/artifacts", get(run_artifacts_get))
        .route("/artifacts/:id", get(artifact_download))
        .route("/memories", get(memories_list))
        .route("/memories/write", axum::routing::post(memories_write))
        // Self‑development API
        .route("/patch/validate", axum::routing::post(patch_validate))
        .route("/patch/apply", axum::routing::post(patch_apply))
        // Approvals and run control
        .route("/runs/:id/approve", axum::routing::post(run_approve))
        .route("/runs/:id/reject", axum::routing::post(run_reject))
        .route("/runs/:id/resume", axum::routing::post(run_resume))
        // Workspaces/Projects and Guidelines management
        .route("/workspaces", get(workspaces_get).post(workspaces_post))
        .route("/projects", get(projects_get).post(projects_post))
        .route("/projects/:id/guidelines", get(guidelines_get).post(guidelines_post))
        .with_state(state)
}

// -------- Self‑development API types/helpers --------
#[derive(Debug, Clone, Deserialize)]
struct PatchReq {
    #[serde(default)]
    description: String,
    #[serde(default)]
    diffs: Vec<String>,
    #[serde(default = "default_unified")] 
    format: String, // "UnifiedDiff" | "Structured"
    // Optional roots
    #[serde(default)] rust_root: Option<String>,
    #[serde(default)] python_root: Option<String>,
    // Optional FS root
    #[serde(default)] fs_root: Option<String>,
    // Optional run context for artifact persistence
    #[serde(default)] run_id: Option<i64>,
    #[serde(default)] step_idx: Option<i32>,
}

fn default_unified() -> String { "UnifiedDiff".into() }

fn to_patch(req: &PatchReq) -> Patch {
    let fmt = match req.format.as_str() { "Structured" => PatchFormat::Structured, _ => PatchFormat::UnifiedDiff };
    Patch { format: fmt, description: req.description.clone(), diffs: req.diffs.clone() }
}

async fn patch_validate(State(state): State<Arc<WebState>>, Json(req): Json<PatchReq>) -> impl IntoResponse {
    let reg = match &state.tool_registry { Some(r) => r.clone(), None => return (StatusCode::BAD_REQUEST, Json(json!({"ok": false, "message": "ToolRegistry not available"}))).into_response(), };
    let reg = Arc::new(reg);
    let patch = to_patch(&req);
    let rust_root = req.rust_root.clone().or_else(|| state.rust_root.clone()).unwrap_or_else(|| ".".into());
    let python_root = req.python_root.clone().or_else(|| state.python_root.clone()).unwrap_or_else(|| ".".into());

    let pipeline = PatchPipeline::new(reg)
        .with_rust_root(rust_root.clone())
        .with_python_root(python_root.clone())
        .with_event_tx(state.event_tx.clone());

    match pipeline.validate(&patch).await {
        Ok(report) => {
            let gates: Vec<serde_json::Value> = report.gate_events.iter().map(|g| json!({
                "gate": g.gate, "root": g.root, "ok": g.ok, "duration_ms": g.duration_ms, "message": g.message
            })).collect();
            let response_json = json!({
                "ok": report.ok,
                "checks": report.checks,
                "details": report.details,
                "duration_ms": report.duration_ms,
                "gates": gates,
                "rust_root": rust_root,
                "python_root": python_root,
            });
            // Persist artifacts/memory if run context provided
            if let (Some(dsn), Some(rid)) = (&state.db_dsn, req.run_id) {
                if let Ok((client, conn)) = pg::connect(dsn, pg::NoTls).await {
                    tokio::spawn(async move { let _ = conn.await; });
                    let _ = ensure_artifacts_tables(&client).await;
                    if let Some(step_idx) = req.step_idx {
                        if let Ok(step_id) = fetch_step_id_for(&client, Some(rid), step_idx).await {
                            let content = serde_json::to_string_pretty(&response_json).unwrap_or_else(|_| "{}".into());
                            if let Ok((path, checksum, mime)) = write_artifact_file(Some(rid), step_id, "validation_report", &content).await {
                                let _ = insert_artifact_row(&client, rid, step_id, "validation_report", Some(path), Some(mime), Some(checksum), Some(content.len() as i64)).await;
                            }
                            for (idx, g) in report.gate_events.iter().enumerate() {
                                let mut log = String::new();
                                log.push_str(&format!("gate: {}\nroot: {}\nstatus: {}\nduration_ms: {}\n", g.gate, g.root, if g.ok {"ok"} else {"failed"}, g.duration_ms));
                                if let Some(m) = &g.message { log.push_str(&format!("message: {}\n", m)); }
                                let kind = format!("gate_{}_{}", idx, if g.ok {"ok"} else {"fail"});
                                if let Ok((path, checksum, mime)) = write_artifact_file(Some(rid), step_id, &kind, &log).await {
                                    let _ = insert_artifact_row(&client, rid, step_id, &kind, Some(path), Some(mime), Some(checksum), Some(log.len() as i64)).await;
                                }
                            }
                            // memory summary
                            let project_key = state.rust_root.clone().or_else(|| state.python_root.clone()).unwrap_or_else(|| "default".to_string());
                            let mem_content = format!("validation {} ({} gates, ok={})", req.description, report.gate_events.len(), report.ok);
                            let mem_tags = if report.ok { "patch_check,ok" } else { "patch_check,failed" };
                            memory_write(&client, &project_key, "patch_check", &format!("run:{}:step:{}:validate", rid, step_idx), &mem_content, mem_tags).await;
                        }
                    }
                }
            }
            Json(response_json).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"ok": false, "error": e.to_string()}))).into_response(),
    }
}

async fn patch_apply(State(state): State<Arc<WebState>>, Json(req): Json<PatchReq>) -> impl IntoResponse {
    let reg = match &state.tool_registry { Some(r) => r.clone(), None => return (StatusCode::BAD_REQUEST, Json(json!({"ok": false, "message": "ToolRegistry not available"}))).into_response(), };
    let reg = Arc::new(reg);
    let patch = to_patch(&req);
    let rust_root = req.rust_root.clone().or_else(|| state.rust_root.clone()).unwrap_or_else(|| ".".into());
    let python_root = req.python_root.clone().or_else(|| state.python_root.clone()).unwrap_or_else(|| ".".into());
    let fs_root = req.fs_root.clone().or_else(|| state.rust_root.clone()).unwrap_or_else(|| ".".into());

    let pipeline = PatchPipeline::new(reg)
        .with_rust_root(rust_root)
        .with_python_root(python_root)
        .with_event_tx(state.event_tx.clone());

    let fs = SafeFs::new(fs_root.clone());
    match pipeline.apply_with_rollback(&patch, &fs).await {
        Ok(report) => {
            let gates: Vec<serde_json::Value> = report.gate_events.iter().map(|g| json!({
                "gate": g.gate, "root": g.root, "ok": g.ok, "duration_ms": g.duration_ms, "message": g.message
            })).collect();
            let response_json = json!({
                "ok": report.applied,
                "applied": report.applied,
                "outcome": {"files_changed": report.outcome.files_changed, "checks": report.outcome.checks},
                "details": report.details,
                "duration_ms": report.duration_ms,
                "summary": report.summary_json,
                "gates": gates,
                "fs_root": fs_root,
            });
            if let (Some(dsn), Some(rid)) = (&state.db_dsn, req.run_id) {
                if let Ok((client, conn)) = pg::connect(dsn, pg::NoTls).await {
                    tokio::spawn(async move { let _ = conn.await; });
                    let _ = ensure_artifacts_tables(&client).await;
                    if let Some(step_idx) = req.step_idx {
                        if let Ok(step_id) = fetch_step_id_for(&client, Some(rid), step_idx).await {
                            let content = serde_json::to_string_pretty(&response_json).unwrap_or_else(|_| "{}".into());
                            let kind = if report.applied { "apply_report" } else { "apply_failed" };
                            if let Ok((path, checksum, mime)) = write_artifact_file(Some(rid), step_id, kind, &content).await {
                                let _ = insert_artifact_row(&client, rid, step_id, kind, Some(path), Some(mime), Some(checksum), Some(content.len() as i64)).await;
                            }
                            for (idx, g) in report.gate_events.iter().enumerate() {
                                let mut log = String::new();
                                log.push_str(&format!("gate: {}\nroot: {}\nstatus: {}\nduration_ms: {}\n", g.gate, g.root, if g.ok {"ok"} else {"failed"}, g.duration_ms));
                                if let Some(m) = &g.message { log.push_str(&format!("message: {}\n", m)); }
                                let kind = format!("gate_{}_{}", idx, if g.ok {"ok"} else {"fail"});
                                if let Ok((path, checksum, mime)) = write_artifact_file(Some(rid), step_id, &kind, &log).await {
                                    let _ = insert_artifact_row(&client, rid, step_id, &kind, Some(path), Some(mime), Some(checksum), Some(log.len() as i64)).await;
                                }
                            }
                            let project_key = state.rust_root.clone().or_else(|| state.python_root.clone()).unwrap_or_else(|| "default".to_string());
                            let mem_content = format!("apply {} (gates={}, applied={})", req.description, report.gate_events.len(), report.applied);
                            let mem_tags = if report.applied { "patch_apply,ok" } else { "patch_apply,failed" };
                            memory_write(&client, &project_key, "patch_apply", &format!("run:{}:step:{}:apply", rid, step_idx), &mem_content, mem_tags).await;
                        }
                    }
                }
            }
            Json(response_json).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"ok": false, "error": e.to_string()}))).into_response(),
    }
}

// --- DB helpers for artifacts (MVP) ---
async fn ensure_artifacts_tables(client: &pg::Client) -> Result<(), String> {
    let q1 = r#"
        CREATE TABLE IF NOT EXISTS artifacts (
            id BIGSERIAL PRIMARY KEY,
            run_id BIGINT NOT NULL,
            step_id BIGINT NOT NULL,
            kind TEXT NOT NULL,
            path TEXT,
            mime TEXT,
            size_bytes BIGINT,
            checksum TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    "#;
    let q2 = r#"
        CREATE TABLE IF NOT EXISTS step_attachments (
            id BIGSERIAL PRIMARY KEY,
            step_id BIGINT NOT NULL,
            kind TEXT NOT NULL,
            path TEXT,
            mime TEXT,
            size_bytes BIGINT,
            checksum TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    "#;
    client.batch_execute(q1).await.map_err(|e| e.to_string())?;
    client.batch_execute(q2).await.map_err(|e| e.to_string())?;
    Ok(())
}

async fn insert_artifact_row(
    client: &pg::Client,
    run_id: i64,
    step_id: i64,
    kind: &str,
    path: Option<String>,
    mime: Option<String>,
    checksum: Option<String>,
    size_bytes: Option<i64>,
) -> Result<i64, String> {
    let row = client
        .query_one(
            "INSERT INTO artifacts(run_id, step_id, kind, path, mime, size_bytes, checksum) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id",
            &[&run_id, &step_id, &kind, &path, &mime, &size_bytes, &checksum],
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(row.get::<usize, i64>(0))
}

// duplicate removed (see definitions later in file)

// --- Memory write endpoint (MVP) ---
#[derive(Debug, serde::Deserialize)]
struct MemoryWriteBody { project: String, kind: String, key: String, content: String, #[serde(default)] tags: String }

async fn memories_write(State(state): State<Arc<WebState>>, Json(body): Json<MemoryWriteBody>) -> impl IntoResponse {
    if let Some(dsn) = &state.db_dsn {
        if let Ok((client, conn)) = pg::connect(dsn, pg::NoTls).await {
            tokio::spawn(async move { let _ = conn.await; });
            memory_write(&client, &body.project, &body.kind, &body.key, &body.content, &body.tags).await;
            return Json(json!({"ok": true})).into_response();
        }
    }
    (StatusCode::BAD_REQUEST, Json(json!({"ok": false, "message": "DB not configured"}))).into_response()
}

async fn health(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    // Base response
    let mut obj = serde_json::Map::new();
    obj.insert("ok".to_string(), serde_json::Value::Bool(true));

    // Include MCP status (ok + capabilities) if registry available
    if let Some(reg) = &state.tool_registry {
        let statuses = reg.status_all(3).await; // 3s timeout per host
        let mut m = serde_json::Map::new();
        for (sid, val) in &statuses {
            m.insert(sid.clone(), val.clone());
        }
        obj.insert("mcp".to_string(), serde_json::Value::Object(m.clone()));
        // Emit SSE health snapshot event
        if let Some(tx) = &state.event_tx {
            let _ = tx.send(json!({
                "event": "mcp_health",
                "status": m,
            }).to_string());
        }
    }

    Json(serde_json::Value::Object(obj))
}

// Simple DB connectivity self-check endpoint
async fn db_check(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    if let Some(dsn) = &state.db_dsn {
        match pg::connect(dsn, pg::NoTls).await {
            Ok((client, conn)) => {
                tokio::spawn(async move { let _ = conn.await; });
                match client.query_one("SELECT 1", &[]).await {
                    Ok(_) => return Json(json!({"ok": true})),
                    Err(e) => return Json(json!({"ok": false, "message": format!("query failed: {}", e)})),
                }
            }
            Err(e) => Json(json!({"ok": false, "message": e.to_string()})),
        }
    } else {
        Json(json!({"ok": false, "message": "DB not configured"}))
    }
}

// ---- Approvals & resume endpoints (QA/Manager governance) ----

#[derive(Debug, Clone, Deserialize)]
struct RoleQuery { role: Option<String> }

#[derive(Debug, Clone, Deserialize)]
struct ApprovalBody {
    rationale: Option<String>,
    #[serde(default)]
    required_changes: Vec<String>,
}

async fn run_approve(
    State(state): State<Arc<WebState>>,
    AxPath(run_id): AxPath<i64>,
    Query(q): Query<RoleQuery>,
    Json(body): Json<ApprovalBody>,
) -> impl IntoResponse {
    let role = q.role.unwrap_or_else(|| "QA".into());
    let rationale = body.rationale.unwrap_or_else(|| "approved".into());
    // Best-effort DB write of verdict linked to latest plan for the run
    if let Some(dsn) = &state.db_dsn {
        if let Ok((client, conn)) = pg::connect(dsn, pg::NoTls).await {
            tokio::spawn(async move { let _ = conn.await; });
            if let Ok(row_opt) = client.query_opt("SELECT id FROM plans WHERE run_id = $1 ORDER BY id DESC LIMIT 1", &[&run_id]).await {
                if let Some(row) = row_opt {
                    let plan_id: i64 = row.get(0);
                    let _ = client.query(
                        "INSERT INTO plan_verdicts(plan_id, status, rationale) VALUES($1,$2,$3)",
                        &[&plan_id, &"Approved", &rationale]
                    ).await;
                }
            }
        }
    }
    // SSE event
    if let Some(tx) = &state.event_tx {
        let _ = tx.send(serde_json::to_string(&SseEvent{
            event: "approval".into(),
            run_id: Some(run_id),
            plan_steps: None,
            step_idx: None,
            tool: None,
            status: Some("Approved".into()),
            message: Some(rationale.clone()),
            chunk: None,
            stream: None,
            snippet: None,
            has_more: None,
            stdout_bytes: None,
            stderr_bytes: None,
            role: Some(role.clone()),
            stage: Some(if role.eq_ignore_ascii_case("manager") { "manager_signoff" } else { "qa_verdict" }.into()),
            json: Some(json!({"required_changes": body.required_changes})),
        }).unwrap_or_else(|_| "{\"event\":\"approval\"}".into()));
    }
    // Orchestrator state transition
    if let Some(orch) = &state.orchestrator {
        let mut guard = orch.lock().await;
        let role_enum = if role.eq_ignore_ascii_case("manager") { Role::Manager } else if role.eq_ignore_ascii_case("dev") { Role::Dev } else { Role::QA };
        let appr = Approval { role: role_enum, status: ApprovalStatus::Approved, rationale: rationale.clone(), required_changes: body.required_changes.clone() };
        if role_enum == Role::Manager { guard.apply_manager_verdict(run_id, &appr); } else { guard.apply_qa_verdict(run_id, &appr); }
    }
    Json(json!({"ok": true, "run_id": run_id, "role": role, "status": "Approved"}))
}

async fn run_reject(
    State(state): State<Arc<WebState>>,
    AxPath(run_id): AxPath<i64>,
    Query(q): Query<RoleQuery>,
    Json(body): Json<ApprovalBody>,
) -> impl IntoResponse {
    let role = q.role.unwrap_or_else(|| "QA".into());
    let rationale = body.rationale.unwrap_or_else(|| "needs changes".into());
    if let Some(dsn) = &state.db_dsn {
        if let Ok((client, conn)) = pg::connect(dsn, pg::NoTls).await {
            tokio::spawn(async move { let _ = conn.await; });
            if let Ok(row_opt) = client.query_opt("SELECT id FROM plans WHERE run_id = $1 ORDER BY id DESC LIMIT 1", &[&run_id]).await {
                if let Some(row) = row_opt {
                    let plan_id: i64 = row.get(0);
                    let _ = client.query(
                        "INSERT INTO plan_verdicts(plan_id, status, rationale) VALUES($1,$2,$3)",
                        &[&plan_id, &"NeedsChanges", &rationale]
                    ).await;
                }
            }
        }
    }
    if let Some(tx) = &state.event_tx {
        let _ = tx.send(serde_json::to_string(&SseEvent{
            event: "approval".into(),
            run_id: Some(run_id),
            plan_steps: None,
            step_idx: None,
            tool: None,
            status: Some("NeedsChanges".into()),
            message: Some(rationale.clone()),
            chunk: None,
            stream: None,
            snippet: None,
            has_more: None,
            stdout_bytes: None,
            stderr_bytes: None,
            role: Some(role.clone()),
            stage: Some(if role.eq_ignore_ascii_case("manager") { "manager_feedback" } else { "qa_verdict" }.into()),
            json: Some(json!({"required_changes": body.required_changes})),
        }).unwrap_or_else(|_| "{\"event\":\"approval\"}".into()));
    }
    // Orchestrator state transition
    if let Some(orch) = &state.orchestrator {
        let mut guard = orch.lock().await;
        let role_enum = if role.eq_ignore_ascii_case("manager") { Role::Manager } else if role.eq_ignore_ascii_case("dev") { Role::Dev } else { Role::QA };
        let appr = Approval { role: role_enum, status: ApprovalStatus::NeedsChanges, rationale: rationale.clone(), required_changes: body.required_changes.clone() };
        if role_enum == Role::Manager { guard.apply_manager_verdict(run_id, &appr); } else { guard.apply_qa_verdict(run_id, &appr); }
    }
    Json(json!({"ok": true, "run_id": run_id, "role": role, "status": "NeedsChanges"}))
}

async fn run_resume(
    State(state): State<Arc<WebState>>,
    AxPath(run_id): AxPath<i64>,
) -> impl IntoResponse {
    if let Some(orch) = &state.orchestrator {
        let mut guard = orch.lock().await;
        guard.resume(run_id);
    }
    if let Some(tx) = &state.event_tx {
        let _ = tx.send(serde_json::to_string(&SseEvent{
            event: "run_resume".into(),
            run_id: Some(run_id),
            plan_steps: None,
            step_idx: None,
            tool: None,
            status: Some("resume".into()),
            message: Some("User requested resume".into()),
            chunk: None,
            stream: None,
            snippet: None,
            has_more: None,
            stdout_bytes: None,
            stderr_bytes: None,
            role: None,
            stage: Some("resume".into()),
            json: None,
        }).unwrap_or_else(|_| "{\"event\":\"run_resume\"}".into()));
    }
    Json(json!({"ok": true, "run_id": run_id}))
}

// ---------------- File-based persistence for Workspaces/Projects/Guidelines ----------------

fn storage_base() -> PathBuf {
    let mut p = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    p.push("configs");
    p
}

async fn ensure_dir(path: &PathBuf) {
    let _ = tfs::create_dir_all(path).await;
}

async fn load_json_file<T: for<'de> Deserialize<'de> + Default>(path: &PathBuf) -> T {
    match tfs::read(path).await {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default()
        , Err(_) => Default::default(),
    }
}

async fn save_json_file<T: Serialize>(path: &PathBuf, value: &T) -> Result<(), String> {
    if let Some(parent) = path.parent() { let _ = tfs::create_dir_all(parent).await; }
    let s = serde_json::to_string_pretty(value).map_err(|e| e.to_string())?;
    tfs::write(path, s.as_bytes()).await.map_err(|e| e.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct WorkspacesDoc { items: Vec<Workspace> }

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ProjectsDoc { items: Vec<Project> }

async fn load_workspaces() -> WorkspacesDoc {
    let mut p = storage_base(); p.push("workspaces.json");
    load_json_file(&p).await
}

async fn save_workspaces(doc: &WorkspacesDoc) -> Result<(), String> {
    let mut p = storage_base(); p.push("workspaces.json");
    save_json_file(&p, doc).await
}

async fn load_projects() -> ProjectsDoc {
    let mut p = storage_base(); p.push("projects.json");
    load_json_file(&p).await
}

async fn save_projects(doc: &ProjectsDoc) -> Result<(), String> {
    let mut p = storage_base(); p.push("projects.json");
    save_json_file(&p, doc).await
}

async fn load_guidelines(project_id: &str) -> MemoryGuidelines {
    let mut p = storage_base(); p.push("guidelines"); p.push(format!("{}.json", project_id));
    load_json_file(&p).await
}

async fn save_guidelines(project_id: &str, g: &MemoryGuidelines) -> Result<(), String> {
    let mut p = storage_base(); p.push("guidelines"); p.push(format!("{}.json", project_id));
    save_json_file(&p, g).await
}

fn gen_id(seed: &str) -> String {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
    let mut hasher = Sha256::new();
    hasher.update(seed.as_bytes());
    hasher.update(ts.to_string().as_bytes());
    let digest = hasher.finalize();
    let hex = hex::encode(digest);
    hex[..12].to_string()
}

// ---------------- Handlers: Workspaces/Projects/Guidelines ----------------

async fn workspaces_get() -> Html<String> {
    let doc = load_workspaces().await;
    let mut html = String::from(r#"<!doctype html><html><head><meta charset='utf-8'><title>Workspaces</title><script src='https://unpkg.com/htmx.org@1.9.12'></script></head><body>"#);
    html.push_str("<h2>Workspaces</h2><ul>");
    for ws in doc.items.iter() {
        html.push_str(&format!("<li><b>{}</b> <code>{}</code> {}</li>", htmlescape::encode_minimal(&ws.name), htmlescape::encode_minimal(&ws.id), htmlescape::encode_minimal(&ws.root_path.clone().unwrap_or_default())));
    }
    html.push_str("</ul><h3>Create Workspace</h3>");
    html.push_str("<form hx-post='/workspaces' hx-target='body' hx-swap='outerHTML'>");
    html.push_str("<input name='name' placeholder='Name' /> <input name='root_path' placeholder='Root path (optional)' /> <button type='submit'>Create</button>");
    html.push_str("</form>");
    html.push_str("<p><a href='/projects'>Projects</a> · <a href='/chat'>Back</a></p>");
    html.push_str("</body></html>");
    Html(html)
}

#[derive(Debug, Deserialize)]
struct WorkspaceForm { name: String, root_path: Option<String> }

async fn workspaces_post(Form(form): Form<WorkspaceForm>) -> impl IntoResponse {
    let mut doc = load_workspaces().await;
    let id = gen_id(&form.name);
    let ws = Workspace{ id, name: form.name, root_path: form.root_path, description: None };
    doc.items.push(ws);
    match save_workspaces(&doc).await {
        Ok(_) => workspaces_get().await.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

async fn projects_get() -> Html<String> {
    let wdoc = load_workspaces().await;
    let pdoc = load_projects().await;
    let mut html = String::from(r#"<!doctype html><html><head><meta charset='utf-8'><title>Projects</title><script src='https://unpkg.com/htmx.org@1.9.12'></script></head><body>"#);
    html.push_str("<h2>Projects</h2><ul>");
    for pr in pdoc.items.iter() {
        let ws_name = wdoc.items.iter().find(|w| w.id==pr.workspace_id).map(|w| w.name.clone()).unwrap_or_else(||"?".into());
        html.push_str(&format!("<li><b>{}</b> <code>{}</code> in <i>{}</i> — path: {} — <a href='/projects/{}/guidelines'>guidelines</a></li>", htmlescape::encode_minimal(&pr.name), htmlescape::encode_minimal(&pr.id), htmlescape::encode_minimal(&ws_name), htmlescape::encode_minimal(&pr.path), htmlescape::encode_minimal(&pr.id)));
    }
    html.push_str("</ul><h3>Create Project</h3>");
    html.push_str("<form hx-post='/projects' hx-target='body' hx-swap='outerHTML'>");
    html.push_str("<input name='name' placeholder='Name' /> ");
    // workspace select
    html.push_str("<select name='workspace_id'>");
    for ws in wdoc.items.iter() { html.push_str(&format!("<option value='{}'>{}</option>", htmlescape::encode_minimal(&ws.id), htmlescape::encode_minimal(&ws.name))); }
    html.push_str("</select> ");
    html.push_str("<input name='path' placeholder='Folder path' style='width:320px'/> ");
    html.push_str("<input name='repo_url' placeholder='Repo URL (optional)' style='width:260px'/> ");
    html.push_str("<label><input type='checkbox' name='init_repo' value='1'/> init git repo</label> ");
    html.push_str("<button type='submit'>Create</button>");
    html.push_str("</form>");
    html.push_str("<p><a href='/workspaces'>Workspaces</a> · <a href='/chat'>Back</a></p>");
    html.push_str("</body></html>");
    Html(html)
}

#[derive(Debug, Deserialize)]
struct ProjectForm { name: String, workspace_id: String, path: String, repo_url: Option<String>, init_repo: Option<String> }

async fn projects_post(Form(form): Form<ProjectForm>) -> impl IntoResponse {
    let mut pdoc = load_projects().await;
    let id = gen_id(&form.name);
    let pr = Project{ id: id.clone(), workspace_id: form.workspace_id, name: form.name, path: form.path.clone(), repo_url: form.repo_url };
    pdoc.items.push(pr);
    if let Err(e) = save_projects(&pdoc).await { return (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(); }
    // optionally init repo
    if form.init_repo.is_some() {
        let _ = tfs::create_dir_all(&form.path).await;
        // best-effort: git init
        let _ = Command::new("git").arg("init").arg(&form.path).output().await;
    }
    projects_get().await.into_response()
}

async fn guidelines_get(AxPath(id): AxPath<String>) -> Html<String> {
    let pdoc = load_projects().await;
    let proj = pdoc.items.iter().find(|p| p.id==id).cloned();
    let g = load_guidelines(&id).await;
    let mut html = String::from(r#"<!doctype html><html><head><meta charset='utf-8'><title>Guidelines</title><script src='https://unpkg.com/htmx.org@1.9.12'></script></head><body>"#);
    if let Some(pr) = proj {
        html.push_str(&format!("<h2>Guidelines for <b>{}</b> <code>{}</code></h2>", htmlescape::encode_minimal(&pr.name), htmlescape::encode_minimal(&pr.id)));
    } else {
        html.push_str("<h2>Guidelines</h2>");
    }
    html.push_str("<form hx-post='' hx-target='body' hx-swap='outerHTML'>");
    html.push_str(&format!("<textarea name='text' style='width:100%; height:200px'>{}</textarea>", htmlescape::encode_minimal(&g.text)));
    html.push_str("<div><small>Metadata (JSON):</small><br/>");
    html.push_str(&format!("<textarea name='metadata' style='width:100%; height:120px'>{}</textarea>", htmlescape::encode_minimal(&serde_json::to_string_pretty(&g.metadata).unwrap_or_else(|_| "{}".into()))));
    html.push_str("</div><button type='submit'>Save</button> <a href='/projects'>Back</a></form>");
    html.push_str("</body></html>");
    Html(html)
}

#[derive(Debug, Deserialize)]
struct GuidelinesForm { text: Option<String>, metadata: Option<String> }

async fn guidelines_post(AxPath(id): AxPath<String>, Form(form): Form<GuidelinesForm>) -> impl IntoResponse {
    let mut g = load_guidelines(&id).await;
    if let Some(t) = form.text { g.text = t; }
    if let Some(m) = form.metadata {
        g.metadata = serde_json::from_str(&m).unwrap_or(serde_json::json!({}));
    }
    match save_guidelines(&id, &g).await {
        Ok(_) => guidelines_get(AxPath(id)).await.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

async fn chat_get() -> Html<&'static str> {
    Html(r##"<!doctype html>
<html><head><meta charset='utf-8'><title>ClaritasAI Chat</title>
<script src="https://unpkg.com/htmx.org@1.9.12"></script></head>
<body>
<h1>ClaritasAI</h1>
<form hx-post="/chat" hx-target="#out" hx-swap="beforeend">
  <input type="text" name="objective" placeholder="Enter objective" style="width:60%" />
  <input type="text" name="project_id" placeholder="Project ID (optional)" style="width:20%" />
  <input type="text" name="parent_plan_id" placeholder="Parent Plan ID (optional)" style="width:20%" />
  <button type="submit">Run</button>
  <div id="out" style="margin-top:1rem;"></div>
</form>
<script src="https://unpkg.com/htmx.org/dist/ext/sse.js"></script>
<div id="db-banner" style="padding:6px 10px; margin:8px 0; border-radius:4px; background:#eee; color:#333; display:none;"></div>
<div id="hosts" style="display:flex; gap:8px; flex-wrap:wrap; margin-bottom:8px;"></div>
<div id="timeline" style="margin-top:10px;"></div>
<script>
  (function(){
    const tl = document.getElementById('timeline');
    const banner = document.getElementById('db-banner');
    const hosts = document.getElementById('hosts');
    const hostState = {};
    function renderHosts(){
      hosts.innerHTML = '';
      const ids = Object.keys(hostState).sort();
      ids.forEach(id => {
        const st = hostState[id] || {};
        const chip = document.createElement('div');
        chip.style.padding = '4px 8px';
        chip.style.borderRadius = '12px';
        chip.style.fontSize = '12px';
        chip.style.border = '1px solid #ddd';
        chip.style.background = st.ok ? '#e6ffed' : '#ffecec';
        chip.style.color = st.ok ? '#0a5' : '#a00';
        const text = id+': '+(st.status|| (st.ok?'ok':'down')) + (st.backoff_ms?(' ('+st.backoff_ms+'ms)'):'');
        chip.textContent = text;
        hosts.appendChild(chip);
      });
    }
    function ensureRun(runId){
      let el = document.getElementById('run-'+runId);
      if(!el){
        el = document.createElement('div');
        el.id = 'run-'+runId;
        el.style.border = '1px solid #ddd';
        el.style.padding = '8px';
        el.style.margin = '8px 0';
        el.innerHTML = '<div style="font-weight:bold;">Run '+runId+'</div><div class="steps"></div>';
        tl.prepend(el);
      }
      return el.querySelector('.steps');
    }
    function esc(s){
      const d = document.createElement('div');
      d.textContent = s; return d.innerHTML;
    }
    const es = new EventSource('/chat/stream');
    es.addEventListener('message', function(e){
      let data = null;
      try { data = JSON.parse(e.data); } catch(_){ return; }
      if(!data || !data.event) return;
      if(data.event === 'db_status'){
        banner.style.display = 'block';
        banner.style.background = data.ok ? '#e6ffed' : '#ffecec';
        banner.style.color = data.ok ? '#0a5' : '#a00';
        banner.innerHTML = data.ok ? 'DB: connected' : ('DB: '+(data.message||'not configured')+ ' — <a href="/db/check" target="_blank">check</a>');
        return;
      }
      if(data.event === 'host_health'){
        const id = data.id || 'unknown';
        hostState[id] = { ok: !!data.ok, status: data.status || (data.ok?'ok':'down'), backoff_ms: data.backoff_ms, message: data.message };
        renderHosts();
        return;
      }
      if(data.event === 'run_started'){
        const steps = ensureRun(data.run_id);
        const head = steps.parentElement.querySelector('div');
        if(head){ head.innerHTML = 'Run '+data.run_id+' — planning…'; }
        return;
      }
      if(data.event === 'plan_saved'){
        const steps = ensureRun(data.run_id);
        const head = steps.parentElement.querySelector('div');
        if(head){ head.innerHTML = 'Run '+data.run_id+' — plan saved ('+(data.plan_steps||0)+' steps)'; }
        return;
      }
      if(data.event === 'verdict'){
        const steps = ensureRun(data.run_id);
        const div = document.createElement('div');
        div.innerHTML = '<span style="color:#555;">Verdict:</span> '+esc(data.status||'')+' — '+esc(data.message||'');
        steps.appendChild(div);
        return;
      }
      if(data.event === 'agent_message'){
        const steps = ensureRun(data.run_id);
        const card = document.createElement('div');
        card.style.border = '1px dashed #ccc';
        card.style.borderRadius = '6px';
        card.style.padding = '6px';
        card.style.margin = '6px 0';
        const role = esc(data.role||'?');
        const stage = esc(data.stage||'');
        let body = esc(data.message||'');
        if(!body && data.json){
          try { body = JSON.stringify(data.json); } catch(_) {}
        }
        card.innerHTML = '<div style="font-weight:bold;">'+role+(stage?(' · '+stage):'')+'</div>'+
                         '<div style="white-space:pre-wrap;">'+body+'</div>';
        steps.appendChild(card);
        return;
      }
      if(data.event === 'execution_started'){
        const steps = ensureRun(data.run_id);
        const div = document.createElement('div');
        div.innerHTML = '<em>Execution started</em>';
        steps.appendChild(div);
        return;
      }
      if(data.event === 'step_started'){
        const steps = ensureRun(data.run_id);
        const div = document.createElement('div');
        div.id = 'run-'+data.run_id+'-step-'+data.step_idx;
        div.innerHTML = '<strong>Step '+data.step_idx+':</strong> '+esc(data.tool||'');
        steps.appendChild(div);
        return;
      }
      if(data.event === 'step_partial'){
        const div = document.getElementById('run-'+data.run_id+'-step-'+data.step_idx);
        if(div){
          const pre = document.createElement('pre');
          pre.style.whiteSpace = 'pre-wrap';
          pre.style.margin = '4px 0 0 0';
          pre.textContent = (data.stream?('['+data.stream+'] '):'') + (data.chunk||'');
          div.appendChild(pre);
        }
        return;
      }
      if(data.event === 'step_finished'){
        const div = document.getElementById('run-'+data.run_id+'-step-'+data.step_idx);
        if(div){
          const tail = document.createElement('div');
          tail.style.color = (data.status==='ok') ? '#0a5' : '#a00';
          const parts = ['Status: '+(data.status||'')];
          if(typeof data.stdout_bytes === 'number'){ parts.push('stdout bytes: '+data.stdout_bytes); }
          if(typeof data.stderr_bytes === 'number'){ parts.push('stderr bytes: '+data.stderr_bytes); }
          tail.textContent = parts.join(' · ');
          div.appendChild(tail);
          if(data.snippet){
            const pre = document.createElement('pre');
            pre.style.whiteSpace = 'pre-wrap';
            pre.style.margin = '4px 0 0 0';
            pre.textContent = data.snippet;
            div.appendChild(pre);
            if(data.has_more){
              const hint = document.createElement('div');
              hint.style.color = '#888';
              hint.style.fontStyle = 'italic';
              hint.textContent = 'Output truncated. Full artifacts coming soon…';
              div.appendChild(hint);
            }
          }
        }
        return;
      }
      if(data.event === 'run_finished'){
        const steps = ensureRun(data.run_id);
        const div = document.createElement('div');
        div.innerHTML = '<strong>Run finished</strong>';
        steps.appendChild(div);
        // Fetch metrics summary and render below
        fetch('/runs/'+data.run_id+'/metrics').then(r=>r.json()).then(j=>{
          if(!j || !j.ok) return;
          const box = document.createElement('div');
          box.style.marginTop = '6px';
          box.style.borderTop = '1px dashed #ddd';
          box.style.paddingTop = '6px';
          const total = (j.metrics||[]).find(m=>m.key==='total_runtime_ms');
          const list = (j.metrics||[]).filter(m=>m.key && m.key.startsWith('step_') && m.key.endsWith('_runtime_ms'));
          let html = '<div style="font-weight:bold;">Metrics</div>';
          if(total){ html += '<div>Total runtime: '+Math.round(total.value_num||0)+' ms</div>'; }
          if(list && list.length){
            html += '<ul style="margin:4px 0; padding-left:20px;">'+list.map(m=>'<li>'+m.key+': '+Math.round(m.value_num||0)+' ms</li>').join('')+'</ul>';
          }
          box.innerHTML = html + '<div><a target="_blank" href="/runs/'+data.run_id+'/metrics">Open metrics JSON</a></div>';
          steps.appendChild(box);
        }).catch(()=>{});
        // Fetch artifacts and render a simple list below
        fetch('/runs/'+data.run_id+'/artifacts').then(r=>r.json()).then(j=>{
          if(!j || !j.ok) return;
          const arts = j.artifacts||[];
          if(arts.length){
            const box = document.createElement('div');
            box.style.marginTop = '6px';
            box.style.borderTop = '1px dashed #ddd';
            box.style.paddingTop = '6px';
            let html = '<div style="font-weight:bold;">Artifacts</div><ul style="margin:4px 0; padding-left:20px;">';
            html += arts.map(a=>{
              const parts = [];
              parts.push(a.kind||'artifact');
              if (a.mime) parts.push('mime: '+a.mime);
              if (typeof a.size_bytes === 'number') parts.push(String(a.size_bytes)+' bytes');
              if (a.checksum) parts.push('sha256: '+a.checksum.substring(0,12)+'…');
              parts.push('<a target="_blank" href="'+a.href+'">download</a>');
              return '<li>'+parts.join(' — ')+'</li>';
            }).join('');
            html += '</ul>';
            box.innerHTML = html;
            steps.appendChild(box);
          }
        }).catch(()=>{});
          return;
        }
    });
  })();
</script>
</body></html>"##)
}

// --- Artifact endpoints ---
async fn run_artifacts_get(
    State(state): State<Arc<WebState>>, 
    AxPath(run_id): AxPath<i64>,
) -> impl IntoResponse {
    let mut items: Vec<serde_json::Value> = Vec::new();
    if let Some(dsn) = &state.db_dsn {
        if let Ok((client, conn)) = pg::connect(dsn, pg::NoTls).await {
            tokio::spawn(async move { let _ = conn.await; });
            let q = "SELECT id, kind, path, mime, size_bytes, checksum, created_at::text FROM artifacts WHERE run_id = $1 ORDER BY id";
            if let Ok(rows) = client.query(q, &[&run_id]).await {
                for r in rows {
                    let id: i64 = r.get(0);
                    let kind: String = r.get(1);
                    let path: Option<String> = r.try_get(2).ok();
                    let mime: Option<String> = r.try_get(3).ok();
                    let size: Option<i64> = r.try_get(4).ok();
                    let checksum: Option<String> = r.try_get(5).ok();
                    let created_at: Option<String> = r.try_get(6).ok();
                    items.push(json!({
                        "id": id,
                        "kind": kind,
                        "path": path,
                        "mime": mime,
                        "size_bytes": size,
                        "checksum": checksum,
                        "created_at": created_at,
                        "href": format!("/artifacts/{}", id),
                    }));
                }
            }
        }
    }
    Json(json!({"ok": true, "run_id": run_id, "artifacts": items}))
}

async fn artifact_download(
    State(state): State<Arc<WebState>>, 
    AxPath(artifact_id): AxPath<i64>,
) -> impl IntoResponse {
    if let Some(dsn) = &state.db_dsn {
        if let Ok((client, conn)) = pg::connect(dsn, pg::NoTls).await {
            tokio::spawn(async move { let _ = conn.await; });
            if let Ok(row) = client.query_opt("SELECT path, mime FROM artifacts WHERE id = $1", &[&artifact_id]).await {
                if let Some(r) = row {
                    let path: Option<String> = r.try_get(0).ok();
                    let mime: String = r.try_get(1).unwrap_or_else(|_| "text/plain".to_string());
                    if let Some(p) = path {
                        match tfs::read(&p).await {
                            Ok(bytes) => {
                                let mut headers = HeaderMap::new();
                                headers.insert(header::CONTENT_TYPE, mime.parse().unwrap_or_else(|_| "application/octet-stream".parse().unwrap()));
                                return (headers, bytes).into_response();
                            }
                            Err(_) => {
                                return (StatusCode::NOT_FOUND, "artifact file not found").into_response();
                            }
                        }
                    }
                }
            }
        }
    }
    (StatusCode::NOT_FOUND, "artifact not found").into_response()
}

// --- Memory helpers and endpoint ---
async fn memory_write(
    client: &pg::Client,
    project_key: &str,
    kind: &str,
    key: &str,
    content: &str,
    tags: &str,
) {
    let _ = client
        .query(
            "INSERT INTO project_memories(project_key, kind, key, content, tags) VALUES($1,$2,$3,$4,$5)",
            &[&project_key, &kind, &key, &content, &tags],
        )
        .await;
}

async fn memory_query(
    client: &pg::Client,
    project_key: &str,
    tags_like: Option<&str>,
    k: i64,
) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    let q_all = "SELECT id, kind, key, content, tags, created_at::text FROM project_memories WHERE project_key=$1 ORDER BY id DESC LIMIT $2";
    let q_tag = "SELECT id, kind, key, content, tags, created_at::text FROM project_memories WHERE project_key=$1 AND tags ILIKE $2 ORDER BY id DESC LIMIT $3";
    let rows = if let Some(t) = tags_like {
        client
            .query(q_tag, &[&project_key, &t, &k])
            .await
            .unwrap_or_default()
    } else {
        client
            .query(q_all, &[&project_key, &k])
            .await
            .unwrap_or_default()
    };
    for r in rows {
        let id: i64 = r.get(0);
        let kind: String = r.get(1);
        let key: String = r.get(2);
        let content: String = r.get(3);
        let tags: String = r.get(4);
        let created_at: Option<String> = r.try_get(5).ok();
        out.push(json!({"id": id, "kind": kind, "key": key, "content": content, "tags": tags, "created_at": created_at}));
    }
    out
}

async fn memories_list(State(state): State<Arc<WebState>>, axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>) -> impl IntoResponse {
    let project = params.get("project").cloned().unwrap_or_else(|| "default".to_string());
    let tags = params.get("tags").cloned();
    let k: i64 = params
        .get("k")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(10);
    if let Some(dsn) = &state.db_dsn {
        if let Ok((client, conn)) = pg::connect(dsn, pg::NoTls).await {
            tokio::spawn(async move { let _ = conn.await; });
            let items = memory_query(&client, &project, tags.as_deref(), k).await;
            return Json(json!({"ok": true, "project": project, "count": items.len(), "items": items}));
        }
    }
    Json(json!({"ok": false, "message": "DB not configured"}))
}

// --- DB helpers for artifacts ---
async fn fetch_step_id_for(client: &pg::Client, run_id: Option<i64>, step_idx: i32) -> Result<i64, String> {
    let rid = run_id.ok_or_else(|| "missing run_id".to_string())?;
    let q = r#"
        SELECT s.id
        FROM steps s
        JOIN executions e ON s.execution_id = e.id
        JOIN plans p ON e.plan_id = p.id
        WHERE p.run_id = $1 AND s.idx = $2
        ORDER BY s.id DESC
        LIMIT 1
    "#;
    match client.query(q, &[&rid, &step_idx]).await {
        Ok(rows) if !rows.is_empty() => Ok(rows[0].get::<_, i32>(0) as i64),
        Ok(_) => Err("step not found".into()),
        Err(e) => Err(e.to_string()),
    }
}

/// Naive MIME detection and file naming based on content/kind.
fn detect_mime_and_filename(kind: &str, content: &str) -> (String, String) {
    // Try JSON first
    let trimmed = content.trim_start();
    if (trimmed.starts_with('{') || trimmed.starts_with('[')) && serde_json::from_str::<serde_json::Value>(trimmed).is_ok() {
        return (format!("{}.json", kind), "application/json".to_string());
    }
    // Minimal HTML check
    let lower = trimmed.to_ascii_lowercase();
    if lower.contains("<html") || lower.contains("<body") || lower.contains("<!doctype html") {
        return (format!("{}.html", kind), "text/html".to_string());
    }
    // Plain text fallback
    (format!("{}.txt", kind), "text/plain".to_string())
}

async fn write_artifact_file(run_id: Option<i64>, step_id: i64, kind: &str, content: &str) -> Result<(String, String, String), String> {
    let rid = run_id.ok_or_else(|| "missing run_id".to_string())?;
    let mut base = std::env::current_dir().map_err(|e| e.to_string())?;
    base.push("files");
    base.push("artifacts");
    base.push(format!("run_{}", rid));
    if let Err(e) = tfs::create_dir_all(&base).await { return Err(e.to_string()); }
    let (basename, mime) = detect_mime_and_filename(kind, content);
    let filename = format!("step_{}_{}", step_id, basename);
    let mut path = base.clone();
    path.push(filename);
    if let Err(e) = tfs::write(&path, content.as_bytes()).await { return Err(e.to_string()); }
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    let digest = hasher.finalize();
    let checksum = hex::encode(digest);
    Ok((path.to_string_lossy().to_string(), checksum, mime))
}

#[derive(Debug, serde::Deserialize)]
struct ChatForm { objective: String, project_id: Option<String>, parent_plan_id: Option<String> }

async fn chat_post(
    State(state): State<Arc<WebState>>,
    Form(form): Form<ChatForm>,
) -> impl IntoResponse {
    // Prefer routing through ToolRegistry if present; fallback to direct StdioClient calls
    if let Some(tx) = &state.event_tx {
        let _ = tx.send(serde_json::json!({
            "event": "objective",
            "message": form.objective
        }).to_string());
    }

    if let Some(reg) = &state.tool_registry {
        // MVP Planner → Verifier → Executor with persistence and streaming
        let planner = Planner::new();
        let verifier = Verifier::new();

        // Connect to DB if configured
        let mut db_client: Option<pg::Client> = None;
        if let Some(dsn) = &state.db_dsn {
            if let Ok((client, conn)) = pg::connect(dsn, pg::NoTls).await {
                tokio::spawn(async move { let _ = conn.await; });
                // Ensure optional extension tables exist (best-effort)
                let _ = ensure_optional_tables(&client).await;
                db_client = Some(client);
                if let Some(tx) = &state.event_tx {
                    let _ = tx.send(serde_json::json!({"event":"db_status","ok": true}).to_string());
                }
            } else if let Some(tx) = &state.event_tx {
                let _ = tx.send(serde_json::json!({"event":"db_status","ok": false, "message":"failed to connect for run persistence"}).to_string());
            }
        }

        // Insert run
        let mut run_id: Option<i64> = None;
        if let Some(client) = db_client.as_mut() {
            if let Ok(rows) = client.query("INSERT INTO runs(agent_profile,status) VALUES($1,$2) RETURNING id", &[&"default", &"running"]).await {
                if let Some(r) = rows.first() { let id: i32 = r.get(0); run_id = Some(id as i64); }
            }
        }
        // mark run start time for metrics
        let run_start = Instant::now();
        if let Some(tx) = &state.event_tx {
            let _ = tx.send(serde_json::to_string(&SseEvent{ event: "run_started".into(), run_id, plan_steps: None, step_idx: None, tool: None, status: None, message: Some("planning started".into()), chunk: None, stream: None, snippet: None, has_more: None, stdout_bytes: None, stderr_bytes: None, role: None, stage: None, json: None }).unwrap_or_else(|_| "{\"event\":\"run_started\"}".into()));
        }
        // Orchestrator: start run
        if let (Some(orch), Some(rid)) = (&state.orchestrator, run_id) {
            let mut guard = orch.lock().await;
            guard.start_run(rid);
        }

        // Draft plan (use agents if available)
        // Load top-K memories for planning context
        let mut planning_memories: Vec<serde_json::Value> = Vec::new();
        if let Some(client) = db_client.as_ref() {
            planning_memories = memory_query(client, "default", Some("%plan%"), 5).await;
        }
        // Include per-project guidelines if provided
        let mut mem_text_parts: Vec<String> = Vec::new();
        if let Some(pid) = &form.project_id {
            let g = load_guidelines(pid).await;
            if !g.text.trim().is_empty() {
                mem_text_parts.push(format!("Project guidelines for {}:\n{}", pid, g.text));
            }
        }
        // Include parent plan context (best-effort: interpret parent_plan_id as prior run_id)
        if let (Some(ppid), Some(client)) = (&form.parent_plan_id, db_client.as_ref()) {
            if let Ok(prior_run_id) = ppid.parse::<i64>() {
                // Fetch latest plan JSON for that run
                if let Ok(rows) = client.query("SELECT json FROM plans WHERE run_id=$1 ORDER BY id DESC LIMIT 1", &[&prior_run_id]).await {
                    if let Some(row) = rows.first() {
                        let pj: serde_json::Value = row.get(0);
                        mem_text_parts.push(format!("Parent plan (run {}):\n{}", prior_run_id, serde_json::to_string_pretty(&pj).unwrap_or_default()));
                    }
                }
                // Fetch latest verdict/rationales if any
                let q = r#"
                    SELECT v.status, v.rationale
                    FROM plan_verdicts v
                    JOIN plans p ON v.plan_id = p.id
                    WHERE p.run_id = $1
                    ORDER BY v.id DESC
                    LIMIT 1
                "#;
                if let Ok(rows) = client.query(q, &[&prior_run_id]).await {
                    if let Some(row) = rows.first() {
                        let status: String = row.get(0);
                        let rationale: String = row.get(1);
                        mem_text_parts.push(format!("Parent verdict: {} — {}", status, rationale));
                    }
                }
            }
        }
        if !planning_memories.is_empty() {
            let mut s = String::from("Relevant prior learnings:\n");
            for it in &planning_memories { if let Some(c) = it.get("content").and_then(|v| v.as_str()) { s.push_str("- "); s.push_str(c); s.push('\n'); } }
            mem_text_parts.push(s);
        }
        let mem_text = if mem_text_parts.is_empty() { None } else { Some(mem_text_parts.join("\n\n")) };

        let mut plan = if let Some(agents) = &state.agents {
            match agents.draft_plan_with_context(&form.objective, mem_text.as_deref().unwrap_or("")) .await {
                Ok(p) => p,
                Err(e) => {
                    if let Some(tx) = &state.event_tx {
                        let _ = tx.send(serde_json::json!({"event":"agents_error","message": e.to_string()}).to_string());
                    }
                    claritasai_core::Plan::from(planner.draft_plan(&form.objective))
                }
            }
        } else {
            claritasai_core::Plan::from(planner.draft_plan(&form.objective))
        };
        // Attach project/parent references
        plan.project_id = form.project_id.clone();
        plan.parent_plan_id = form.parent_plan_id.clone();
        // Agent message: Dev plan draft (when agents are enabled)
        if state.agents.is_some() {
            if let Some(tx) = &state.event_tx {
                let _ = tx.send(serde_json::to_string(&SseEvent{
                    event: "agent_message".into(),
                    run_id,
                    plan_steps: None,
                    step_idx: None,
                    tool: None,
                    status: Some("draft".into()),
                    message: Some("DevAgent drafted plan".into()),
                    chunk: None,
                    stream: None,
                    snippet: None,
                    has_more: None,
                    stdout_bytes: None,
                    stderr_bytes: None,
                    role: Some("Dev".into()),
                    stage: Some("plan_draft".into()),
                    json: Some(json!(plan)),
                }).unwrap_or_else(|_| "{\"event\":\"agent_message\"}".into()));
            }
            // Persist a brief agent memory
            if let Some(client) = db_client.as_mut() {
                let _ = client.query(
                    "INSERT INTO project_memories(project_key, kind, key, content, tags) VALUES($1,$2,$3,$4,$5)",
                    &[&"default", &"agent", &format!("run:{:?}:dev:plan_draft", run_id), &"DevAgent drafted plan", &"agent,dev,plan" ]
                ).await;
                // Persist detailed agent message
                let _ = client.query(
                    "INSERT INTO agent_messages(run_id, role, stage, content, json) VALUES($1,$2,$3,$4,$5)",
                    &[&run_id, &"Dev", &"plan_draft", &"DevAgent drafted plan", &json!(plan)]
                ).await;
            }
        }
        if let Some(client) = db_client.as_mut() {
            let _ = client.query("INSERT INTO plans(run_id, objective, json) VALUES($1,$2,$3)", &[&run_id, &plan.objective, &json!(plan)]).await;
            // Record initial plan revision as rev_no = 1 (best-effort)
            let _ = client.query(
                "INSERT INTO plan_revisions(run_id, rev_no, plan_json) VALUES($1,$2,$3)",
                &[&run_id, &1i32, &json!(plan)]
            ).await;
        }
        if let Some(tx) = &state.event_tx {
            let _ = tx.send(serde_json::to_string(&SseEvent{ event: "plan_saved".into(), run_id, plan_steps: Some(plan.steps.len()), step_idx: None, tool: None, status: Some("ok".into()), message: None, chunk: None, stream: None, snippet: None, has_more: None, stdout_bytes: None, stderr_bytes: None, role: None, stage: None, json: None }).unwrap_or_else(|_| "{\"event\":\"plan_saved\"}".into()));
        }
        // Orchestrator: plan saved -> move to Reviewing
        if let (Some(orch), Some(rid)) = (&state.orchestrator, run_id) {
            let mut guard = orch.lock().await;
            guard.plan_saved(rid);
        }

        // Verify (use agents QA + Manager if available)
        let verdict = if let Some(agents) = &state.agents {
            // Prepare memories for QA stage
            let mut qa_mems: Vec<serde_json::Value> = Vec::new();
            if let Some(client) = db_client.as_ref() { qa_mems = memory_query(client, "default", Some("%qa%"), 5).await; }
            let qa_mem_text = if qa_mems.is_empty() { "".to_string() } else { let mut s=String::from("Context:\n"); for it in &qa_mems { if let Some(c)=it.get("content").and_then(|v| v.as_str()){ s.push_str("- "); s.push_str(c); s.push('\n'); } } s };
            let mut qa = match agents.review_plan_with_context(&plan, &qa_mem_text).await {
                Ok(v) => v,
                Err(e) => {
                    if let Some(tx) = &state.event_tx {
                        let _ = tx.send(serde_json::json!({"event":"agents_error","stage":"qa","message": e.to_string()}).to_string());
                    }
                    verifier.review(&plan)
                }
            };
            // Emit QA agent message
            if let Some(tx) = &state.event_tx {
                let _ = tx.send(serde_json::to_string(&SseEvent{
                    event: "agent_message".into(),
                    run_id,
                    plan_steps: None,
                    step_idx: None,
                    tool: None,
                    status: Some(qa.status.clone()),
                    message: Some(qa.rationale.clone()),
                    chunk: None,
                    stream: None,
                    snippet: None,
                    has_more: None,
                    stdout_bytes: None,
                    stderr_bytes: None,
                    role: Some("QA".into()),
                    stage: Some("plan_review".into()),
                    json: Some(json!(qa)),
                }).unwrap_or_else(|_| "{\"event\":\"agent_message\"}".into()));
            }
            if let Some(client) = db_client.as_mut() {
                let _ = client.query(
                    "INSERT INTO project_memories(project_key, kind, key, content, tags) VALUES($1,$2,$3,$4,$5)",
                    &[&"default", &"agent", &format!("run:{:?}:qa:review", run_id), &format!("{}: {}", qa.status, qa.rationale), &"agent,qa,review" ]
                ).await;
                let _ = client.query(
                    "INSERT INTO agent_messages(run_id, role, stage, content, json) VALUES($1,$2,$3,$4,$5)",
                    &[&run_id, &"QA", &"plan_review", &qa.rationale, &json!(qa)]
                ).await;
            }
            // Bounded Dev <-> QA refinement loop when QA requests changes
            let mut plan_current = plan.clone();
            let mut rev_no: i32 = 1;
            let max_attempts: usize = state.agents_max_attempts.unwrap_or(2);
            let mut attempts: usize = 0;
            while qa.status.eq_ignore_ascii_case("NeedsChanges") && attempts < max_attempts {
                attempts += 1;
                let changes: Vec<String> = qa.required_changes.clone().unwrap_or_default();
                // Ask Dev to refine plan
                let refine_mem = if let Some(client) = db_client.as_ref() { memory_query(client, "default", Some("%plan%"), 3).await } else { Vec::new() };
                let refine_ctx = if refine_mem.is_empty() { String::new() } else { let mut s=String::new(); for it in &refine_mem { if let Some(c)=it.get("content").and_then(|v| v.as_str()){ s.push_str("- "); s.push_str(c); s.push('\n'); } } s };
                match agents.refine_plan_with_context(&plan_current, &changes, &refine_ctx).await {
                    Ok(new_plan) => {
                        plan_current = new_plan;
                        // Persist revision and emit agent message
                        if let Some(client) = db_client.as_mut() {
                            rev_no += 1;
                            let _ = client.query(
                                "INSERT INTO plan_revisions(run_id, rev_no, plan_json) VALUES($1,$2,$3)",
                                &[&run_id, &rev_no, &json!(plan_current)]
                            ).await;
                            // Also update the latest plan row json for convenience
                            let _ = client.query(
                                "UPDATE plans SET json = $1 WHERE run_id = $2 ORDER BY id DESC LIMIT 1",
                                &[&json!(plan_current), &run_id]
                            ).await;
                        }
                        if let Some(tx) = &state.event_tx {
                            let _ = tx.send(serde_json::to_string(&SseEvent{
                                event: "agent_message".into(),
                                run_id,
                                plan_steps: None,
                                step_idx: None,
                                tool: None,
                                status: Some("refined".into()),
                                message: Some("DevAgent refined plan per QA changes".into()),
                                chunk: None,
                                stream: None,
                                snippet: None,
                                has_more: None,
                                stdout_bytes: None,
                                stderr_bytes: None,
                                role: Some("Dev".into()),
                                stage: Some("plan_refine".into()),
                                json: Some(json!(plan_current)),
                            }).unwrap_or_else(|_| "{\"event\":\"agent_message\"}".into()));
                        }
                        if let Some(client) = db_client.as_mut() {
                            let _ = client.query(
                                "INSERT INTO agent_messages(run_id, role, stage, content, json) VALUES($1,$2,$3,$4,$5)",
                                &[&run_id, &"Dev", &"plan_refine", &"DevAgent refined plan", &json!(plan_current)]
                            ).await;
                        }
                        // QA reviews refined plan
                        qa = match agents.review_plan_with_context(&plan_current, &qa_mem_text).await {
                            Ok(v) => v,
                            Err(e) => {
                                if let Some(tx) = &state.event_tx {
                                    let _ = tx.send(serde_json::json!({"event":"agents_error","stage":"qa","message": e.to_string()}).to_string());
                                }
                                verifier.review(&plan_current)
                            }
                        };
                        // Emit QA follow-up message
                        if let Some(tx) = &state.event_tx {
                            let _ = tx.send(serde_json::to_string(&SseEvent{
                                event: "agent_message".into(),
                                run_id,
                                plan_steps: None,
                                step_idx: None,
                                tool: None,
                                status: Some(qa.status.clone()),
                                message: Some(qa.rationale.clone()),
                                chunk: None,
                                stream: None,
                                snippet: None,
                                has_more: None,
                                stdout_bytes: None,
                                stderr_bytes: None,
                                role: Some("QA".into()),
                                stage: Some("plan_review".into()),
                                json: Some(json!(qa)),
                            }).unwrap_or_else(|_| "{\"event\":\"agent_message\"}".into()));
                        }
                        if let Some(client) = db_client.as_mut() {
                            let _ = client.query(
                                "INSERT INTO agent_messages(run_id, role, stage, content, json) VALUES($1,$2,$3,$4,$5)",
                                &[&run_id, &"QA", &"plan_review", &qa.rationale, &json!(qa)]
                            ).await;
                        }
                    }
                    Err(e) => {
                        if let Some(tx) = &state.event_tx { let _ = tx.send(serde_json::json!({"event":"agents_error","stage":"dev_refine","message": e.to_string()}).to_string()); }
                        break;
                    }
                }
            }
            // If still NeedsChanges and attempts exhausted, emit a system notice
            if qa.status.eq_ignore_ascii_case("NeedsChanges") && attempts >= max_attempts {
                // SSE banner/card
                if let Some(tx) = &state.event_tx {
                    let _ = tx.send(serde_json::to_string(&SseEvent{
                        event: "agent_message".into(),
                        run_id,
                        plan_steps: None,
                        step_idx: None,
                        tool: None,
                        status: Some("exhausted".into()),
                        message: Some(format!("Dev↔QA refinement attempts exhausted after {} attempt(s). Proceeding to manager gate with last QA verdict.", attempts)),
                        chunk: None,
                        stream: None,
                        snippet: None,
                        has_more: None,
                        stdout_bytes: None,
                        stderr_bytes: None,
                        role: Some("System".into()),
                        stage: Some("devqa_exhausted".into()),
                        json: Some(json!({"attempts": attempts, "max_attempts": max_attempts, "qa": qa})),
                    }).unwrap_or_else(|_| "{\"event\":\"agent_message\"}".into()));
                }
                if let Some(client) = db_client.as_mut() {
                    let _ = client.query(
                        "INSERT INTO agent_messages(run_id, role, stage, content, json) VALUES($1,$2,$3,$4,$5)",
                        &[&run_id, &"System", &"devqa_exhausted", &format!("Attempts exhausted at {}", attempts), &json!({"attempts": attempts, "max_attempts": max_attempts})]
                    ).await;
                }
            }
            // Use refined plan going forward
            let plan = plan_current;
            // Manager context
            let mgr_ctx = if let Some(client) = db_client.as_ref() { memory_query(client, "default", Some("%mgr%"), 5).await } else { Vec::new() };
            let mgr_ctx_text = if mgr_ctx.is_empty() { String::new() } else { let mut s=String::from("Context for gate:\n"); for it in &mgr_ctx { if let Some(c)=it.get("content").and_then(|v| v.as_str()){ s.push_str("- "); s.push_str(c); s.push('\n'); } } s };
            match agents.manager_gate_with_context(&plan, &qa, &mgr_ctx_text).await {
                Ok(mv) => {
                    // Emit Manager message
                    if let Some(tx) = &state.event_tx {
                        let _ = tx.send(serde_json::to_string(&SseEvent{
                            event: "agent_message".into(),
                            run_id,
                            plan_steps: None,
                            step_idx: None,
                            tool: None,
                            status: Some(mv.status.clone()),
                            message: Some(mv.rationale.clone()),
                            chunk: None,
                            stream: None,
                            snippet: None,
                            has_more: None,
                            stdout_bytes: None,
                            stderr_bytes: None,
                            role: Some("Mgr".into()),
                            stage: Some("gate".into()),
                            json: Some(json!(mv)),
                        }).unwrap_or_else(|_| "{\"event\":\"agent_message\"}".into()));
                    }
                    if let Some(client) = db_client.as_mut() {
                        let _ = client.query(
                            "INSERT INTO project_memories(project_key, kind, key, content, tags) VALUES($1,$2,$3,$4,$5)",
                            &[&"default", &"agent", &format!("run:{:?}:mgr:gate", run_id), &format!("{}: {}", mv.status, mv.rationale), &"agent,mgr,gate" ]
                        ).await;
                        let _ = client.query(
                            "INSERT INTO agent_messages(run_id, role, stage, content, json) VALUES($1,$2,$3,$4,$5)",
                            &[&run_id, &"Mgr", &"gate", &mv.rationale, &json!(mv)]
                        ).await;
                    }
                    mv
                },
                Err(_) => qa,
            }
        } else {
            verifier.review(&plan)
        };
        if let Some(tx) = &state.event_tx {
            let _ = tx.send(serde_json::to_string(&SseEvent{ event: "verdict".into(), run_id, plan_steps: None, step_idx: None, tool: None, status: Some(verdict.status.clone()), message: Some(verdict.rationale.clone()), chunk: None, stream: None, snippet: None, has_more: None, stdout_bytes: None, stderr_bytes: None, role: None, stage: None, json: None }).unwrap_or_else(|_| "{\"event\":\"verdict\"}".into()));
        }
        if let Some(client) = db_client.as_mut() {
            let _ = client.query("INSERT INTO plan_verdicts(plan_id, status, rationale, json) SELECT id, $1, $2, $3 FROM plans WHERE run_id = $4 ORDER BY id DESC LIMIT 1",
                &[&verdict.status, &verdict.rationale, &json!(verdict), &run_id]).await;
        }

        // Execute if approved
        let mut combined = String::new();
        combined.push_str(&format!("<div>Objective: {}<br/>Plan steps: {}</div>", htmlescape::encode_minimal(&form.objective), plan.steps.len()));

        if verdict.status.eq_ignore_ascii_case("Approved") {
            if let Some(tx) = &state.event_tx {
                let _ = tx.send(serde_json::to_string(&SseEvent{ event: "execution_started".into(), run_id, plan_steps: None, step_idx: None, tool: None, status: None, message: None, chunk: None, stream: None, snippet: None, has_more: None, stdout_bytes: None, stderr_bytes: None, role: None, stage: None, json: None }).unwrap_or_else(|_| "{\"event\":\"execution_started\"}".into()));
            }
            // create execution row
            if let Some(client) = db_client.as_mut() {
                let _ = client.query("INSERT INTO executions(plan_id, status) SELECT id, $1 FROM plans WHERE run_id = $2 ORDER BY id DESC LIMIT 1",
                    &[&"running", &run_id]).await;
            }

            // Prepare per-execution context: project guidelines and parent context
            let exec_guidelines = if let Some(pid) = &plan.project_id { load_guidelines(pid).await } else { MemoryGuidelines::default() };
            let mut parent_ctx: Option<serde_json::Value> = None;
            if let (Some(ppid), Some(client)) = (&plan.parent_plan_id, db_client.as_ref()) {
                if let Ok(prior_run_id) = ppid.parse::<i64>() {
                    let mut obj = serde_json::json!({});
                    if let Ok(rows) = client.query("SELECT json FROM plans WHERE run_id=$1 ORDER BY id DESC LIMIT 1", &[&prior_run_id]).await {
                        if let Some(row) = rows.first() { obj["plan"] = row.get::<_, serde_json::Value>(0); }
                    }
                    let q = r#"
                        SELECT v.status, v.rationale
                        FROM plan_verdicts v
                        JOIN plans p ON v.plan_id = p.id
                        WHERE p.run_id = $1
                        ORDER BY v.id DESC
                        LIMIT 1
                    "#;
                    if let Ok(rows) = client.query(q, &[&prior_run_id]).await {
                        if let Some(row) = rows.first() {
                            let status: String = row.get(0);
                            let rationale: String = row.get(1);
                            obj["verdict"] = serde_json::json!({"status": status, "rationale": rationale});
                        }
                    }
                    parent_ctx = Some(obj);
                }
            }

            for (idx, step) in plan.steps.iter().enumerate() {
                if let Some(tx) = &state.event_tx {
                    let _ = tx.send(serde_json::to_string(&SseEvent{ event: "step_started".into(), run_id, plan_steps: None, step_idx: Some(idx as i32), tool: Some(step.tool_ref.clone()), status: None, message: None, chunk: None, stream: None, snippet: None, has_more: None, stdout_bytes: None, stderr_bytes: None, role: None, stage: None, json: None }).unwrap_or_else(|_| "{\"event\":\"step_started\"}".into()));
                }
                // persist step start
                if let Some(client) = db_client.as_mut() {
                    let _ = client.query(
                        "INSERT INTO steps(execution_id, idx, tool_ref, input_json, status) \
                         SELECT id, $1, $2, $3, $4 FROM executions WHERE plan_id = (SELECT id FROM plans WHERE run_id = $5 ORDER BY id DESC LIMIT 1) ORDER BY id DESC LIMIT 1",
                        &[&(idx as i32), &step.tool_ref, &json!(step.input), &"running", &run_id]
                    ).await;
                }
                let step_start = Instant::now();
                // Inject execution context into the tool input
                let mut injected_input = step.input.clone();
                // attach guidelines
                let g_json = serde_json::json!({
                    "text": exec_guidelines.text,
                    "metadata": exec_guidelines.metadata,
                });
                if let serde_json::Value::Object(ref mut map) = injected_input {
                    map.insert("__guidelines".into(), g_json);
                    if let Some(pc) = parent_ctx.clone() { map.insert("__parent".into(), pc); }
                }
                let call_res = reg.call(&step.tool_ref, injected_input).await;
                match call_res {
                    Ok(out) => {
                        // Emit partial chunks to UI for large outputs (stdout simulation)
                        if let Some(tx) = &state.event_tx {
                            let s = out.to_string();
                            let bytes = s.as_bytes();
                            let mut offset = 0usize;
                            // Use configurable limits (with sane defaults)
                            let max_chunks = if state.partial_max_chunks == 0 { 3usize } else { state.partial_max_chunks };
                            let mut sent = 0usize;
                            let chunk_size = if state.partial_chunk_size == 0 { 1024usize } else { state.partial_chunk_size };
                            while offset < bytes.len() && sent < max_chunks {
                                let end = (offset + chunk_size).min(bytes.len());
                                let chunk = String::from_utf8_lossy(&bytes[offset..end]).to_string();
                                let evt = SseEvent{ event: "step_partial".into(), run_id, plan_steps: None, step_idx: Some(idx as i32), tool: Some(step.tool_ref.clone()), status: None, message: None, chunk: Some(chunk), stream: Some("stdout".into()), snippet: None, has_more: None, stdout_bytes: None, stderr_bytes: None, role: None, stage: None, json: None };
                                let _ = tx.send(serde_json::to_string(&evt).unwrap_or_else(|_| "{\"event\":\"step_partial\"}".into()));
                                offset = end;
                                sent += 1;
                            }
                        }
                        // Persist stdout snippet and metrics
                        let stdout_full = out.to_string();
                        let stdout_bytes_f = stdout_full.as_bytes().len() as f64;
                        let stdout_bytes_u = stdout_full.as_bytes().len() as u64;
                        let mut stdout_snip = stdout_full.clone();
                        // Cap snippet to ~16KB
                        let cap: usize = 16 * 1024;
                        let mut has_more = false;
                        if stdout_snip.len() > cap { stdout_snip.truncate(cap); has_more = true; }
                        let safe = htmlescape::encode_minimal(&out.to_string());
                        combined.push_str(&format!("<div>step {} {} ok: <code>{}</code></div>", idx, htmlescape::encode_minimal(&step.tool_ref), safe));
                        if let Some(client) = db_client.as_mut() {
                            let _ = client.query(
                                "UPDATE steps SET output_json=$1, stdout_snip=$2, status='ok', finished_at=now() WHERE execution_id = (SELECT id FROM executions WHERE plan_id=(SELECT id FROM plans WHERE run_id=$3 ORDER BY id DESC LIMIT 1) ORDER BY id DESC LIMIT 1) AND idx=$4",
                                &[&out, &stdout_snip, &run_id, &(idx as i32)]
                            ).await;
                            // write per-step runtime metric
                            let elapsed_ms: f64 = step_start.elapsed().as_millis() as f64;
                            let _ = client.query(
                                "INSERT INTO metrics(run_id, key, value_num, source) VALUES($1,$2,$3,$4)",
                                &[&run_id, &format!("step_{}_runtime_ms", idx), &elapsed_ms, &"system".to_string()]
                            ).await;
                            // write per-step stdout bytes metric
                            let _ = client.query(
                                "INSERT INTO metrics(run_id, key, value_num, source) VALUES($1,$2,$3,$4)",
                                &[&run_id, &format!("step_{}_stdout_bytes", idx), &stdout_bytes_f, &"system".to_string()]
                            ).await;
                            // write a brief memory record
                            let mem_txt = format!("tool={} status=ok bytes={}", step.tool_ref, stdout_bytes_u);
                            let _ = client.query(
                                "INSERT INTO project_memories(project_key, kind, key, content, tags) VALUES($1,$2,$3,$4,$5)",
                                &[&"default", &"step", &format!("run:{:?}:step:{}", run_id, idx), &mem_txt, &"tool,ok" ]
                            ).await;
                            // If output exceeds snippet cap, store as artifact file
                            if has_more {
                                if let Ok(step_id) = fetch_step_id_for(&client, run_id, idx as i32).await {
                                    if let Ok((path, checksum, mime)) = write_artifact_file(run_id, step_id, "stdout", &stdout_full).await {
                                        let _ = client.query(
                                            "INSERT INTO artifacts(run_id, step_id, kind, path, mime, size_bytes, checksum) VALUES($1,$2,$3,$4,$5,$6,$7)",
                                            &[&run_id, &step_id, &"stdout".to_string(), &path, &mime, &((stdout_bytes_u) as i64), &checksum]
                                        ).await;
                                    }
                                }
                            }
                        }
                        if let Some(tx) = &state.event_tx {
                            let _ = tx.send(serde_json::to_string(&SseEvent{
                                event: "step_finished".into(),
                                run_id,
                                plan_steps: None,
                                step_idx: Some(idx as i32),
                                tool: Some(step.tool_ref.clone()),
                                status: Some("ok".into()),
                                message: None,
                                chunk: None,
                                stream: None,
                                snippet: Some(stdout_snip),
                                has_more: Some(has_more),
                                stdout_bytes: Some(stdout_bytes_u),
                                stderr_bytes: None,
                                role: None,
                                stage: None,
                                json: None,
                            }).unwrap_or_else(|_| "{\"event\":\"step_finished\"}".into()));
                        }
                    }
                    Err(e) => {
                        combined.push_str(&format!("<div>step {} {} failed: {}</div>", idx, htmlescape::encode_minimal(&step.tool_ref), htmlescape::encode_minimal(&e.to_string())));
                        // Precompute error details for DB and SSE
                        let err_txt = e.to_string();
                        let err_bytes_f = err_txt.as_bytes().len() as f64;
                        let err_bytes_u = err_txt.as_bytes().len() as u64;
                        let mut err_snip = err_txt.clone();
                        let cap: usize = 8 * 1024; // smaller cap for stderr snippet
                        let mut has_more = false;
                        if err_snip.len() > cap { err_snip.truncate(cap); has_more = true; }
                        if let Some(client) = db_client.as_mut() {
                            let _ = client.query(
                                "UPDATE steps SET output_json=$1, stderr_snip=$2, status='error', finished_at=now() WHERE execution_id = (SELECT id FROM executions WHERE plan_id=(SELECT id FROM plans WHERE run_id=$3 ORDER BY id DESC LIMIT 1) ORDER BY id DESC LIMIT 1) AND idx=$4",
                                &[&json!({"error": err_txt}), &err_snip, &run_id, &(idx as i32)]
                            ).await;
                            // write per-step runtime metric (even on error)
                            let elapsed_ms: f64 = step_start.elapsed().as_millis() as f64;
                            let _ = client.query(
                                "INSERT INTO metrics(run_id, key, value_num, value_text, source) VALUES($1,$2,$3,$4,$5)",
                                &[&run_id, &format!("step_{}_runtime_ms", idx), &elapsed_ms, &Some(err_snip.clone()), &"system".to_string()]
                            ).await;
                            // write per-step stderr bytes metric
                            let _ = client.query(
                                "INSERT INTO metrics(run_id, key, value_num, source) VALUES($1,$2,$3,$4)",
                                &[&run_id, &format!("step_{}_stderr_bytes", idx), &err_bytes_f, &"system".to_string()]
                            ).await;
                            // memory record
                            let mem_txt = format!("tool={} status=error bytes={} msg={}", step.tool_ref, err_bytes_u, err_snip);
                            let _ = client.query(
                                "INSERT INTO project_memories(project_key, kind, key, content, tags) VALUES($1,$2,$3,$4,$5)",
                                &[&"default", &"step", &format!("run:{:?}:step:{}", run_id, idx), &mem_txt, &"tool,error" ]
                            ).await;
                            // If error output exceeds snippet cap, store as artifact file
                            if has_more {
                                if let Ok(step_id) = fetch_step_id_for(&client, run_id, idx as i32).await {
                                    if let Ok((path, checksum, mime)) = write_artifact_file(run_id, step_id, "stderr", &err_txt).await {
                                        let _ = client.query(
                                            "INSERT INTO artifacts(run_id, step_id, kind, path, mime, size_bytes, checksum) VALUES($1,$2,$3,$4,$5,$6,$7)",
                                            &[&run_id, &step_id, &"stderr".to_string(), &path, &mime, &((err_bytes_u) as i64), &checksum]
                                        ).await;
                                    }
                                }
                            }
                        }
                        if let Some(tx) = &state.event_tx {
                            let _ = tx.send(serde_json::to_string(&SseEvent{
                                event: "step_finished".into(),
                                run_id,
                                plan_steps: None,
                                step_idx: Some(idx as i32),
                                tool: Some(step.tool_ref.clone()),
                                status: Some("error".into()),
                                message: Some(err_txt.clone()),
                                chunk: None,
                                stream: None,
                                snippet: Some(err_snip.clone()),
                                has_more: Some(has_more),
                                stdout_bytes: None,
                                stderr_bytes: Some(err_bytes_u),
                                role: None,
                                stage: None,
                                json: None,
                            }).unwrap_or_else(|_| "{\"event\":\"step_finished\"}".into()));
                        }
                    }
                }
            }
            // mark execution done
            if let Some(client) = db_client.as_mut() { let _ = client.query("UPDATE executions SET status='done', finished_at=now() WHERE plan_id=(SELECT id FROM plans WHERE run_id=$1 ORDER BY id DESC LIMIT 1) ORDER BY id DESC LIMIT 1", &[&run_id]).await; }
            if let Some(client) = db_client.as_mut() {
                let _ = client.query("UPDATE runs SET status='done' WHERE id=$1", &[&run_id]).await;
                // write total runtime metric
                let total_ms: f64 = run_start.elapsed().as_millis() as f64;
                let _ = client.query(
                    "INSERT INTO metrics(run_id, key, value_num, source) VALUES($1,$2,$3,$4)",
                    &[&run_id, &"total_runtime_ms".to_string(), &total_ms, &"system".to_string()]
                ).await;
                // also render a brief summary with a metrics link
                let (rid_text, metrics_href) = if let Some(rid) = run_id { (rid.to_string(), format!("/runs/{}/metrics", rid)) } else { ("N/A".to_string(), "#".to_string()) };
                combined.push_str(&format!(
                    "<div style=\"margin-top:8px;\"><strong>Total runtime:</strong> {} ms · <strong>Run ID:</strong> {} · <a href=\"{}\" target=\"_blank\">View metrics JSON</a></div>",
                    total_ms as u64,
                    htmlescape::encode_minimal(&rid_text),
                    htmlescape::encode_minimal(&metrics_href)
                ));
            }
            // notifications: run finished
            if let Some(hub) = &state.notifier_hub {
                if let Some(rid) = run_id {
                    let msg = NotifyMessage{ title: "ClaritasAI: run finished".into(), body: format!("Run #{} finished successfully.", rid), link: Some(format!("/runs/{}/metrics", rid)), run_id: Some(rid), tags: vec!["run".into(), "success".into()], };
                    hub.send_all(&msg);
                }
            }
            if let Some(tx) = &state.event_tx {
                let _ = tx.send(serde_json::to_string(&SseEvent{ event: "run_finished".into(), run_id, plan_steps: None, step_idx: None, tool: None, status: Some("done".into()), message: None, chunk: None, stream: None, snippet: None, has_more: None, stdout_bytes: None, stderr_bytes: None, role: None, stage: None, json: None }).unwrap_or_else(|_| "{\"event\":\"run_finished\"}".into()));
            }
        } else {
            if let Some(client) = db_client.as_mut() { let _ = client.query("UPDATE runs SET status='blocked' WHERE id=$1", &[&run_id]).await; }
            // notifications: run blocked
            if let Some(hub) = &state.notifier_hub {
                if let Some(rid) = run_id {
                    let msg = NotifyMessage{ title: "ClaritasAI: run blocked".into(), body: format!("Run #{} blocked by verifier.", rid), link: Some(format!("/chat?run={}", rid)), run_id: Some(rid), tags: vec!["run".into(), "blocked".into()], };
                    hub.send_all(&msg);
                }
            }
        }

        return Html(combined);
    }

    // Fallback path — direct calls per-host
    let cmd = if state.mcp_python_cmd.is_empty() {
        "claritas_mcp_python".to_string()
    } else {
        state.mcp_python_cmd.clone()
    };

    let mut combined = String::new();

    match StdioClient::call_method(&cmd, &[], "meta.ping", json!({})).await {
        Ok(result) => {
            let safe = htmlescape::encode_minimal(&result.to_string());
            combined.push_str(&format!(
                "<div>Objective: {}<br/>MCP python meta.ping: <code>{}</code></div>",
                htmlescape::encode_minimal(&form.objective),
                safe
            ));
        }
        Err(e) => {
            combined.push_str(&format!(
                "<div>Failed calling MCP host: {}</div>",
                htmlescape::encode_minimal(&e.to_string())
            ));
        }
    }

    if let Some(root) = &state.python_root {
        let lint_params = json!({"path": root});
        match StdioClient::call_method(&cmd, &[], "python.lint", lint_params).await {
            Ok(result) => {
                let safe = htmlescape::encode_minimal(&result.to_string());
                combined.push_str(&format!(
                    "<div>python.lint on {}: <code>{}</code></div>",
                    htmlescape::encode_minimal(root),
                    safe
                ));
            }
            Err(e) => {
                combined.push_str(&format!(
                    "<div>python.lint failed: {}</div>",
                    htmlescape::encode_minimal(&e.to_string())
                ));
            }
        }
    }

    // Rust: rust.cargo.build.debug on configured rust root
    let rust_cmd = if state.mcp_rust_cmd.is_empty() {
        "claritas_mcp_rust".to_string()
    } else {
        state.mcp_rust_cmd.clone()
    };
    if let Some(rroot) = &state.rust_root {
        let params = json!({"path": rroot});
        match StdioClient::call_method(&rust_cmd, &[], "rust.cargo.build.debug", params).await {
            Ok(result) => {
                let safe = htmlescape::encode_minimal(&result.to_string());
                combined.push_str(&format!(
                    "<div>rust.cargo.build.debug on {}: <code>{}</code></div>",
                    htmlescape::encode_minimal(rroot),
                    safe
                ));
            }
            Err(e) => {
                combined.push_str(&format!(
                    "<div>rust.cargo.build.debug failed: {}</div>",
                    htmlescape::encode_minimal(&e.to_string())
                ));
            }
        }
    }

    // Clarium: clarium.validate_sql with a simple SQL; use configured args (dsn/spec)
    let cl_cmd = if state.mcp_clarium_cmd.is_empty() {
        "claritas_mcp_clarium".to_string()
    } else {
        state.mcp_clarium_cmd.clone()
    };
    let sql = "SELECT 1;";
    match StdioClient::call_method(&cl_cmd, &state.mcp_clarium_args.iter().map(|s| s.as_str()).collect::<Vec<_>>(), "clarium.validate_sql", json!({"sql": sql})).await {
        Ok(result) => {
            let safe = htmlescape::encode_minimal(&result.to_string());
            combined.push_str(&format!(
                "<div>clarium.validate_sql: <code>{}</code></div>",
                safe
            ));
        }
        Err(e) => {
            combined.push_str(&format!(
                "<div>clarium.validate_sql failed: {}</div>",
                htmlescape::encode_minimal(&e.to_string())
            ));
        }
    }

    Html(combined)
}

async fn chat_stream(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    // Build a BroadcastStream in both branches to unify the concrete type
    let bs = if let Some(tx) = &state.event_tx {
        let rx = tx.subscribe();
        BroadcastStream::new(rx)
    } else {
        let (tx_local, rx_local) = broadcast::channel::<String>(8);
        // Emit a minimal db_status event based on configuration presence
        let db_evt = if state.db_dsn.is_some() {
            serde_json::json!({"event":"db_status","ok": true}).to_string()
        } else {
            serde_json::json!({"event":"db_status","ok": false, "message":"db not configured"}).to_string()
        };
        let _ = tx_local.send(db_evt);
        BroadcastStream::new(rx_local)
    };
    let stream = bs.filter_map(|msg| match msg {
        Ok(s) => Some(Ok::<Event, std::convert::Infallible>(Event::default().event("message").data(s))),
        Err(_) => None,
    });
    Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(10)))
}

// ------------- Metrics endpoint -------------
#[derive(serde::Serialize)]
struct MetricRow {
    id: i64,
    run_id: i64,
    key: String,
    value_num: Option<f64>,
    value_text: Option<String>,
    source: Option<String>,
    created_at: Option<String>,
}

// --- One-time optional table bootstrap (best-effort, idempotent) ---
async fn ensure_optional_tables(client: &pg::Client) -> Result<(), String> {
    // agent_messages
    let _ = client.query(
        "CREATE TABLE IF NOT EXISTS agent_messages (\n            id BIGSERIAL PRIMARY KEY,\n            run_id BIGINT,\n            role TEXT,\n            stage TEXT,\n            content TEXT,\n            json JSONB,\n            created_at TIMESTAMPTZ DEFAULT now()\n        )",
        &[]
    ).await.map_err(|e| e.to_string());
    let _ = client.query(
        "CREATE INDEX IF NOT EXISTS idx_agent_messages_run ON agent_messages(run_id, created_at)",
        &[]
    ).await.map_err(|e| e.to_string());

    // plan_revisions
    let _ = client.query(
        "CREATE TABLE IF NOT EXISTS plan_revisions (\n            id BIGSERIAL PRIMARY KEY,\n            run_id BIGINT,\n            rev_no INT,\n            plan_json JSONB,\n            created_at TIMESTAMPTZ DEFAULT now()\n        )",
        &[]
    ).await.map_err(|e| e.to_string());
    let _ = client.query(
        "CREATE INDEX IF NOT EXISTS idx_plan_revisions_run ON plan_revisions(run_id, rev_no)",
        &[]
    ).await.map_err(|e| e.to_string());

    // artifacts table optional columns (noop if table doesn't exist)
    let _ = client.query(
        "ALTER TABLE IF EXISTS artifacts ADD COLUMN IF NOT EXISTS checksum TEXT",
        &[]
    ).await.map_err(|e| e.to_string());

    Ok(())
}

async fn metrics_get(State(state): State<Arc<WebState>>, AxPath(id): AxPath<i64>) -> impl IntoResponse {
    if let Some(dsn) = &state.db_dsn {
        // best-effort connect and fetch
        match pg::connect(dsn, pg::NoTls).await {
            Ok((client, conn)) => {
                tokio::spawn(async move { let _ = conn.await; });
                let q = r#"SELECT id, run_id, key, value_num, value_text, source, to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') FROM metrics WHERE run_id = $1 ORDER BY id"#;
                match client.query(q, &[&id]).await {
                    Ok(rows) => {
                        let mut out: Vec<MetricRow> = Vec::with_capacity(rows.len());
                        for r in rows {
                            let idv: i64 = r.get(0);
                            let run_id: i64 = r.get(1);
                            let key: &str = r.get(2);
                            let value_num: Option<f64> = r.try_get(3).ok();
                            let value_text: Option<String> = r.try_get(4).ok();
                            let source: Option<String> = r.try_get(5).ok();
                            let created_at: Option<String> = r.try_get(6).ok();
                            out.push(MetricRow { id: idv, run_id, key: key.to_string(), value_num, value_text, source, created_at });
                        }
                        return axum::Json(serde_json::json!({"ok": true, "metrics": out})).into_response();
                    }
                    Err(e) => {
                        return axum::Json(serde_json::json!({"ok": false, "error": e.to_string()})).into_response();
                    }
                }
            }
            Err(e) => {
                return axum::Json(serde_json::json!({"ok": false, "error": format!("db connect failed: {}", e)})).into_response();
            }
        }
    }
    axum::Json(serde_json::json!({"ok": false, "error": "db not configured"})).into_response()
}
