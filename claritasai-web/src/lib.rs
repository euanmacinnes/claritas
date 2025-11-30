use axum::{
    extract::{Form, State, Path as AxPath},
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
use tokio_stream::StreamExt;
use tokio::time::Instant;
use tokio_stream::wrappers::BroadcastStream;
use claritasai_notify::{NotifierHub, NotifyMessage};
use serde::{Serialize, Deserialize};
use std::fs;
use std::path::{Path as FsPath, PathBuf};
use tokio::fs as tfs;
use axum::Json;
use axum::http::{HeaderMap, header, StatusCode};
use sha2::{Sha256, Digest};

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
        .with_state(state)
}

async fn health() -> &'static str { "OK" }

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

async fn chat_get() -> Html<&'static str> {
    Html(r##"<!doctype html>
<html><head><meta charset='utf-8'><title>ClaritasAI Chat</title>
<script src="https://unpkg.com/htmx.org@1.9.12"></script></head>
<body>
<h1>ClaritasAI</h1>
<form hx-post="/chat" hx-target="#out" hx-swap="beforeend">
  <input type="text" name="objective" placeholder="Enter objective" style="width:60%" />
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
struct ChatForm { objective: String }

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

        // Draft plan (use agents if available)
        // Load top-K memories for planning context
        let mut planning_memories: Vec<serde_json::Value> = Vec::new();
        if let Some(client) = db_client.as_ref() {
            planning_memories = memory_query(client, "default", Some("%plan%"), 5).await;
        }
        let mem_text = if planning_memories.is_empty() { None } else {
            let mut s = String::from("Relevant prior learnings:\n");
            for it in &planning_memories { if let Some(c) = it.get("content").and_then(|v| v.as_str()) { s.push_str("- "); s.push_str(c); s.push('\n'); } }
            Some(s)
        };
        let plan = if let Some(agents) = &state.agents {
            match agents.draft_plan_with_context(&form.objective, mem_text.as_deref().unwrap_or("")) .await {
                Ok(p) => p,
                Err(e) => {
                    if let Some(tx) = &state.event_tx {
                        let _ = tx.send(serde_json::json!({"event":"agents_error","message": e.to_string()}).to_string());
                    }
                    planner.draft_plan(&form.objective)
                }
            }
        } else {
            planner.draft_plan(&form.objective)
        };
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
                let call_res = reg.call(&step.tool_ref, step.input.clone()).await;
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
