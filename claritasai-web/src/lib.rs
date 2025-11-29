use axum::{
    extract::{Form, State},
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
// use claritasai_core types if needed later
use tokio_postgres as pg;
use serde_json::json;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio::time::Instant;
use tokio_stream::wrappers::BroadcastStream;

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
}

pub fn router(state: Arc<WebState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/chat", get(chat_get).post(chat_post))
        .route("/chat/stream", get(chat_stream))
        .route("/runs/:id/metrics", get(metrics_get))
        .with_state(state)
}

async fn health() -> &'static str { "OK" }

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
<div id="stream" hx-ext="sse" sse-connect="/chat/stream" sse-swap="message"></div>
</body></html>"##)
}

#[derive(Debug, serde::Deserialize)]
struct ChatForm { objective: String }

async fn chat_post(
    State(state): State<Arc<WebState>>,
    Form(form): Form<ChatForm>,
) -> impl IntoResponse {
    // Prefer routing through ToolRegistry if present; fallback to direct StdioClient calls
    if let Some(tx) = &state.event_tx { let _ = tx.send(format!("objective received: {}", form.objective)); }

    if let Some(reg) = &state.tool_registry {
        // MVP Planner → Verifier → Executor with persistence and streaming
        let planner = Planner::new();
        let verifier = Verifier::new();

        // Connect to DB if configured
        let mut db_client: Option<pg::Client> = None;
        if let Some(dsn) = &state.db_dsn {
            if let Ok((client, conn)) = pg::connect(dsn, pg::NoTls).await {
                tokio::spawn(async move { let _ = conn.await; });
                db_client = Some(client);
            } else if let Some(tx) = &state.event_tx {
                let _ = tx.send("[db] failed to connect for run persistence".into());
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
        if let Some(tx) = &state.event_tx { let _ = tx.send(format!("[run {:?}] planning started", run_id)); }

        // Draft plan
        let plan = planner.draft_plan(&form.objective);
        if let Some(client) = db_client.as_mut() {
            let _ = client.query("INSERT INTO plans(run_id, objective, json) VALUES($1,$2,$3)", &[&run_id, &plan.objective, &json!(plan)]).await;
        }

        // Verify
        let verdict = verifier.review(&plan);
        if let Some(tx) = &state.event_tx { let _ = tx.send(format!("[run {:?}] verdict: {}", run_id, verdict.status)); }
        if let Some(client) = db_client.as_mut() {
            let _ = client.query("INSERT INTO plan_verdicts(plan_id, status, rationale, json) SELECT id, $1, $2, $3 FROM plans WHERE run_id = $4 ORDER BY id DESC LIMIT 1",
                &[&verdict.status, &verdict.rationale, &json!(verdict), &run_id]).await;
        }

        // Execute if approved
        let mut combined = String::new();
        combined.push_str(&format!("<div>Objective: {}<br/>Plan steps: {}</div>", htmlescape::encode_minimal(&form.objective), plan.steps.len()));

        if verdict.status.eq_ignore_ascii_case("Approved") {
            if let Some(tx) = &state.event_tx { let _ = tx.send(format!("[run {:?}] execution started", run_id)); }
            // create execution row
            if let Some(client) = db_client.as_mut() {
                let _ = client.query("INSERT INTO executions(plan_id, status) SELECT id, $1 FROM plans WHERE run_id = $2 ORDER BY id DESC LIMIT 1",
                    &[&"running", &run_id]).await;
            }

            for (idx, step) in plan.steps.iter().enumerate() {
                if let Some(tx) = &state.event_tx { let _ = tx.send(format!("[run {:?}] step {} -> {}", run_id, idx, step.tool_ref)); }
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
                        let safe = htmlescape::encode_minimal(&out.to_string());
                        combined.push_str(&format!("<div>step {} {} ok: <code>{}</code></div>", idx, htmlescape::encode_minimal(&step.tool_ref), safe));
                        if let Some(client) = db_client.as_mut() {
                            let _ = client.query(
                                "UPDATE steps SET output_json=$1, status='ok', finished_at=now() WHERE execution_id = (SELECT id FROM executions WHERE plan_id=(SELECT id FROM plans WHERE run_id=$2 ORDER BY id DESC LIMIT 1) ORDER BY id DESC LIMIT 1) AND idx=$3",
                                &[&out, &run_id, &(idx as i32)]
                            ).await;
                            // write per-step runtime metric
                            let elapsed_ms: f64 = step_start.elapsed().as_millis() as f64;
                            let _ = client.query(
                                "INSERT INTO metrics(run_id, key, value_num, source) VALUES($1,$2,$3,$4)",
                                &[&run_id, &format!("step_{}_runtime_ms", idx), &elapsed_ms, &"system".to_string()]
                            ).await;
                        }
                    }
                    Err(e) => {
                        combined.push_str(&format!("<div>step {} {} failed: {}</div>", idx, htmlescape::encode_minimal(&step.tool_ref), htmlescape::encode_minimal(&e.to_string())));
                        if let Some(client) = db_client.as_mut() {
                            let _ = client.query(
                                "UPDATE steps SET output_json=$1, status='error', finished_at=now() WHERE execution_id = (SELECT id FROM executions WHERE plan_id=(SELECT id FROM plans WHERE run_id=$2 ORDER BY id DESC LIMIT 1) ORDER BY id DESC LIMIT 1) AND idx=$3",
                                &[&json!({"error": e.to_string()}), &run_id, &(idx as i32)]
                            ).await;
                            // write per-step runtime metric (even on error)
                            let elapsed_ms: f64 = step_start.elapsed().as_millis() as f64;
                            let _ = client.query(
                                "INSERT INTO metrics(run_id, key, value_num, value_text, source) VALUES($1,$2,$3,$4,$5)",
                                &[&run_id, &format!("step_{}_runtime_ms", idx), &elapsed_ms, &Some(e.to_string()), &"system".to_string()]
                            ).await;
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
            if let Some(tx) = &state.event_tx { let _ = tx.send(format!("[run {:?}] execution finished", run_id)); }
        } else {
            if let Some(client) = db_client.as_mut() { let _ = client.query("UPDATE runs SET status='blocked' WHERE id=$1", &[&run_id]).await; }
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
        let _ = tx_local.send("ClaritasAI stream connected".into());
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

async fn metrics_get(State(state): State<Arc<WebState>>, axum::extract::Path(id): axum::extract::Path<i64>) -> impl IntoResponse {
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
