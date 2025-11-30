use clap::Parser;
use serde_json::{json, Value};
use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, select};
use tracing::{error, info};
use tokio_postgres as pg;
use tokio_postgres::types::ToSql;

#[derive(Parser, Debug)]
struct Cli {
    /// Return 200 OK JSON for health
    #[arg(long, default_value_t = false)]
    ping: bool,

    /// Print capabilities JSON and exit
    #[arg(long, default_value_t = false)]
    capabilities: bool,

    /// Clarium Postgres DSN (e.g., postgres://localhost:5433/claritas)
    #[arg(long)]
    dsn: Option<String>,

    /// Path to Clarium spec root
    #[arg(long)]
    spec: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let cli = Cli::parse();

    if cli.ping {
        println!("{}", json!({"ok": true, "name": "claritas_mcp_clarium"}));
        return Ok(());
    }
    if cli.capabilities {
        println!(
            "{}",
            json!({
                "tools": [
                    "clarium.generate_sql","clarium.validate_sql","clarium.parse","clarium.execute",
                    "clarium.execute.extended",
                    "db.schema.inspect","db.schema.diff","db.index.suggest","db.query.explain",
                    "db.query.explain.extended",
                    "meta.ping","meta.capabilities"
                ]
            })
        );
        return Ok(());
    }

    info!(dsn = ?cli.dsn, spec = ?cli.spec, "claritas_mcp_clarium starting JSON-RPC (stdio)");
    let mut stdin = BufReader::new(tokio::io::stdin());
    let mut stdout = tokio::io::stdout();
    let mut buf = String::new();

    loop {
        buf.clear();
        select! {
            read_res = stdin.read_line(&mut buf) => {
                match read_res {
                    Ok(0) => break,
                    Ok(_) => {
                        if let Err(e) = handle_line(&buf, &mut stdout, cli.dsn.clone(), cli.spec.clone()).await { error!(error=%e, "handler error"); }
                    }
                    Err(e) => { error!(error=%e, "stdin read error"); break; }
                }
            }
            _ = tokio::signal::ctrl_c() => { break; }
        }
    }

    Ok(())
}

async fn handle_line(
    line: &str,
    stdout: &mut tokio::io::Stdout,
    dsn: Option<String>,
    _spec: Option<String>,
) -> anyhow::Result<()> {
    let req: Value = match serde_json::from_str(line) { Ok(v) => v, Err(_) => return Ok(()) };
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let params = req.get("params").cloned().unwrap_or(json!({}));

    let result = match method {
        "meta.ping" => json!({"ok": true, "name": "claritas_mcp_clarium"}),
        "meta.capabilities" => json!({"tools":[
            "clarium.generate_sql","clarium.validate_sql","clarium.parse","clarium.execute",
            "db.schema.inspect","db.schema.diff","db.index.suggest","db.query.explain"
        ]}),
        "clarium.validate_sql" => {
            // Validate SQL by using PREPARE against the configured Postgres DSN
            let sql = params.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            if let Some(ref dsn_s) = dsn {
                match validate_with_postgres(dsn_s, sql).await {
                    Ok(v) => v,
                    Err(e) => {
                        if let Some(db) = e.downcast_ref::<pg::Error>() {
                            json!({"ok": false, "error": db_error_json(db)})
                        } else {
                            json!({"ok": false, "error": {"message": e.to_string()}})
                        }
                    },
                }
            } else {
                json!({"ok": false, "error": "missing DSN for clarium.validate_sql"})
            }
        }
        "clarium.parse" => {
            // Use EXPLAIN (PARSE only) equivalent by asking Postgres for a plan without execution
            let sql = params.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            if let Some(ref dsn_s) = dsn {
                match explain_with_postgres(dsn_s, sql, false).await {
                    Ok(v) => v,
                    Err(e) => {
                        if let Some(db) = e.downcast_ref::<pg::Error>() {
                            json!({"ok": false, "error": db_error_json(db)})
                        } else {
                            json!({"ok": false, "error": {"message": e.to_string()}})
                        }
                    },
                }
            } else {
                json!({"ok": false, "error": "missing DSN for clarium.parse"})
            }
        }
        "clarium.execute" => {
            // Execute SQL with row limiting. Params may include { sql, limit }
            let sql = params.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as i64;
            if let Some(ref dsn_s) = dsn {
                match execute_with_postgres(dsn_s, sql, limit).await {
                    Ok(v) => v,
                    Err(e) => {
                        if let Some(db) = e.downcast_ref::<pg::Error>() {
                            json!({"ok": false, "error": db_error_json(db)})
                        } else {
                            json!({"ok": false, "error": {"message": e.to_string()}})
                        }
                    },
                }
            } else {
                json!({"ok": false, "error": "missing DSN for clarium.execute"})
            }
        }
        "clarium.execute.extended" => {
            // Extended execute with positional parameters: { sql, params: [], limit? }
            let sql = params.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as i64;
            let param_values: Vec<Value> = params.get("params").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            if let Some(ref dsn_s) = dsn {
                match execute_with_postgres_ext(dsn_s, sql, &param_values, limit).await {
                    Ok(v) => v,
                    Err(e) => {
                        if let Some(db) = e.downcast_ref::<pg::Error>() { json!({"ok": false, "error": db_error_json(db)}) }
                        else { json!({"ok": false, "error": {"message": e.to_string()}}) }
                    }
                }
            } else {
                json!({"ok": false, "error": "missing DSN for clarium.execute.extended"})
            }
        }
        "db.query.explain" => {
            // Params: { sql: string, analyze?: bool }
            let sql = params.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            let analyze = params.get("analyze").and_then(|v| v.as_bool()).unwrap_or(false);
            if let Some(ref dsn_s) = dsn {
                match explain_with_postgres(dsn_s, sql, analyze).await {
                    Ok(v) => v,
                    Err(e) => {
                        if let Some(db) = e.downcast_ref::<pg::Error>() {
                            json!({"ok": false, "error": db_error_json(db)})
                        } else {
                            json!({"ok": false, "error": {"message": e.to_string()}})
                        }
                    },
                }
            } else {
                json!({"ok": false, "error": "missing DSN for db.query.explain"})
            }
        }
        "db.query.explain.extended" => {
            // Params: { sql: string, params: [], analyze?: bool }
            let sql = params.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            let analyze = params.get("analyze").and_then(|v| v.as_bool()).unwrap_or(false);
            let param_values: Vec<Value> = params.get("params").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            if let Some(ref dsn_s) = dsn {
                match explain_with_postgres_ext(dsn_s, sql, &param_values, analyze).await {
                    Ok(v) => v,
                    Err(e) => {
                        if let Some(db) = e.downcast_ref::<pg::Error>() { json!({"ok": false, "error": db_error_json(db)}) }
                        else { json!({"ok": false, "error": {"message": e.to_string()}}) }
                    }
                }
            } else {
                json!({"ok": false, "error": "missing DSN for db.query.explain.extended"})
            }
        }
        "db.schema.inspect" => {
            if let Some(ref dsn_s) = dsn {
                match inspect_schemas(dsn_s).await {
                    Ok(v) => v,
                    Err(e) => {
                        if let Some(db) = e.downcast_ref::<pg::Error>() {
                            json!({"ok": false, "error": db_error_json(db)})
                        } else {
                            json!({"ok": false, "error": {"message": e.to_string()}})
                        }
                    },
                }
            } else {
                json!({"ok": false, "error": "missing DSN for db.schema.inspect"})
            }
        }
        "db.schema.diff" => {
            // Compare schemas (tables and columns) between two schema names
            // Params: { left: string, right: string }
            let left = params.get("left").and_then(|v| v.as_str()).unwrap_or("");
            let right = params.get("right").and_then(|v| v.as_str()).unwrap_or("");
            if left.is_empty() || right.is_empty() {
                json!({"ok": false, "error": "params {left,right} are required"})
            } else if let Some(ref dsn_s) = dsn {
                match diff_schemas(dsn_s, left, right).await {
                    Ok(v) => v,
                    Err(e) => {
                        if let Some(db) = e.downcast_ref::<pg::Error>() {
                            json!({"ok": false, "error": db_error_json(db)})
                        } else {
                            json!({"ok": false, "error": {"message": e.to_string()}})
                        }
                    },
                }
            } else {
                json!({"ok": false, "error": "missing DSN for db.schema.diff"})
            }
        }
        "db.index.suggest" => {
            // Params: { table: string, workload?: [{query: string, freq?: number}] }
            let table = params.get("table").and_then(|v| v.as_str()).unwrap_or("");
            let workload = params.get("workload").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            if table.is_empty() {
                json!({"ok": false, "error": "param {table} is required"})
            } else if let Some(ref dsn_s) = dsn {
                match index_suggest(dsn_s, table, workload).await {
                    Ok(v) => v,
                    Err(e) => json!({"ok": false, "error": {"message": e.to_string()}}),
                }
            } else {
                json!({"ok": false, "error": "missing DSN for db.index.suggest"})
            }
        }
        _ => json!({"error": {"code": -32601, "message": format!("method not found: {}", method)}}),
    };

    let resp = json!({"id": id, "result": result});
    let s = serde_json::to_string(&resp)?;
    stdout.write_all(s.as_bytes()).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await?;
    Ok(())
}

// --- Postgres helpers (placeholder/simple) ---

/// Very naive normalization: replace Clarium-style named bind variables (e.g., :user_id)
/// with NULL so Postgres can parse the statement without requiring parameter types/values.
/// This is intended only for validate/parse flows, not for execution.
fn normalize_named_binds(sql: &str) -> String {
    // Match occurrences like :name (where name starts with a letter/underscore and continues with word chars)
    // Avoid replacing '::' casts by requiring a word boundary or whitespace before ':' when possible.
    // Simpler approach: replace ":identifier" not preceded by ':' (to avoid '::type').
    let re = regex::Regex::new(r"(?s)(?P<prefix>[^:]|^):(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)").unwrap();
    let mut out = String::with_capacity(sql.len());
    let mut last = 0usize;
    for cap in re.captures_iter(sql) {
        let m = cap.get(0).unwrap();
        // prefix is the char before ':' (or start anchor)
        let prefix = cap.name("prefix").unwrap().as_str();
        out.push_str(&sql[last..m.start()]);
        out.push_str(prefix);
        // Replace the entire ":name" with NULL
        out.push_str("NULL");
        last = m.end();
    }
    out.push_str(&sql[last..]);
    out
}

async fn validate_with_postgres(dsn: &str, sql: &str) -> anyhow::Result<Value> {
    // Use PREPARE to validate syntax and types without executing.
    // For validate, we normalize named binds to NULL to allow parsing without parameter metadata.
    let sql_norm = normalize_named_binds(sql);
    let (client, conn) = pg::connect(dsn, pg::NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let stmt_text = format!("PREPARE claritasai_tmp AS {}", sql_norm);
    let res = client.batch_execute(&stmt_text).await;
    // Try to DEALLOCATE even if prepare failed
    let _ = client.batch_execute("DEALLOCATE ALL").await;
    match res {
        Ok(_) => Ok(json!({"ok": true})),
        Err(e) => {
            if let Some(db) = e.as_db_error() {
                Ok(json!({"ok": false, "error": db_error_json_from_db(db)}))
            } else {
                Ok(json!({"ok": false, "error": {"message": e.to_string()}}))
            }
        },
    }
}

async fn explain_with_postgres(dsn: &str, sql: &str, analyze: bool) -> anyhow::Result<Value> {
    let (client, conn) = pg::connect(dsn, pg::NoTls).await?;
    tokio::spawn(async move { let _ = conn.await; });
    // Normalize named binds to NULL for parse/explain.
    let prefix = if analyze { "EXPLAIN (ANALYZE, VERBOSE, COSTS) " } else { "EXPLAIN (VERBOSE, COSTS) " };
    let q = format!("{}{}", prefix, normalize_named_binds(sql));
    let rows = client.query(q.as_str(), &[]).await?;
    let mut plan_lines: Vec<String> = Vec::new();
    for r in rows { let txt: &str = r.get(0); plan_lines.push(txt.to_string()); }
    if analyze {
        // Parse timing lines like "Planning Time: 1.234 ms" and "Execution Time: 5.678 ms"
        let mut planning_ms: Option<f64> = None;
        let mut execution_ms: Option<f64> = None;
        for l in &plan_lines {
            let s = l.trim();
            if s.starts_with("Planning Time:") {
                if let Some(val) = s.split(':').nth(1) { planning_ms = extract_ms(val); }
            } else if s.starts_with("Execution Time:") {
                if let Some(val) = s.split(':').nth(1) { execution_ms = extract_ms(val); }
            }
        }
        let total_runtime_ms = execution_ms; // Postgres reports Execution Time as total runtime
        Ok(json!({"ok": true, "plan": plan_lines, "planning_ms": planning_ms, "execution_ms": execution_ms, "total_runtime_ms": total_runtime_ms}))
    } else {
        Ok(json!({"ok": true, "plan": plan_lines}))
    }
}

/// Convert JSON params into Postgres ToSql arguments.
fn build_param_refs(params: &[Value]) -> (Vec<Box<dyn ToSql + Sync>>, Vec<&(dyn ToSql + Sync)>) {
    let mut boxed: Vec<Box<dyn ToSql + Sync>> = Vec::with_capacity(params.len());
    for v in params {
        match v {
            Value::Null => {
                // Use Option::<String>::None to represent NULL
                let none: Option<String> = None;
                boxed.push(Box::new(none));
            }
            Value::Bool(b) => boxed.push(Box::new(*b)),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() { boxed.push(Box::new(i)); }
                else if let Some(u) = n.as_u64() { boxed.push(Box::new(u as i64)); }
                else if let Some(f) = n.as_f64() { boxed.push(Box::new(f)); }
                else { boxed.push(Box::new(n.to_string())); }
            }
            Value::String(s) => boxed.push(Box::new(s.clone())),
            other @ Value::Array(_) | other @ Value::Object(_) => {
                // Pass through as JSON (requires with-serde_json feature)
                boxed.push(Box::new(other.clone()));
            }
        }
    }
    let ptrs: Vec<&(dyn ToSql + Sync)> = boxed.iter().map(|b| &**b as &(dyn ToSql + Sync)).collect();
    (boxed, ptrs)
}

async fn execute_with_postgres_ext(dsn: &str, sql: &str, params: &[Value], limit: i64) -> anyhow::Result<Value> {
    use tokio::time::Instant;
    let start = Instant::now();
    let (client, conn) = pg::connect(dsn, pg::NoTls).await?;
    tokio::spawn(async move { let _ = conn.await; });
    let trimmed = sql.trim().to_ascii_lowercase();
    let (owned, refs) = build_param_refs(params);
    if trimmed.starts_with("select") {
        let q = format!("WITH _q AS ({}) SELECT * FROM _q LIMIT {}", sql, limit);
        let rows = client.query(q.as_str(), &refs).await?;
        let elapsed_ms = start.elapsed().as_millis() as u64;
        let mut out_rows: Vec<Vec<String>> = Vec::new();
        for row in rows {
            let mut cols: Vec<String> = Vec::new();
            for i in 0..row.len() {
                let v: Result<String, _> = row.try_get(i);
                cols.push(v.unwrap_or_else(|_| "<unrepr>".into()));
            }
            out_rows.push(cols);
        }
        // keep owned alive
        std::mem::drop(owned);
        Ok(json!({"ok": true, "rows": out_rows, "elapsed_ms": elapsed_ms}))
    } else {
        let res = client.execute(sql, &refs).await;
        let elapsed_ms = start.elapsed().as_millis() as u64;
        std::mem::drop(owned);
        match res {
            Ok(affected) => Ok(json!({"ok": true, "rows_affected": affected as i64, "elapsed_ms": elapsed_ms})),
            Err(e) => {
                if let Some(db) = e.as_db_error() {
                    Ok(json!({"ok": false, "error": db_error_json_from_db(db)}))
                } else {
                    Ok(json!({"ok": false, "error": {"message": e.to_string()}}))
                }
            }
        }
    }
}

async fn explain_with_postgres_ext(dsn: &str, sql: &str, params: &[Value], analyze: bool) -> anyhow::Result<Value> {
    let (client, conn) = pg::connect(dsn, pg::NoTls).await?;
    tokio::spawn(async move { let _ = conn.await; });
    let prefix = if analyze { "EXPLAIN (ANALYZE, VERBOSE, COSTS) " } else { "EXPLAIN (VERBOSE, COSTS) " };
    let q = format!("{}{}", prefix, sql);
    let (_owned, refs) = build_param_refs(params);
    let rows = client.query(q.as_str(), &refs).await?;
    let mut plan_lines: Vec<String> = Vec::new();
    for r in rows { let txt: &str = r.get(0); plan_lines.push(txt.to_string()); }
    if analyze {
        let mut planning_ms: Option<f64> = None;
        let mut execution_ms: Option<f64> = None;
        for l in &plan_lines {
            let s = l.trim();
            if s.starts_with("Planning Time:") {
                if let Some(val) = s.split(':').nth(1) { planning_ms = extract_ms(val); }
            } else if s.starts_with("Execution Time:") {
                if let Some(val) = s.split(':').nth(1) { execution_ms = extract_ms(val); }
            }
        }
        let total_runtime_ms = execution_ms;
        Ok(json!({"ok": true, "plan": plan_lines, "planning_ms": planning_ms, "execution_ms": execution_ms, "total_runtime_ms": total_runtime_ms}))
    } else {
        Ok(json!({"ok": true, "plan": plan_lines}))
    }
}

async fn execute_with_postgres(dsn: &str, sql: &str, limit: i64) -> anyhow::Result<Value> {
    use tokio::time::Instant;
    let start = Instant::now();
    let (client, conn) = pg::connect(dsn, pg::NoTls).await?;
    tokio::spawn(async move { let _ = conn.await; });
    // Wrap in a CTE to enforce limit if it's a SELECT; otherwise just execute
    let trimmed = sql.trim().to_ascii_lowercase();
    if trimmed.starts_with("select") {
        let q = format!("WITH _q AS ({}) SELECT * FROM _q LIMIT {}", sql, limit);
        let rows = client.query(q.as_str(), &[]).await?;
        let elapsed_ms = start.elapsed().as_millis() as u64;
        // Convert rows to JSON-ish arrays (textual)
        let mut out_rows: Vec<Vec<String>> = Vec::new();
        for row in rows {
            let mut cols: Vec<String> = Vec::new();
            for i in 0..row.len() {
                let v: Result<String, _> = row.try_get(i);
                cols.push(v.unwrap_or_else(|_| "<unrepr>".into()));
            }
            out_rows.push(cols);
        }
        Ok(json!({"ok": true, "rows": out_rows, "elapsed_ms": elapsed_ms}))
    } else {
        // Non-select: just execute and return rows_affected
        let res = client.batch_execute(sql).await;
        let elapsed_ms = start.elapsed().as_millis() as u64;
        match res {
            Ok(_) => Ok(json!({"ok": true, "elapsed_ms": elapsed_ms})),
            Err(e) => {
                if let Some(db) = e.as_db_error() {
                    Ok(json!({"ok": false, "error": db_error_json_from_db(db)}))
                } else {
                    Ok(json!({"ok": false, "error": {"message": e.to_string()}}))
                }
            },
        }
    }
}

async fn inspect_schemas(dsn: &str) -> anyhow::Result<Value> {
    let (client, conn) = pg::connect(dsn, pg::NoTls).await?;
    tokio::spawn(async move { let _ = conn.await; });
    let rows = client
        .query(
            "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
            &[],
        )
        .await?;
    let mut schemas: Vec<String> = Vec::new();
    for r in rows {
        let s: &str = r.get(0);
        schemas.push(s.to_string());
    }
    Ok(json!({"ok": true, "schemas": schemas}))
}

async fn diff_schemas(dsn: &str, left: &str, right: &str) -> anyhow::Result<Value> {
    // Fetch tables and columns for both schemas and compute set diffs
    let (client, conn) = pg::connect(dsn, pg::NoTls).await?;
    tokio::spawn(async move { let _ = conn.await; });

    // helper to get tables
    async fn get_tables(client: &pg::Client, schema: &str) -> anyhow::Result<Vec<String>> {
        let rows = client
            .query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name",
                &[&schema],
            )
            .await?;
        Ok(rows.iter().map(|r| r.get::<_, &str>(0).to_string()).collect())
    }

    // helper to get columns map: table -> [columns]
    async fn get_columns(client: &pg::Client, schema: &str) -> anyhow::Result<std::collections::HashMap<String, Vec<String>>> {
        let rows = client
            .query(
                "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = $1 ORDER BY table_name, ordinal_position",
                &[&schema],
            )
            .await?;
        let mut map: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
        for r in rows {
            let t: &str = r.get(0);
            let c: &str = r.get(1);
            map.entry(t.to_string()).or_default().push(c.to_string());
        }
        Ok(map)
    }

    let left_tables = get_tables(&client, left).await?;
    let right_tables = get_tables(&client, right).await?;

    let left_set: std::collections::HashSet<_> = left_tables.iter().cloned().collect();
    let right_set: std::collections::HashSet<_> = right_tables.iter().cloned().collect();

    let tables_only_left: Vec<String> = left_set.difference(&right_set).cloned().collect();
    let tables_only_right: Vec<String> = right_set.difference(&left_set).cloned().collect();
    let tables_common: Vec<String> = left_set.intersection(&right_set).cloned().collect();

    let left_cols = get_columns(&client, left).await?;
    let right_cols = get_columns(&client, right).await?;

    // column diffs for common tables
    let mut column_diffs: Vec<Value> = Vec::new();
    for t in tables_common.iter() {
        let lcols: std::collections::HashSet<String> = left_cols.get(t).cloned().unwrap_or_default().into_iter().collect();
        let rcols: std::collections::HashSet<String> = right_cols.get(t).cloned().unwrap_or_default().into_iter().collect();
        let only_left: Vec<String> = lcols.difference(&rcols).cloned().collect();
        let only_right: Vec<String> = rcols.difference(&lcols).cloned().collect();
        if !only_left.is_empty() || !only_right.is_empty() {
            column_diffs.push(json!({
                "table": t,
                "columns_only_left": only_left,
                "columns_only_right": only_right
            }));
        }
    }

    // indexes
    async fn get_indexes(client: &pg::Client, schema: &str) -> anyhow::Result<Vec<Value>> {
        let q = r#"
            SELECT schemaname, tablename, indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = $1
            ORDER BY tablename, indexname
        "#;
        let rows = client.query(q, &[&schema]).await?;
        let mut out = Vec::new();
        for r in rows {
            let schemaname: &str = r.get(0);
            let table: &str = r.get(1);
            let name: &str = r.get(2);
            let def: &str = r.get(3);
            out.push(json!({"schema": schemaname, "table": table, "name": name, "def": def}));
        }
        Ok(out)
    }
    let left_indexes = get_indexes(&client, left).await?;
    let right_indexes = get_indexes(&client, right).await?;

    // constraints
    async fn get_constraints(client: &pg::Client, schema: &str) -> anyhow::Result<Vec<Value>> {
        let q = r#"
            SELECT tc.table_name, tc.constraint_name, tc.constraint_type,
                   kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            LEFT JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            LEFT JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
            WHERE tc.table_schema = $1
            ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
        "#;
        let rows = client.query(q, &[&schema]).await?;
        let mut grouped: std::collections::BTreeMap<(String,String,String), Vec<Value>> = std::collections::BTreeMap::new();
        for r in rows {
            let table: &str = r.get(0);
            let cname: &str = r.get(1);
            let ctype: &str = r.get(2);
            let col: Option<&str> = r.try_get(3).ok();
            let ftable: Option<&str> = r.try_get(4).ok();
            let fcol: Option<&str> = r.try_get(5).ok();
            grouped.entry((table.to_string(), cname.to_string(), ctype.to_string()))
                .or_default()
                .push(json!({"column": col, "ref_table": ftable, "ref_column": fcol}));
        }
        let mut out = Vec::new();
        for ((table, cname, ctype), cols) in grouped {
            out.push(json!({"table": table, "name": cname, "type": ctype, "details": cols}));
        }
        Ok(out)
    }
    let left_constraints = get_constraints(&client, left).await?;
    let right_constraints = get_constraints(&client, right).await?;

    // human-readable summary
    let mut summary: Vec<String> = Vec::new();
    for t in &tables_only_left { summary.push(format!("- only in {}: table {}", left, t)); }
    for t in &tables_only_right { summary.push(format!("- only in {}: table {}", right, t)); }
    for diff in &column_diffs {
        if let (Some(table), Some(only_l), Some(only_r)) = (
            diff.get("table").and_then(|v| v.as_str()),
            diff.get("columns_only_left").and_then(|v| v.as_array()),
            diff.get("columns_only_right").and_then(|v| v.as_array()),
        ) {
            if !only_l.is_empty() { summary.push(format!("- {}: cols only in {}: {}", table, left, join_strs(only_l))); }
            if !only_r.is_empty() { summary.push(format!("- {}: cols only in {}: {}", table, right, join_strs(only_r))); }
        }
    }

    Ok(json!({
        "ok": true,
        "left": left,
        "right": right,
        "tables_only_left": tables_only_left,
        "tables_only_right": tables_only_right,
        "column_diffs": column_diffs,
        "indexes": { left: left_indexes, right: right_indexes },
        "constraints": { left: left_constraints, right: right_constraints },
        "summary": summary
    }))
}

// --- Error normalization helpers ---
fn db_error_json(err: &pg::Error) -> serde_json::Value {
    if let Some(db) = err.as_db_error() {
        db_error_json_from_db(db)
    } else {
        json!({"message": err.to_string()})
    }
}

fn db_error_json_from_db(db: &pg::error::DbError) -> serde_json::Value {
    let code = db.code().code().to_string();
    let severity = db.severity().to_string();
    let message = db.message().to_string();
    let detail = db.detail().map(|s| s.to_string());
    let hint = db.hint().map(|s| s.to_string());
    json!({
        "sqlstate": code,
        "severity": severity,
        "message": message,
        "detail": detail,
        "hint": hint,
    })
}

// --- Utility helpers ---
fn extract_ms(s: &str) -> Option<f64> {
    // expects like " 1.234 ms" or "1 ms"
    let trimmed = s.trim();
    let num = trimmed.trim_end_matches("ms").trim();
    num.parse::<f64>().ok()
}

fn join_strs(arr: &Vec<Value>) -> String {
    let mut out: Vec<String> = Vec::new();
    for v in arr {
        if let Some(s) = v.as_str() { out.push(s.to_string()); }
        else { out.push(v.to_string()); }
    }
    out.join(", ")
}

async fn index_suggest(dsn: &str, table: &str, workload: Vec<Value>) -> anyhow::Result<Value> {
    use regex::Regex;
    let (client, conn) = pg::connect(dsn, pg::NoTls).await?;
    tokio::spawn(async move { let _ = conn.await; });
    // Skip very small tables
    let est_rows: f64 = {
        let q = "SELECT reltuples::float8 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = $1 ORDER BY n.nspname LIMIT 1";
        match client.query_opt(q, &[&table]).await? { Some(r) => r.get(0), None => 0.0 }
    };
    if est_rows < 10_000.0 {
        return Ok(json!({"ok": true, "suggestions": [], "reason": "table too small (reltuples < 10k)"}));
    }

    // Very simple heuristic parser: extract columns from WHERE equality and JOIN ON equality
    let re_where = Regex::new(r"(?i)\bwhere\b([^;]+)").ok();
    let re_eq_col = Regex::new(r"(?i)\b([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*\$?\d+|\b([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*'[^']*'").ok();
    let re_join = Regex::new(r"(?i)\bjoin\b[^\n]*\bon\b([^\n]+)").ok();

    use std::collections::BTreeMap;
    let mut col_score: BTreeMap<String, f64> = BTreeMap::new();
    for item in workload.iter() {
        let q = item.get("query").and_then(|v| v.as_str()).unwrap_or("");
        let freq = item.get("freq").and_then(|v| v.as_f64()).unwrap_or(1.0);
        if q.is_empty() { continue; }
        if let Some(re) = &re_where {
            if let Some(cap) = re.captures(q) {
                let where_clause = cap.get(1).map(|m| m.as_str()).unwrap_or("");
                if let Some(eq) = &re_eq_col {
                    for cap2 in eq.captures_iter(where_clause) {
                        if let Some(m) = cap2.get(1).or_else(|| cap2.get(2)) {
                            *col_score.entry(m.as_str().to_string()).or_insert(0.0) += 1.0 * freq;
                        }
                    }
                }
            }
        }
        if let Some(rj) = &re_join {
            for cap in rj.captures_iter(q) {
                let on = cap.get(1).map(|m| m.as_str()).unwrap_or("");
                // naive split by '=' to catch join keys like a.user_id = b.id
                for part in on.split('=') {
                    let token = part.trim();
                    // take trailing identifier
                    if let Some(id) = token.split('.').last() {
                        if id.chars().all(|c| c.is_alphanumeric() || c == '_') {
                            *col_score.entry(id.to_string()).or_insert(0.0) += 0.5 * freq;
                        }
                    }
                }
            }
        }
    }

    // Build suggestions: top-2 columns by score, propose composite index
    let mut scored: Vec<(String, f64)> = col_score.into_iter().collect();
    scored.sort_by(|a,b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let mut suggestions: Vec<Value> = Vec::new();
    if !scored.is_empty() {
        let first = &scored[0].0;
        let ddl = format!("CREATE INDEX CONCURRENTLY IF NOT EXISTS {0}_{1}_idx ON {0} ({1})", table, first);
        suggestions.push(json!({"table": table, "columns": [first], "reason": "frequent equality/join on column", "ddl": ddl}));
        if scored.len() > 1 {
            let second = &scored[1].0;
            let ddl2 = format!("CREATE INDEX CONCURRENTLY IF NOT EXISTS {0}_{1}_{2}_idx ON {0} ({1}, {2})", table, first, second);
            suggestions.push(json!({"table": table, "columns": [first, second], "reason": "composite index based on workload frequency", "ddl": ddl2}));
        }
    }

    Ok(json!({"ok": true, "suggestions": suggestions}))
}
