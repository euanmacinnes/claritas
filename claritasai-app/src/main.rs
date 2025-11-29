use std::{collections::HashSet, net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Result;
use axum::Router;
use tokio::net::TcpListener;
use tokio::process::Command;
use clap::Parser;
use tracing::{error, info, warn, debug};
use tracing_subscriber::EnvFilter;
use tokio::sync::broadcast;

use claritasai_web::{router, WebState};
use claritasai_mcp::ToolRegistry;
use tokio_postgres as pg;
use url::Url;

#[derive(Parser, Debug)]
#[command(name = "claritasai", version)]
struct Cli {
    /// Config YAML path
    #[arg(short, long, default_value = "configs/claritasai.yaml")]
    config: PathBuf,

    /// Bind address
    #[arg(long, default_value = "0.0.0.0:7040")]
    bind: String,

    /// Disable launching local MCP hosts
    #[arg(long, default_value_t = false)]
    no_local_mcp: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let cli = Cli::parse();
    info!(?cli, "starting claritasai");

    // Load YAML config (best-effort)
    let cfg: Option<AppConfig> = match std::fs::read_to_string(&cli.config) {
        Ok(s) => match serde_yaml::from_str(&s) {
            Ok(c) => Some(c),
            Err(e) => {
                error!(error = %e, "failed to parse config YAML; proceeding with defaults");
                None
            }
        },
        Err(e) => {
            error!(error = %e, path = %cli.config.display(), "failed to read config YAML; proceeding with defaults");
            None
        }
    };

    // Basic local MCP supervision (spawn processes) if enabled
    if cli.no_local_mcp {
        info!("local MCP hosts disabled via CLI");
    } else {
        if let Some(cfg) = &cfg {
            if cfg.mcp.startup.launch_local {
                let disabled: HashSet<String> = cfg
                    .mcp
                    .startup
                    .disabled
                    .iter()
                    .map(|s| s.to_lowercase())
                    .collect();
                // Create a shared event channel for streaming to the web UI
                let (evt_tx, _evt_rx) = broadcast::channel::<String>(256);

                for server in &cfg.mcp.servers {
                    let id_lc = server.id.to_lowercase();
                    if disabled.contains(&id_lc) {
                        info!(id = %server.id, "MCP host disabled by config");
                        continue;
                    }
                    if server.endpoint.as_deref().unwrap_or("auto").eq_ignore_ascii_case("auto") {
                        // supervise each local host in its own task
                        let server_cloned = server.clone();
                        let tx_clone = evt_tx.clone();
                        tokio::spawn(async move {
                            supervise_local_mcp(server_cloned, Some(tx_clone)).await;
                        });
                    } else {
                        info!(id = %server.id, endpoint = ?server.endpoint, "using remote MCP endpoint; not spawning local host");
                    }
                }
            } else {
                info!("config disables launching local MCP hosts");
            }
        } else {
            info!("no config loaded; skipping MCP host spawn");
        }
    }

    // Bootstrap storage (Clarium on Postgres) if configured
    if let Some(cfg) = &cfg {
        if let Some(storage) = &cfg.storage {
            if storage.backend.eq_ignore_ascii_case("clarium") {
                if let Some(cl) = &storage.clarium {
                    if let Some(dsn) = &cl.dsn {
                        if let Err(e) = ensure_db_and_migrate(dsn).await {
                            error!(error=%e, "database bootstrap failed");
                        }
                    }
                }
            }
        }
    }

    // Web server
    // Build initial WebState from config when available
    // Attach an event channel for SSE
    let (event_tx, _event_rx) = broadcast::channel::<String>(256);
    let mut ws = build_web_state_from_config(cfg.as_ref());
    ws.event_tx = Some(event_tx.clone());
    // Initialize a simple ToolRegistry from config
    let registry = build_tool_registry_from_config(cfg.as_ref());
    ws.tool_registry = Some(registry);
    let state = Arc::new(ws);
    let app: Router = router(state);
    let addr: SocketAddr = cli
        .bind
        .parse()
        .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 7040)));
    let listener = TcpListener::bind(addr).await?;
    info!(addr = %listener.local_addr()?, "listening");
    if let Err(e) = axum::serve(listener, app).await {
        error!(error = %e, "server error");
    }
    Ok(())
}

// ---------------- Config ----------------

#[derive(Debug, Clone, serde::Deserialize)]
struct AppConfig {
    #[allow(dead_code)]
    version: Option<u32>,
    #[allow(dead_code)]
    server: Option<ServerConfig>,
    #[allow(dead_code)]
    workspace: Option<WorkspaceConfig>,
    mcp: McpConfig,
    #[allow(dead_code)]
    storage: Option<StorageConfig>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ServerConfig {
    #[allow(dead_code)]
    bind: Option<String>,
    #[allow(dead_code)]
    port: Option<u16>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct StorageConfig {
    backend: String,
    clarium: Option<ClariumStorage>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ClariumStorage {
    dsn: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct WorkspaceConfig {
    #[allow(dead_code)]
    python: Option<PythonWorkspace>,
    #[allow(dead_code)]
    clarium: Option<ClariumWorkspace>,
    #[allow(dead_code)]
    rust: Option<RustWorkspace>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct PythonWorkspace {
    #[allow(dead_code)]
    root: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ClariumWorkspace {
    #[allow(dead_code)]
    spec_root: Option<String>,
    #[allow(dead_code)]
    output_dir: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct RustWorkspace {
    #[allow(dead_code)]
    root: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct McpConfig {
    startup: McpStartup,
    #[allow(dead_code)]
    transports: Option<McpTransports>,
    servers: Vec<McpServer>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct McpStartup {
    #[serde(default = "default_true")]
    launch_local: bool,
    #[serde(default)]
    disabled: Vec<String>,
}

fn default_true() -> bool { true }

#[derive(Debug, Clone, serde::Deserialize)]
struct McpTransports {
    #[allow(dead_code)]
    mode: Option<String>,
    #[allow(dead_code)]
    http_bind: Option<String>,
    #[allow(dead_code)]
    http_base_port: Option<u16>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct McpServer {
    id: String,
    #[serde(default)]
    endpoint: Option<String>,
    command: String,
    #[serde(default)]
    args: Vec<String>,
}

fn build_web_state_from_config(cfg: Option<&AppConfig>) -> WebState {
    let mut state = WebState::default();
    if let Some(cfg) = cfg {
        // pick python workspace root if present
        if let Some(ws) = &cfg.workspace {
            if let Some(py) = &ws.python {
                if let Some(root) = &py.root {
                    state.python_root = Some(expand_env_vars(root));
                }
            }
            if let Some(rs) = &ws.rust {
                if let Some(root) = &rs.root {
                    state.rust_root = Some(expand_env_vars(root));
                }
            }
        }
        // set DB DSN if configured
        if let Some(storage) = &cfg.storage {
            if storage.backend.eq_ignore_ascii_case("clarium") {
                if let Some(cl) = &storage.clarium {
                    if let Some(dsn) = &cl.dsn {
                        state.db_dsn = Some(expand_env_vars(dsn));
                    }
                }
            }
        }
        // choose python MCP command from server list if present
        for s in &cfg.mcp.servers {
            if s.id.eq_ignore_ascii_case("python") {
                state.mcp_python_cmd = s.command.clone();
            } else if s.id.eq_ignore_ascii_case("rust") {
                state.mcp_rust_cmd = s.command.clone();
            } else if s.id.eq_ignore_ascii_case("clarium") {
                state.mcp_clarium_cmd = s.command.clone();
                // expand env vars in args for convenience
                state.mcp_clarium_args = s.args.iter().map(|a| expand_env_vars(a)).collect();
            }
        }
    }
    state
}

fn expand_env_vars(input: &str) -> String {
    // very small ${VAR} expander for config convenience
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            // find closing '}'
            if let Some(end) = input[i + 2..].find('}') {
                let var = &input[i + 2..i + 2 + end];
                let val = std::env::var(var).unwrap_or_default();
                out.push_str(&val);
                i += 2 + end + 1; // skip ${VAR}
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

// ---------------- MCP Supervision ----------------

async fn supervise_local_mcp(server: McpServer, events: Option<broadcast::Sender<String>>) {
    let id = server.id.clone();
    let cmd_name = server.command.clone();
    let args = server.args.clone();
    let mut backoff_ms: u64 = 500; // start with 0.5s
    let max_backoff_ms: u64 = 30_000; // cap at 30s

    loop {
        info!(id = %id, command = %cmd_name, args = ?args, "starting local MCP host");
        let mut cmd = Command::new(&cmd_name);
        cmd.args(&args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        match cmd.spawn() {
            Ok(mut child) => {
                // Pipe stdout
                if let Some(stdout) = child.stdout.take() {
                    let id_out = id.clone();
                    let tx = events.clone();
                    tokio::spawn(async move {
                        use tokio::io::{AsyncBufReadExt, BufReader};
                        let mut reader = BufReader::new(stdout);
                        let mut line = String::new();
                        loop {
                            line.clear();
                            match reader.read_line(&mut line).await {
                                Ok(0) => break,
                                Ok(_) => {
                                    let trimmed = line.trim_end();
                                    if !trimmed.is_empty() {
                                        debug!(id = %id_out, "MCP stdout: {}", trimmed);
                                        if let Some(tx) = &tx { let _ = tx.send(format!("[{}][stdout] {}", id_out, trimmed)); }
                                    }
                                }
                                Err(e) => {
                                    warn!(id = %id_out, error = %e, "error reading MCP stdout");
                                    break;
                                }
                            }
                        }
                    });
                }
                // Pipe stderr
                if let Some(stderr) = child.stderr.take() {
                    let id_err = id.clone();
                    let tx = events.clone();
                    tokio::spawn(async move {
                        use tokio::io::{AsyncBufReadExt, BufReader};
                        let mut reader = BufReader::new(stderr);
                        let mut line = String::new();
                        loop {
                            line.clear();
                            match reader.read_line(&mut line).await {
                                Ok(0) => break,
                                Ok(_) => {
                                    let trimmed = line.trim_end();
                                    if !trimmed.is_empty() {
                                        warn!(id = %id_err, "MCP stderr: {}", trimmed);
                                        if let Some(tx) = &tx { let _ = tx.send(format!("[{}][stderr] {}", id_err, trimmed)); }
                                    }
                                }
                                Err(e) => {
                                    warn!(id = %id_err, error = %e, "error reading MCP stderr");
                                    break;
                                }
                            }
                        }
                    });
                }

                // Wait for process to exit
                match child.wait().await {
                    Ok(status) => {
                        warn!(id = %id, exit = %status, "MCP host exited");
                    }
                    Err(e) => {
                        error!(id = %id, error = %e, "failed waiting for MCP host");
                    }
                }
            }
            Err(e) => {
                error!(id = %id, error = %e, "failed to spawn MCP host");
            }
        }

        // Backoff before restart
        warn!(id = %id, backoff_ms, "restarting MCP host after backoff");
        if let Some(tx) = &events { let _ = tx.send(format!("[{}] restarting after backoff {}ms", id, backoff_ms)); }
        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(max_backoff_ms);
    }
}

// Build a simple ToolRegistry from config server list
fn build_tool_registry_from_config(cfg: Option<&AppConfig>) -> ToolRegistry {
    let reg = ToolRegistry::new();
    if let Some(cfg) = cfg {
        for s in &cfg.mcp.servers {
            // Expand env vars in args for convenience
            let args: Vec<String> = s.args.iter().map(|a| expand_env_vars(a)).collect();
            let id_lc = s.id.to_lowercase();
            let prefixes: Vec<String> = match id_lc.as_str() {
                "python" => vec!["python.".into(), "code.".into(), "fs.".into(), "deploy.".into(), "meta.".into()],
                "rust" => vec!["rust.".into(), "code.".into(), "fs.".into()],
                "clarium" => vec!["clarium.".into(), "db.".into()],
                _ => vec![format!("{}.", s.id)],
            };
            // Fire-and-forget: registry is async
            let command = s.command.clone();
            let id = s.id.clone();
            let reg_clone = reg.clone();
            tokio::spawn(async move {
                reg_clone.register_server(id, command, args, prefixes).await;
            });
        }
    }
    reg
}

// ---------------- Storage (Postgres) ----------------

async fn ensure_db_and_migrate(target_dsn: &str) -> anyhow::Result<()> {
    // Try to connect to the target DB directly
    if let Ok((_client, conn)) = pg::connect(target_dsn, pg::NoTls).await {
        // spawn connection driver
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::error!(error=%e, "postgres connection error");
            }
        });
        run_migrations(target_dsn).await?;
        return Ok(());
    }

    // Parse DSN to extract db name and construct admin DSN pointing to 'postgres'
    let url = Url::parse(target_dsn)?;
    let db_name = url.path().trim_start_matches('/').to_string();
    let mut admin_url = url.clone();
    admin_url.set_path("/postgres");

    // Connect to admin DB
    let (admin_client, admin_conn) = pg::connect(admin_url.as_str(), pg::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = admin_conn.await {
            tracing::error!(error=%e, "postgres admin connection error");
        }
    });

    // Try to create the database if it doesn't exist
    // Note: CREATE DATABASE IF NOT EXISTS is not supported in Postgres; use conditional check
    let check_exists = "SELECT 1 FROM pg_database WHERE datname = $1";
    let exists = admin_client
        .query(check_exists, &[&db_name])
        .await
        .map(|rows| !rows.is_empty())
        .unwrap_or(false);
    if !exists {
        let stmt = format!("CREATE DATABASE \"{}\"", db_name.replace('"', ""));
        let _ = admin_client.batch_execute(&stmt).await;
    }

    // Connect to target and run migrations
    run_migrations(target_dsn).await?;
    Ok(())
}

async fn run_migrations(dsn: &str) -> anyhow::Result<()> {
    let (client, conn) = pg::connect(dsn, pg::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!(error=%e, "postgres connection error");
        }
    });

    // Minimal schema to get started
    let stmts = r#"
        CREATE TABLE IF NOT EXISTS runs (
            id SERIAL PRIMARY KEY,
            started_at TIMESTAMPTZ DEFAULT now(),
            agent_profile TEXT,
            status TEXT
        );
        CREATE TABLE IF NOT EXISTS plans (
            id SERIAL PRIMARY KEY,
            run_id INT REFERENCES runs(id) ON DELETE CASCADE,
            objective TEXT,
            json JSONB,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS plan_verdicts (
            id SERIAL PRIMARY KEY,
            plan_id INT REFERENCES plans(id) ON DELETE CASCADE,
            status TEXT,
            rationale TEXT,
            json JSONB,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS executions (
            id SERIAL PRIMARY KEY,
            plan_id INT REFERENCES plans(id) ON DELETE CASCADE,
            started_at TIMESTAMPTZ DEFAULT now(),
            finished_at TIMESTAMPTZ,
            status TEXT
        );
        CREATE TABLE IF NOT EXISTS steps (
            id SERIAL PRIMARY KEY,
            execution_id INT REFERENCES executions(id) ON DELETE CASCADE,
            idx INT,
            tool_ref TEXT,
            input_json JSONB,
            output_json JSONB,
            status TEXT,
            started_at TIMESTAMPTZ DEFAULT now(),
            finished_at TIMESTAMPTZ
        );
        CREATE TABLE IF NOT EXISTS metrics (
            id SERIAL PRIMARY KEY,
            run_id INT REFERENCES runs(id) ON DELETE CASCADE,
            key TEXT,
            value_num DOUBLE PRECISION,
            value_text TEXT,
            source TEXT,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS global_memories (
            id SERIAL PRIMARY KEY,
            kind TEXT,
            key TEXT,
            content TEXT,
            tags TEXT,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS project_memories (
            id SERIAL PRIMARY KEY,
            project_key TEXT,
            kind TEXT,
            key TEXT,
            content TEXT,
            tags TEXT,
            created_at TIMESTAMPTZ DEFAULT now()
        );
    "#;
    client.batch_execute(stmts).await?;
    Ok(())
}
