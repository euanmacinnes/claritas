use std::{collections::HashSet, net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Result;
use axum::Router;
use tokio::net::TcpListener;
use tokio::process::Command;
use clap::Parser;
use tracing::{error, info, warn, debug};
use tracing_subscriber::EnvFilter;
use tokio::sync::broadcast;
use tokio::time::{timeout, Duration};
use reqwest::Client as HttpClient;

use claritasai_web::{router, WebState};
use claritasai_agents::AgentsHarness;
use claritasai_mcp::ToolRegistry;
use claritasai_notify::{NotifierHub, Provider as NotifyProvider, TelegramConfig as NotifyTelegramCfg, EmailConfig as NotifyEmailCfg, WhatsAppConfig as NotifyWhatsAppCfg};
use claritasai_runtime::{Orchestrator, OrchestratorConfig};
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

    /// Validate config (DB + LLM/models) and exit
    #[arg(long, default_value_t = false)]
    validate_config: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI first so we can control output style for validate-only runs
    let cli = Cli::parse();

    // For normal runs, initialize tracing. For --validate-config, we intentionally skip
    // tracing init so the validator can "just print" plain lines without log prefixes.
    if !cli.validate_config {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_target(false)
            .init();
        info!(?cli, "starting claritasai");
    }

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

    // If requested, perform startup configuration validation and exit
    if cli.validate_config {
        match cfg.as_ref() {
            Some(c) => {
                // Always let the validator print its own summary, then exit with proper code.
                match validate_startup(c).await {
                    Ok(_) => {
                        std::process::exit(0);
                    }
                    Err(_e) => {
                        // Summary has already been printed inside validate_startup.
                        std::process::exit(1);
                    }
                }
            }
            None => {
                // Print a minimal message and exit with distinct code
                println!("[ERR] no config loaded; cannot validate");
                println!("overall: FAIL (0 ok, 1 errors)");
                std::process::exit(2);
            }
        }
    }

    // Shared event channel for SSE across app (MCP supervision + Web UI)
    let (event_tx, _event_rx) = broadcast::channel::<String>(256);

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

                for server in &cfg.mcp.servers {
                    let id_lc = server.id.to_lowercase();
                    if disabled.contains(&id_lc) {
                        info!(id = %server.id, "MCP host disabled by config");
                        continue;
                    }
                    if server.endpoint.as_deref().unwrap_or("auto").eq_ignore_ascii_case("auto") {
                        // supervise each local host in its own task
                        let server_cloned = server.clone();
                        let tx_clone = event_tx.clone();
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
                        // Validate DSN early to produce clearer diagnostics than a generic "invalid configuration"
                        match validate_dsn(dsn) {
                            Ok(_) => {
                                if let Err(e) = ensure_db_and_migrate(dsn).await {
                                    error!(error=%e, "database bootstrap failed");
                                }
                            }
                            Err(e) => {
                                error!(error = %e, dsn=?mask_dsn(dsn), "invalid database configuration (DSN)");
                            }
                        }
                    }
                }
            }
        }
    }

    // Web server
    // Build initial WebState from config when available
    // Attach the shared event channel for SSE
    let mut ws = build_web_state_from_config(cfg.as_ref());
    ws.event_tx = Some(event_tx.clone());
    // Initialize a simple ToolRegistry from config
    let registry = build_tool_registry_from_config(cfg.as_ref());
    ws.tool_registry = Some(registry);
    // Initialize Orchestrator with event_tx and configured timeouts
    let orch_cfg = OrchestratorConfig { step_timeout_ms: ws.step_timeout_ms.unwrap_or(60_000) };
    let orchestrator = Orchestrator::new(orch_cfg, Some(event_tx.clone()));
    let orchestrator = Arc::new(tokio::sync::Mutex::new(orchestrator));
    ws.orchestrator = Some(orchestrator);

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
    #[allow(dead_code)]
    notify: Option<NotifyConfig>,
    #[allow(dead_code)]
    agents: Option<AgentsConfig>,
    #[allow(dead_code)]
    executor: Option<ExecutorConfig>,
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
struct AgentsConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    ollama: Option<OllamaConfig>,
    /// Max Dev↔QA refinement attempts
    #[serde(default)]
    max_attempts: Option<usize>,
    #[serde(default)]
    planner: Option<AgentRoleConfig>,
    #[serde(default)]
    verifier: Option<AgentRoleConfig>,
    #[serde(default)]
    executor: Option<AgentRoleConfig>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct OllamaConfig {
    #[serde(default)] url: String,
    #[serde(default)] model: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct AgentRoleConfig {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    tools: Option<Vec<String>>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct NotifyConfig {
    #[serde(default)]
    telegram: Option<NotifyTelegramBlock>,
    #[serde(default)]
    email: Option<NotifyEmailBlock>,
    #[serde(default)]
    whatsapp: Option<NotifyWhatsAppBlock>,
    #[serde(default)]
    max_retries: Option<usize>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct NotifyTelegramBlock {
    #[serde(default)] bot_token: String,
    #[serde(default)] chat_id: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct NotifyEmailBlock {
    #[serde(default)] smtp_url: String,
    #[serde(default)] from: String,
    #[serde(default)] to: Vec<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct NotifyWhatsAppBlock {
    #[serde(default)] api_url: String,
    #[serde(default)] token: String,
    #[serde(default)] to: Vec<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ExecutorConfig {
    #[serde(default)]
    partial_chunk_size: Option<usize>,
    #[serde(default)]
    partial_max_chunks: Option<usize>,
    #[serde(default)]
    step_timeout_ms: Option<u64>,
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
    // Set sane defaults for streaming
    state.partial_chunk_size = 1024;
    state.partial_max_chunks = 3;
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
        // Build NotifierHub from config if present
        if let Some(nc) = &cfg.notify {
            let mut hub = NotifierHub::new();
            if let Some(maxr) = nc.max_retries { hub.max_retries = maxr; }
            if let Some(tg) = &nc.telegram {
                if !tg.bot_token.is_empty() && !tg.chat_id.is_empty() {
                    let cfg = NotifyTelegramCfg { bot_token: expand_env_vars(&tg.bot_token), chat_id: expand_env_vars(&tg.chat_id) };
                    hub = hub.with_provider(NotifyProvider::Telegram(cfg));
                }
            }
            if let Some(em) = &nc.email {
                if !em.smtp_url.is_empty() && !em.from.is_empty() && !em.to.is_empty() {
                    let cfg = NotifyEmailCfg { smtp_url: expand_env_vars(&em.smtp_url), from: em.from.clone(), to: em.to.clone() };
                    hub = hub.with_provider(NotifyProvider::Email(cfg));
                }
            }
            if let Some(wa) = &nc.whatsapp {
                if !wa.api_url.is_empty() && !wa.token.is_empty() && !wa.to.is_empty() {
                    let cfg = NotifyWhatsAppCfg { api_url: expand_env_vars(&wa.api_url), token: expand_env_vars(&wa.token), to: wa.to.clone() };
                    hub = hub.with_provider(NotifyProvider::WhatsApp(cfg));
                }
            }
            state.notifier_hub = Some(Arc::new(hub));
        }

        // Build AgentsHarness (Ollama) if enabled
        if let Some(ag) = &cfg.agents {
            if ag.enabled {
                if let Some(ol) = &ag.ollama {
                    if !ol.url.is_empty() && !ol.model.is_empty() {
                        state.agents = Some(AgentsHarness::new(expand_env_vars(&ol.url), expand_env_vars(&ol.model)));
                    }
                }
                // Apply max attempts if provided
                if let Some(maxa) = ag.max_attempts { state.agents_max_attempts = Some(maxa); }
            }
        }

        // Executor streaming/timeouts
        if let Some(exec) = &cfg.executor {
            if let Some(sz) = exec.partial_chunk_size { state.partial_chunk_size = sz.max(128); }
            if let Some(mc) = exec.partial_max_chunks { state.partial_max_chunks = mc.max(1); }
            state.step_timeout_ms = exec.step_timeout_ms;
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
        if let Some(tx) = &events {
            let _ = tx.send(serde_json::json!({
                "event": "host_health",
                "id": id,
                "ok": true,
                "status": "starting"
            }).to_string());
        }
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
                        if let Some(tx) = &events {
                            let _ = tx.send(serde_json::json!({
                                "event": "host_health",
                                "id": id,
                                "ok": false,
                                "status": "exited",
                                "message": format!("exit status: {}", status)
                            }).to_string());
                        }
                    }
                    Err(e) => {
                        error!(id = %id, error = %e, "failed waiting for MCP host");
                        if let Some(tx) = &events {
                            let _ = tx.send(serde_json::json!({
                                "event": "host_health",
                                "id": id,
                                "ok": false,
                                "status": "error",
                                "message": e.to_string()
                            }).to_string());
                        }
                    }
                }
            }
            Err(e) => {
                error!(id = %id, error = %e, "failed to spawn MCP host");
                if let Some(tx) = &events {
                    let _ = tx.send(serde_json::json!({
                        "event": "host_health",
                        "id": id,
                        "ok": false,
                        "status": "spawn_failed",
                        "message": e.to_string()
                    }).to_string());
                }
            }
        }

        // Backoff before restart
        warn!(id = %id, backoff_ms, "restarting MCP host after backoff");
        if let Some(tx) = &events {
            let _ = tx.send(serde_json::json!({
                "event": "host_health",
                "id": id,
                "ok": false,
                "status": "restarting",
                "backoff_ms": backoff_ms
            }).to_string());
        }
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

/// Very small DSN validator to provide clearer errors before attempting a connection.
fn validate_dsn(dsn: &str) -> anyhow::Result<()> {
    let url = Url::parse(dsn)?;
    let scheme = url.scheme();
    if scheme != "postgres" && scheme != "postgresql" {
        anyhow::bail!("unsupported scheme '{}', expected 'postgres' or 'postgresql'", scheme);
    }
    // database name must be present in URL path
    let db_name = url.path().trim_start_matches('/');
    if db_name.is_empty() {
        anyhow::bail!("missing database name in DSN path");
    }
    Ok(())
}

/// Mask credentials in DSN for logs.
fn mask_dsn(dsn: &str) -> String {
    if let Ok(url) = Url::parse(dsn) {
        let mut masked = url.clone();
        if masked.username().len() > 0 || masked.password().is_some() {
            // Replace userinfo with ****
            let host = masked.host_str().unwrap_or("").to_string();
            let port = masked.port().map(|p| format!(":{}", p)).unwrap_or_default();
            let path = masked.path().to_string();
            let mut out = format!("{}://****:****@{}{}{}", masked.scheme(), host, port, path);
            if let Some(q) = masked.query() { out.push_str(&format!("?{}", q)); }
            return out;
        }
        return dsn.to_string();
    }
    dsn.to_string()
}

async fn ensure_db_and_migrate(target_dsn: &str) -> anyhow::Result<()> {
    // First, parse and validate the DSN to produce actionable errors early.
    let url = Url::parse(target_dsn)?;
    let db_name = url.path().trim_start_matches('/').to_string();

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

    // Construct admin DSN pointing to 'postgres'
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

// ---------------- Startup Validation ----------------

async fn validate_startup(cfg: &AppConfig) -> anyhow::Result<()> {
    let mut errors: Vec<String> = Vec::new();
    let mut oks: Vec<String> = Vec::new();

    // DB connectivity check (when storage configured for clarium)
    if let Some(storage) = &cfg.storage {
        if storage.backend.eq_ignore_ascii_case("clarium") {
            if let Some(cl) = &storage.clarium {
                if let Some(dsn_raw) = &cl.dsn {
                    let dsn = expand_env_vars(dsn_raw);
                    if let Err(e) = validate_dsn(&dsn) {
                        errors.push(format!("DB DSN invalid: {} ({})", mask_dsn(&dsn), e));
                    } else if let Err(e) = check_db_connectivity(&dsn).await {
                        errors.push(format!("DB connection failed: {} ({})", mask_dsn(&dsn), e));
                    } else {
                        let msg = format!("database connection OK: {}", mask_dsn(&dsn));
                        oks.push(msg);

                        // Since DB is reachable, ensure DB exists and run migrations/setup now
                        match ensure_db_and_migrate(&dsn).await {
                            Ok(_) => {
                                oks.push("database setup/migrations OK".to_string());
                                // After migrations, verify expected tables/columns exist
                                match check_db_schema(&dsn).await {
                                    Ok((schema_oks, schema_errs)) => {
                                        oks.extend(schema_oks);
                                        errors.extend(schema_errs);
                                    }
                                    Err(se) => {
                                        errors.push(format!("database schema check failed: {}", se));
                                    }
                                }
                            }
                            Err(me) => {
                                errors.push(format!("database setup/migrations failed: {}", me));
                            }
                        }
                    }
                }
            }
        }
    }

    // LLM (Ollama) availability and models
    let (ollama_url_opt, required_ollama_models) = collect_ollama_targets(cfg);
    if let Some(ollama_url) = ollama_url_opt {
        match check_ollama(&ollama_url, &required_ollama_models).await {
            Ok(_) => {
                if !required_ollama_models.is_empty() {
                    let msg = format!(
                        "ollama OK: {} (models available: {})",
                        ollama_url,
                        required_ollama_models.iter().cloned().collect::<Vec<_>>().join(", ")
                    );
                    info!("{}", msg);
                    oks.push(msg);
                } else {
                    let msg = format!("ollama reachable: {}", ollama_url);
                    info!("{}", msg);
                    oks.push(msg);
                }
            }
            Err(e) => {
                // If the error indicates missing models, try to pull them, then re-check once
                let emsg = e.to_string();
                const PREFIX: &str = "missing ollama models:";
                if let Some(idx) = emsg.to_lowercase().find(PREFIX) {
                    // extract the CSV list that follows the prefix
                    let list_str = emsg[(idx + PREFIX.len())..].trim();
                    let missing: Vec<String> = list_str
                        .split(',')
                        .map(|s| s.trim().trim_matches('"').to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                    if !missing.is_empty() {
                        info!(url=%ollama_url, models=?missing, "attempting to pull missing Ollama models");
                        match pull_ollama_models(&ollama_url, &missing).await {
                            Ok(_) => {
                                oks.push(format!("ollama: pulled missing models at {}: {}", ollama_url, missing.join(", ")));
                                // Re-check once after successful pulls
                                match check_ollama(&ollama_url, &required_ollama_models).await {
                                    Ok(_) => {
                                        let msg = if required_ollama_models.is_empty() {
                                            format!("ollama OK after pull: {}", ollama_url)
                                        } else {
                                            format!(
                                                "ollama OK after pull: {} (models available: {})",
                                                ollama_url,
                                                required_ollama_models.iter().cloned().collect::<Vec<_>>().join(", ")
                                            )
                                        };
                                        info!("{}", msg);
                                        oks.push(msg);
                                    }
                                    Err(e2) => {
                                        errors.push(format!("Ollama check failed at {} after pull: {}", ollama_url, e2));
                                    }
                                }
                            }
                            Err(pe) => {
                                errors.push(format!("Ollama model pull failed at {}: {}", ollama_url, pe));
                            }
                        }
                    } else {
                        errors.push(format!("Ollama check failed at {}: {}", ollama_url, e));
                    }
                } else {
                    errors.push(format!("Ollama check failed at {}: {}", ollama_url, e));
                }
            }
        }
    }

    // Always print a concise summary so users see both successes and failures
    println!("config validation summary:");
    for ok in &oks {
        println!("  [OK] {}", ok);
    }
    for err in &errors {
        println!("  [ERR] {}", err);
    }

    if errors.is_empty() {
        println!("overall: PASS ({} checks ok)", oks.len());
        Ok(())
    } else {
        println!("overall: FAIL ({} ok, {} errors)", oks.len(), errors.len());
        let msg = errors.join("; ");
        anyhow::bail!(msg)
    }
}

/// Verify that expected tables and columns exist after migrations.
async fn check_db_schema(dsn: &str) -> anyhow::Result<(Vec<String>, Vec<String>)> {
    let mut oks = Vec::new();
    let mut errs = Vec::new();

    let (client, conn) = pg::connect(dsn, pg::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            debug!(error=%e, "postgres connection driver ended during schema check");
        }
    });

    use std::collections::{HashMap, HashSet};
    let mut expected: HashMap<&str, HashSet<&str>> = HashMap::new();
    expected.insert("runs", HashSet::from([
        "id","started_at","agent_profile","status"
    ]));
    expected.insert("plans", HashSet::from([
        "id","run_id","objective","json","created_at"
    ]));
    expected.insert("plan_verdicts", HashSet::from([
        "id","plan_id","status","rationale","json","created_at"
    ]));
    expected.insert("executions", HashSet::from([
        "id","plan_id","started_at","finished_at","status"
    ]));
    expected.insert("steps", HashSet::from([
        "id","execution_id","idx","tool_ref","input_json","output_json","status","started_at","finished_at","stdout_snip","stderr_snip"
    ]));
    expected.insert("metrics", HashSet::from([
        "id","run_id","key","value_num","value_text","source","created_at"
    ]));
    expected.insert("global_memories", HashSet::from([
        "id","kind","key","content","tags","created_at"
    ]));
    expected.insert("project_memories", HashSet::from([
        "id","project_key","kind","key","content","tags","created_at"
    ]));
    expected.insert("artifacts", HashSet::from([
        "id","run_id","step_id","kind","path","mime","size_bytes","checksum","created_at"
    ]));
    expected.insert("step_attachments", HashSet::from([
        "id","step_id","name","content_json","content_text","mime","size_bytes","created_at"
    ]));

    // Fetch existing tables in public schema
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            &[],
        )
        .await?;
    let existing_tables: HashSet<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

    for (table, columns) in expected.iter() {
        if !existing_tables.contains(&table.to_string()) {
            errs.push(format!("missing table: {}", table));
            continue;
        }
        // For present table, check columns
        let rows = client
            .query(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name = $1",
                &[&table],
            )
            .await?;
        let have: HashSet<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        let mut missing_cols: Vec<&str> = Vec::new();
        for col in columns {
            if !have.contains(&col.to_string()) { missing_cols.push(col); }
        }
        if missing_cols.is_empty() {
            oks.push(format!("table '{}' columns OK", table));
        } else {
            errs.push(format!(
                "table '{}' missing columns: {}",
                table,
                missing_cols.join(", ")
            ));
        }
    }

    Ok((oks, errs))
}

async fn check_db_connectivity(dsn: &str) -> anyhow::Result<()> {
    // Try connect with a short timeout and run a trivial query
    let fut = pg::connect(dsn, pg::NoTls);
    let (client, conn) = timeout(Duration::from_secs(5), fut)
        .await
        .map_err(|_| anyhow::anyhow!("timeout connecting to database"))??;
    // Drive connection in background
    tokio::spawn(async move {
        if let Err(e) = conn.await { tracing::debug!(error=%e, "postgres connection driver ended"); }
    });
    // Simple health query with timeout
    timeout(Duration::from_secs(3), client.simple_query("SELECT 1"))
        .await
        .map_err(|_| anyhow::anyhow!("timeout running health query"))??;
    Ok(())
}

fn collect_ollama_targets(cfg: &AppConfig) -> (Option<String>, std::collections::HashSet<String>) {
    let mut url_opt: Option<String> = None;
    let mut models: std::collections::HashSet<String> = std::collections::HashSet::new();
    if let Some(agents) = &cfg.agents {
        if let Some(ol) = &agents.ollama {
            if !ol.url.is_empty() { url_opt = Some(expand_env_vars(&ol.url)); }
            if !ol.model.is_empty() { models.insert(expand_env_vars(&ol.model)); }
        }
        // role models
        let push_model = |m: &Option<String>, set: &mut std::collections::HashSet<String>| {
            if let Some(v) = m {
                let mv = v.trim();
                if mv.eq_ignore_ascii_case("none") || mv.is_empty() { return; }
                // Only collect ollama-backed models here
                if let Some(rest) = mv.strip_prefix("mcp:ollama:") {
                    set.insert(rest.to_string());
                }
            }
        };
        if let Some(role) = &agents.planner { push_model(&role.model, &mut models); }
        if let Some(role) = &agents.verifier { push_model(&role.model, &mut models); }
        if let Some(role) = &agents.executor { push_model(&role.model, &mut models); }
    }
    (url_opt, models)
}

#[derive(Debug, serde::Deserialize)]
struct OllamaTagsResponse { #[serde(default)] models: Vec<OllamaModelTag> }
#[derive(Debug, serde::Deserialize)]
struct OllamaModelTag { #[serde(default)] name: String, #[serde(default)] model: String }

async fn check_ollama(base_url: &str, required: &std::collections::HashSet<String>) -> anyhow::Result<()> {
    let http = HttpClient::builder().timeout(Duration::from_secs(5)).build()?;
    // Check server is up
    let version_url = format!("{}/api/version", base_url.trim_end_matches('/'));
    let _version = http.get(&version_url).send().await?.error_for_status()?;

    if required.is_empty() { return Ok(()); }

    // List models
    let tags_url = format!("{}/api/tags", base_url.trim_end_matches('/'));
    let tags: OllamaTagsResponse = http.get(&tags_url).send().await?.error_for_status()?.json().await?;
    let available: std::collections::HashSet<String> = tags.models.into_iter().map(|m| if !m.name.is_empty() { m.name } else { m.model }).collect();

    // normalize helper: both with and without tag
    let mut missing: Vec<String> = Vec::new();
    'outer: for req in required {
        let req_full = req;
        let req_base = req_full.split(':').next().unwrap_or(req_full);
        for have in &available {
            if have == req_full { continue 'outer; }
            let have_base = have.split(':').next().unwrap_or(have);
            if have_base == req_base { continue 'outer; }
        }
        missing.push(req_full.clone());
    }
    if missing.is_empty() { Ok(()) } else { anyhow::bail!(format!("missing ollama models: {}", missing.join(", "))) }
}

/// Attempt to pull one or more missing models from the Ollama server.
async fn pull_ollama_models(base_url: &str, models: &Vec<String>) -> anyhow::Result<()> {
    if models.is_empty() { return Ok(()); }
    let http = HttpClient::builder().timeout(Duration::from_secs(600)).build()?;
    let pull_url = format!("{}/api/pull", base_url.trim_end_matches('/'));
    for m in models {
        info!(model=%m, url=%base_url, "pulling Ollama model");
        let resp = http
            .post(&pull_url)
            .json(&serde_json::json!({ "name": m, "stream": false }))
            .send()
            .await?;
        if let Err(e) = resp.error_for_status_ref() {
            anyhow::bail!("failed to pull model {}: {}", m, e);
        }
        // Drain body to avoid connection reuse issues; ignore content
        let _ = resp.bytes().await;
        info!(model=%m, url=%base_url, "model pull completed");
    }
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

        -- MVP: persist trimmed stdout/stderr snippets for steps
        ALTER TABLE steps ADD COLUMN IF NOT EXISTS stdout_snip TEXT;
        ALTER TABLE steps ADD COLUMN IF NOT EXISTS stderr_snip TEXT;

        -- Artifacts and attachments for richer step outputs
        CREATE TABLE IF NOT EXISTS artifacts (
          id BIGSERIAL PRIMARY KEY,
          run_id BIGINT REFERENCES runs(id) ON DELETE CASCADE,
          step_id BIGINT REFERENCES steps(id) ON DELETE CASCADE,
          kind TEXT NOT NULL,
          path TEXT,
          mime TEXT,
          size_bytes BIGINT,
          checksum TEXT,
          created_at TIMESTAMPTZ DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS step_attachments (
          id BIGSERIAL PRIMARY KEY,
          step_id BIGINT REFERENCES steps(id) ON DELETE CASCADE,
          name TEXT NOT NULL,
          content_json JSONB,
          content_text TEXT,
          created_at TIMESTAMPTZ DEFAULT now()
        );

        -- Helpful indexes
        CREATE INDEX IF NOT EXISTS idx_metrics_run_key ON metrics(run_id, key);
        CREATE INDEX IF NOT EXISTS idx_steps_execution_idx ON steps(execution_id, idx);
        CREATE INDEX IF NOT EXISTS idx_artifacts_run ON artifacts(run_id);
    "#;
    client.batch_execute(stmts).await?;
    Ok(())
}
