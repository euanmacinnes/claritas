use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Result;
use axum::Router;
use tokio::net::TcpListener;
use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use claritasai_web::{router, WebState};

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

    // TODO: load YAML config; for now, only log the path
    info!(config = %cli.config.display(), "config path");

    // TODO: supervisor for local MCP hosts based on config and --no-local-mcp
    if cli.no_local_mcp {
        info!("local MCP hosts disabled via CLI");
    } else {
        info!("local MCP host supervision not yet implemented (stub)");
    }

    // Web server
    let state = Arc::new(WebState::default());
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
