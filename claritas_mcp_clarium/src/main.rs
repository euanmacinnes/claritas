use clap::Parser;
use serde_json::json;
use tracing::info;

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
                    "db.schema.inspect","db.schema.diff","db.index.suggest","db.query.explain",
                    "meta.ping","meta.capabilities"
                ]
            })
        );
        return Ok(());
    }

    info!(dsn = ?cli.dsn, spec = ?cli.spec, "claritas_mcp_clarium started (stub)");
    tokio::signal::ctrl_c().await?;
    Ok(())
}
