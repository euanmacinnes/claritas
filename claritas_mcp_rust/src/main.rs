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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let cli = Cli::parse();

    if cli.ping {
        println!("{}", json!({"ok": true, "name": "claritas_mcp_rust"}));
        return Ok(());
    }
    if cli.capabilities {
        println!(
            "{}",
            json!({
                "tools": [
                    "rust.cargo.build.debug","rust.cargo.build.release","rust.cargo.test","rust.cargo.check",
                    "rust.clippy","rust.fmt","rust.doc","rust.coverage.report","rust.workspace.graph",
                    "code.edit","code.refactor.rename","fs.read","fs.write","fs.list",
                    "meta.ping","meta.capabilities"
                ]
            })
        );
        return Ok(());
    }

    info!("claritas_mcp_rust started (stub)");
    tokio::signal::ctrl_c().await?;
    Ok(())
}
