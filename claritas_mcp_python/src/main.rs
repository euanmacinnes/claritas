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
        println!("{}", json!({"ok": true, "name": "claritas_mcp_python"}));
        return Ok(());
    }
    if cli.capabilities {
        println!(
            "{}",
            json!({
                "tools": [
                    "python.build.debug","python.build.test","python.build.release",
                    "python.test.unit","python.test.integration","python.coverage.report",
                    "python.lint","python.format","python.typecheck","python.warn","python.sec.check",
                    "code.generate","code.edit","code.refactor.rename","code.refactor.extract","code.refactor.move",
                    "fs.read","fs.write","fs.list","fs.delete","fs.move","fs.mkdir",
                    "deploy.package","deploy.publish","deploy.run","deploy.k8s.apply",
                    "meta.ping","meta.capabilities"
                ]
            })
        );
        return Ok(());
    }

    // TODO: JSON-RPC loop over stdio. For now, just log and stay idle.
    info!("claritas_mcp_python started (stub)");
    tokio::signal::ctrl_c().await?;
    Ok(())
}
