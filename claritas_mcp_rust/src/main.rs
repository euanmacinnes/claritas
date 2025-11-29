use clap::Parser;
use serde_json::{json, Value};
use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, select};
use tracing::{error, info};
use tokio::process::Command;

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

    info!("claritas_mcp_rust starting JSON-RPC (stdio)");
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
                        if let Err(e) = handle_line(&buf, &mut stdout).await { error!(error=%e, "handler error"); }
                    }
                    Err(e) => { error!(error=%e, "stdin read error"); break; }
                }
            }
            _ = tokio::signal::ctrl_c() => { break; }
        }
    }

    Ok(())
}

async fn handle_line(line: &str, stdout: &mut tokio::io::Stdout) -> anyhow::Result<()> {
    let req: Value = match serde_json::from_str(line) { Ok(v) => v, Err(_) => return Ok(()) };
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let params = req.get("params").cloned().unwrap_or(json!({}));

    let result = match method {
        "meta.ping" => json!({"ok": true, "name": "claritas_mcp_rust"}),
        "meta.capabilities" => json!({"tools":[
            "rust.cargo.build.debug","rust.cargo.build.release","rust.cargo.test","rust.cargo.check",
            "rust.clippy","rust.fmt","rust.doc","rust.coverage.report","rust.workspace.graph",
            "code.edit","code.refactor.rename","fs.read","fs.write","fs.list"
        ]}),
        "rust.cargo.build.debug" => {
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or(".");
            // Run `cargo build` in the provided path and capture output
            let mut cmd = Command::new("cargo");
            cmd.arg("build").current_dir(path);
            match tokio::time::timeout(std::time::Duration::from_secs(120), cmd.output()).await {
                Ok(Ok(out)) => {
                    let code = out.status.code().unwrap_or(-1);
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let trim = |s: &str| -> String {
                        const MAX: usize = 4000;
                        if s.len() > MAX { format!("{}…", &s[..MAX]) } else { s.to_string() }
                    };
                    json!({
                        "ok": out.status.success(),
                        "tool": "rust.cargo.build.debug",
                        "exit_code": code,
                        "stdout": trim(&stdout),
                        "stderr": trim(&stderr)
                    })
                }
                Ok(Err(e)) => json!({"ok": false, "error": format!("spawn error: {}", e)}),
                Err(_) => json!({"ok": false, "error": "timeout running cargo build"}),
            }
        }
        "rust.cargo.test" => {
            // Run `cargo test` in the provided path and capture output
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or(".");
            let mut cmd = Command::new("cargo");
            cmd.arg("test").arg("--no-run").current_dir(path);
            match tokio::time::timeout(std::time::Duration::from_secs(300), cmd.output()).await {
                Ok(Ok(out)) => {
                    let code = out.status.code().unwrap_or(-1);
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let trim = |s: &str| -> String {
                        const MAX: usize = 4000;
                        if s.len() > MAX { format!("{}…", &s[..MAX]) } else { s.to_string() }
                    };
                    json!({
                        "ok": out.status.success(),
                        "tool": "rust.cargo.test",
                        "exit_code": code,
                        "stdout": trim(&stdout),
                        "stderr": trim(&stderr)
                    })
                }
                Ok(Err(e)) => json!({"ok": false, "error": format!("spawn error: {}", e)}),
                Err(_) => json!({"ok": false, "error": "timeout running cargo test"}),
            }
        }
        "rust.clippy" => {
            // Run `cargo clippy` in the provided path
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or(".");
            let mut cmd = Command::new("cargo");
            cmd.arg("clippy").arg("--all-targets").arg("--all-features").current_dir(path);
            match tokio::time::timeout(std::time::Duration::from_secs(300), cmd.output()).await {
                Ok(Ok(out)) => {
                    let code = out.status.code().unwrap_or(-1);
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let trim = |s: &str| -> String {
                        const MAX: usize = 8000;
                        if s.len() > MAX { format!("{}…", &s[..MAX]) } else { s.to_string() }
                    };
                    json!({
                        "ok": out.status.success(),
                        "tool": "rust.clippy",
                        "exit_code": code,
                        "stdout": trim(&stdout),
                        "stderr": trim(&stderr)
                    })
                }
                Ok(Err(e)) => json!({"ok": false, "error": format!("spawn error: {}", e)}),
                Err(_) => json!({"ok": false, "error": "timeout running cargo clippy"}),
            }
        }
        "rust.fmt" => {
            // Run `cargo fmt` (check mode configurable)
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or(".");
            let check = params.get("check").and_then(|v| v.as_bool()).unwrap_or(false);
            let mut cmd = Command::new("cargo");
            cmd.arg("fmt").current_dir(path);
            if check { cmd.arg("--").arg("--check"); }
            match tokio::time::timeout(std::time::Duration::from_secs(180), cmd.output()).await {
                Ok(Ok(out)) => {
                    let code = out.status.code().unwrap_or(-1);
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let trim = |s: &str| -> String {
                        const MAX: usize = 8000;
                        if s.len() > MAX { format!("{}…", &s[..MAX]) } else { s.to_string() }
                    };
                    json!({
                        "ok": out.status.success(),
                        "tool": "rust.fmt",
                        "mode": if check { "check" } else { "format" },
                        "exit_code": code,
                        "stdout": trim(&stdout),
                        "stderr": trim(&stderr)
                    })
                }
                Ok(Err(e)) => json!({"ok": false, "error": format!("spawn error: {}", e)}),
                Err(_) => json!({"ok": false, "error": "timeout running cargo fmt"}),
            }
        }
        "rust.cargo.check" => {
            // Run `cargo check` in the provided path
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or(".");
            let mut cmd = Command::new("cargo");
            cmd.arg("check").arg("--all-targets").arg("--all-features").current_dir(path);
            match tokio::time::timeout(std::time::Duration::from_secs(300), cmd.output()).await {
                Ok(Ok(out)) => {
                    let code = out.status.code().unwrap_or(-1);
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let trim = |s: &str| -> String {
                        const MAX: usize = 8000;
                        if s.len() > MAX { format!("{}…", &s[..MAX]) } else { s.to_string() }
                    };
                    json!({
                        "ok": out.status.success(),
                        "tool": "rust.cargo.check",
                        "exit_code": code,
                        "stdout": trim(&stdout),
                        "stderr": trim(&stderr)
                    })
                }
                Ok(Err(e)) => json!({"ok": false, "error": format!("spawn error: {}", e)}),
                Err(_) => json!({"ok": false, "error": "timeout running cargo check"}),
            }
        }
        "rust.doc" => {
            // Run `cargo doc` in the provided path
            // Params: { path?: string, no_deps?: bool }
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or(".");
            let no_deps = params.get("no_deps").and_then(|v| v.as_bool()).unwrap_or(true);
            let mut cmd = Command::new("cargo");
            cmd.arg("doc").current_dir(path);
            if no_deps { cmd.arg("--no-deps"); }
            match tokio::time::timeout(std::time::Duration::from_secs(900), cmd.output()).await {
                Ok(Ok(out)) => {
                    let code = out.status.code().unwrap_or(-1);
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let trim = |s: &str| -> String {
                        const MAX: usize = 8000;
                        if s.len() > MAX { format!("{}…", &s[..MAX]) } else { s.to_string() }
                    };
                    json!({
                        "ok": out.status.success(),
                        "tool": "rust.doc",
                        "exit_code": code,
                        "stdout": trim(&stdout),
                        "stderr": trim(&stderr),
                        "hint_output_dir": "target/doc"
                    })
                }
                Ok(Err(e)) => json!({"ok": false, "error": format!("spawn error: {}", e)}),
                Err(_) => json!({"ok": false, "error": "timeout running cargo doc"}),
            }
        }
        "rust.coverage.report" => {
            // Try `cargo llvm-cov --summary-only`; return stdout
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or(".");
            let mut cmd = Command::new("cargo");
            cmd.arg("llvm-cov").arg("--summary-only").current_dir(path);
            match tokio::time::timeout(std::time::Duration::from_secs(1200), cmd.output()).await {
                Ok(Ok(out)) => {
                    let code = out.status.code().unwrap_or(-1);
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let trim = |s: &str| -> String {
                        const MAX: usize = 16000;
                        if s.len() > MAX { format!("{}…", &s[..MAX]) } else { s.to_string() }
                    };
                    json!({
                        "ok": out.status.success(),
                        "tool": "rust.coverage.report",
                        "exit_code": code,
                        "stdout": trim(&stdout),
                        "stderr": trim(&stderr)
                    })
                }
                Ok(Err(e)) => json!({"ok": false, "error": format!("spawn error: {}", e)}),
                Err(_) => json!({"ok": false, "error": "timeout running cargo llvm-cov"}),
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
