use clap::Parser;
use serde_json::{json, Value};
use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, select};
use tokio::process::Command;
use tracing::{error, info};

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

    info!("claritas_mcp_python starting JSON-RPC (stdio)");
    let mut stdin = BufReader::new(tokio::io::stdin());
    let mut stdout = tokio::io::stdout();
    let mut buf = String::new();

    loop {
        buf.clear();
        select! {
            read_res = stdin.read_line(&mut buf) => {
                match read_res {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        if let Err(e) = handle_line(&buf, &mut stdout).await { error!(error=%e, "handler error"); }
                    }
                    Err(e) => { error!(error=%e, "stdin read error"); break; }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                break;
            }
        }
    }

    Ok(())
}

async fn handle_line(line: &str, stdout: &mut tokio::io::Stdout) -> anyhow::Result<()> {
    let req: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let params = req.get("params").cloned().unwrap_or(json!({}));

    let result = match method {
        "meta.ping" => json!({"ok": true, "name": "claritas_mcp_python"}),
        "meta.capabilities" => json!({"tools":[
            "python.build.debug","python.build.test","python.build.release",
            "python.test.unit","python.test.integration","python.coverage.report",
            "python.lint","python.format","python.typecheck","python.warn","python.sec.check",
            "code.generate","code.edit","code.refactor.rename","code.refactor.extract","code.refactor.move",
            "fs.read","fs.write","fs.list","fs.delete","fs.move","fs.mkdir",
            "deploy.package","deploy.publish","deploy.run","deploy.k8s.apply"
        ]}),
        "python.lint" => {
            // Try ruff first, then flake8 as fallback
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or(".");
            let path_owned = path.to_string();
            let mut tried = Vec::new();

            async fn run_tool(prog: &str, args: &[&str], path: &str) -> Option<Value> {
                let mut cmd = Command::new(prog);
                cmd.args(args).current_dir(path);
                match tokio::time::timeout(std::time::Duration::from_secs(120), cmd.output()).await {
                    Ok(Ok(out)) => {
                        let code = out.status.code().unwrap_or(-1);
                        let stdout = String::from_utf8_lossy(&out.stdout);
                        let stderr = String::from_utf8_lossy(&out.stderr);
                        let trim = |s: &str| -> String { const MAX: usize = 4000; if s.len()>MAX { format!("{}…", &s[..MAX]) } else { s.to_string() } };
                        Some(json!({
                            "ok": out.status.success(),
                            "exit_code": code,
                            "stdout": trim(&stdout),
                            "stderr": trim(&stderr),
                            "path": path,
                        }))
                    }
                    _ => None,
                }
            }

            // ruff
            tried.push("ruff");
            if let Some(res) = run_tool("ruff", &["check"], &path_owned).await { res } else {
                // flake8
                tried.push("flake8");
                if let Some(res) = run_tool("flake8", &[], &path_owned).await { res } else {
                    json!({
                        "ok": false,
                        "error": "no linter available (ruff/flake8 not found or failed)",
                        "tried": tried,
                        "path": path_owned
                    })
                }
            }
        }
        "python.format" => {
            // Use black to format code. Params:
            // - path: directory or file to format (default '.')
            // - check: if true, run in --check mode (no changes), otherwise format in-place
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or(".");
            let check = params.get("check").and_then(|v| v.as_bool()).unwrap_or(false);

            async fn run_black(path: &str, check: bool) -> Value {
                let mut cmd = Command::new("black");
                if check {
                    cmd.arg("--check");
                }
                cmd.arg(path);
                match tokio::time::timeout(std::time::Duration::from_secs(180), cmd.output()).await {
                    Ok(Ok(out)) => {
                        let code = out.status.code().unwrap_or(-1);
                        let stdout = String::from_utf8_lossy(&out.stdout);
                        let stderr = String::from_utf8_lossy(&out.stderr);
                        let trim = |s: &str| -> String { const MAX: usize = 4000; if s.len()>MAX { format!("{}…", &s[..MAX]) } else { s.to_string() } };
                        json!({
                            "ok": out.status.success(),
                            "exit_code": code,
                            "stdout": trim(&stdout),
                            "stderr": trim(&stderr),
                            "path": path,
                            "mode": if check { "check" } else { "format" }
                        })
                    }
                    Ok(Err(e)) => json!({"ok": false, "error": format!("spawn error: {}", e)}),
                    Err(_) => json!({"ok": false, "error": "timeout running black"}),
                }
            }

            run_black(path, check).await
        }
        "python.test.unit" => {
            // Run pytest in the provided path. Params:
            // - path: directory to run tests from (default '.')
            // - args: optional array of extra pytest args
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or(".");
            let extra: Vec<String> = params
                .get("args")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();

            async fn run_pytest(path: &str, extra: &[String]) -> Value {
                let mut cmd = Command::new("pytest");
                // Quiet but with summary of all tests; show warnings
                cmd.arg("-q").arg("-rA").current_dir(path);
                for a in extra { cmd.arg(a); }
                match tokio::time::timeout(std::time::Duration::from_secs(600), cmd.output()).await {
                    Ok(Ok(out)) => {
                        let code = out.status.code().unwrap_or(-1);
                        let stdout = String::from_utf8_lossy(&out.stdout);
                        let stderr = String::from_utf8_lossy(&out.stderr);
                        let trim = |s: &str| -> String { const MAX: usize = 8000; if s.len()>MAX { format!("{}…", &s[..MAX]) } else { s.to_string() } };
                        json!({
                            "ok": out.status.success(),
                            "exit_code": code,
                            "stdout": trim(&stdout),
                            "stderr": trim(&stderr),
                            "path": path,
                        })
                    }
                    Ok(Err(e)) => json!({"ok": false, "error": format!("spawn error: {}", e)}),
                    Err(_) => json!({"ok": false, "error": "timeout running pytest"}),
                }
            }

            run_pytest(path, &extra).await
        }
        "python.typecheck" => {
            // Run mypy on the provided path (or files). Params:
            // - path: directory or module path (default '.')
            // - strict: bool for --strict
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or(".");
            let strict = params.get("strict").and_then(|v| v.as_bool()).unwrap_or(false);

            async fn run_mypy(path: &str, strict: bool) -> Value {
                let mut cmd = Command::new("mypy");
                if strict { cmd.arg("--strict"); }
                cmd.arg(path);
                match tokio::time::timeout(std::time::Duration::from_secs(480), cmd.output()).await {
                    Ok(Ok(out)) => {
                        let code = out.status.code().unwrap_or(-1);
                        let stdout = String::from_utf8_lossy(&out.stdout);
                        let stderr = String::from_utf8_lossy(&out.stderr);
                        let trim = |s: &str| -> String { const MAX: usize = 8000; if s.len()>MAX { format!("{}…", &s[..MAX]) } else { s.to_string() } };
                        json!({
                            "ok": out.status.success(),
                            "exit_code": code,
                            "stdout": trim(&stdout),
                            "stderr": trim(&stderr),
                            "path": path,
                            "strict": strict,
                        })
                    }
                    Ok(Err(e)) => json!({"ok": false, "error": format!("spawn error: {}", e)}),
                    Err(_) => json!({"ok": false, "error": "timeout running mypy"}),
                }
            }

            run_mypy(path, strict).await
        }
        "python.test.integration" => {
            // Run pytest with optional marker/path for integration tests
            // Params: { path?: string, marker?: string, args?: string[] }
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or(".");
            let marker = params.get("marker").and_then(|v| v.as_str());
            let extra: Vec<String> = params
                .get("args")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();

            async fn run_pytest_integration(path: &str, marker: Option<&str>, extra: &[String]) -> Value {
                let mut cmd = Command::new("pytest");
                cmd.arg("-q").arg("-rA").current_dir(path);
                if let Some(m) = marker { cmd.arg("-m").arg(m); }
                for a in extra { cmd.arg(a); }
                match tokio::time::timeout(std::time::Duration::from_secs(900), cmd.output()).await {
                    Ok(Ok(out)) => {
                        let code = out.status.code().unwrap_or(-1);
                        let stdout = String::from_utf8_lossy(&out.stdout);
                        let stderr = String::from_utf8_lossy(&out.stderr);
                        let trim = |s: &str| -> String { const MAX: usize = 12000; if s.len()>MAX { format!("{}…", &s[..MAX]) } else { s.to_string() } };
                        json!({
                            "ok": out.status.success(),
                            "exit_code": code,
                            "stdout": trim(&stdout),
                            "stderr": trim(&stderr),
                            "path": path,
                            "marker": marker
                        })
                    }
                    Ok(Err(e)) => json!({"ok": false, "error": format!("spawn error: {}", e)}),
                    Err(_) => json!({"ok": false, "error": "timeout running pytest (integration)"}),
                }
            }

            run_pytest_integration(path, marker, &extra).await
        }
        "python.coverage.report" => {
            // Attempt pytest with coverage plugin; fallback to `coverage` tool
            // Params: { path?: string, args?: string[] }
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or(".");
            let extra: Vec<String> = params
                .get("args")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();

            async fn run_pytest_cov(path: &str, extra: &[String]) -> Option<Value> {
                let mut cmd = Command::new("pytest");
                cmd.arg("-q").arg("--maxfail=1").arg("--cov=.").arg("--cov-report=term-missing").current_dir(path);
                for a in extra { cmd.arg(a); }
                match tokio::time::timeout(std::time::Duration::from_secs(1200), cmd.output()).await {
                    Ok(Ok(out)) => {
                        let code = out.status.code().unwrap_or(-1);
                        let stdout = String::from_utf8_lossy(&out.stdout);
                        let stderr = String::from_utf8_lossy(&out.stderr);
                        let trim = |s: &str| -> String { const MAX: usize = 16000; if s.len()>MAX { format!("{}…", &s[..MAX]) } else { s.to_string() } };
                        Some(json!({
                            "ok": out.status.success(),
                            "exit_code": code,
                            "stdout": trim(&stdout),
                            "stderr": trim(&stderr),
                            "path": path,
                            "tool": "pytest-cov"
                        }))
                    }
                    _ => None,
                }
            }

            async fn run_coverage(path: &str) -> Value {
                // Run: coverage run -m pytest -q; coverage report -m
                let mut run = Command::new("coverage");
                run.arg("run").arg("-m").arg("pytest").arg("-q").current_dir(path);
                let run_out = tokio::time::timeout(std::time::Duration::from_secs(1200), run.output()).await;
                match run_out {
                    Ok(Ok(_)) => {
                        let mut rep = Command::new("coverage");
                        rep.arg("report").arg("-m").current_dir(path);
                        match tokio::time::timeout(std::time::Duration::from_secs(300), rep.output()).await {
                            Ok(Ok(out)) => {
                                let code = out.status.code().unwrap_or(-1);
                                let stdout = String::from_utf8_lossy(&out.stdout);
                                let stderr = String::from_utf8_lossy(&out.stderr);
                                let trim = |s: &str| -> String { const MAX: usize = 16000; if s.len()>MAX { format!("{}…", &s[..MAX]) } else { s.to_string() } };
                                json!({
                                    "ok": out.status.success(),
                                    "exit_code": code,
                                    "stdout": trim(&stdout),
                                    "stderr": trim(&stderr),
                                    "path": path,
                                    "tool": "coverage"
                                })
                            }
                            Ok(Err(e)) => json!({"ok": false, "error": format!("coverage report spawn error: {}", e)}),
                            Err(_) => json!({"ok": false, "error": "timeout running coverage report"}),
                        }
                    }
                    Ok(Err(e)) => json!({"ok": false, "error": format!("coverage run spawn error: {}", e)}),
                    Err(_) => json!({"ok": false, "error": "timeout running coverage"}),
                }
            }

            if let Some(v) = run_pytest_cov(path, &extra).await { v } else { run_coverage(path).await }
        }
        "python.sec.check" => {
            // Run bandit security checks if available
            // Params: { path?: string }
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or(".");
            let mut cmd = Command::new("bandit");
            cmd.arg("-r").arg(path).arg("-q");
            match tokio::time::timeout(std::time::Duration::from_secs(900), cmd.output()).await {
                Ok(Ok(out)) => {
                    let code = out.status.code().unwrap_or(-1);
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let trim = |s: &str| -> String { const MAX: usize = 16000; if s.len()>MAX { format!("{}…", &s[..MAX]) } else { s.to_string() } };
                    json!({
                        "ok": out.status.success(),
                        "exit_code": code,
                        "stdout": trim(&stdout),
                        "stderr": trim(&stderr),
                        "path": path,
                        "tool": "bandit"
                    })
                }
                Ok(Err(e)) => json!({"ok": false, "error": format!("spawn error: {}", e)}),
                Err(_) => json!({"ok": false, "error": "timeout running bandit"}),
            }
        }
        "python.warn" => {
            // Run pytest in warnings-on mode
            let path = params.get("path").and_then(|v| v.as_str()).unwrap_or(".");
            let mut cmd = Command::new("pytest");
            cmd.arg("-q").arg("-rA").arg("-W").arg("default").current_dir(path);
            match tokio::time::timeout(std::time::Duration::from_secs(600), cmd.output()).await {
                Ok(Ok(out)) => {
                    let code = out.status.code().unwrap_or(-1);
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let trim = |s: &str| -> String { const MAX: usize = 12000; if s.len()>MAX { format!("{}…", &s[..MAX]) } else { s.to_string() } };
                    json!({
                        "ok": out.status.success(),
                        "exit_code": code,
                        "stdout": trim(&stdout),
                        "stderr": trim(&stderr),
                        "path": path
                    })
                }
                Ok(Err(e)) => json!({"ok": false, "error": format!("spawn error: {}", e)}),
                Err(_) => json!({"ok": false, "error": "timeout running pytest (warnings)"}),
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
