use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::RwLock;
use tokio::time::{timeout, Duration};
use tokio::time::sleep;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capability {
    pub name: String,
}

pub fn default_capabilities() -> Vec<Capability> {
    vec![
        Capability { name: "meta.ping".into() },
        Capability { name: "meta.capabilities".into() },
    ]
}

/// Minimal stdio JSON-RPC client for calling a single MCP method by spawning a process.
/// This is a temporary helper until the long-lived supervised hosts are wired.
pub struct StdioClient;

impl StdioClient {
    /// Call a single JSON-RPC method on a command, returning the parsed `result` field.
    /// A simple newline-delimited JSON protocol is assumed.
    pub async fn call_method(
        command: &str,
        args: &[&str],
        method: &str,
        params: Value,
    ) -> anyhow::Result<Value> {
        let mut cmd = Command::new(command);
        if !args.is_empty() {
            cmd.args(args);
        }
        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped());

        let mut child = cmd.spawn()?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("failed to open child stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("failed to open child stdout"))?;
        let mut reader = BufReader::new(stdout);

        let req = json!({"id": 1, "method": method, "params": params});
        let line = serde_json::to_string(&req)? + "\n";
        stdin.write_all(line.as_bytes()).await?;
        stdin.flush().await?;

        // Read a single response line with a timeout to avoid hanging.
        let mut resp_line = String::new();
        timeout(Duration::from_secs(5), reader.read_line(&mut resp_line))
            .await
            .map_err(|_| anyhow::anyhow!("timeout waiting for MCP response"))??;

        let resp: Value = serde_json::from_str(resp_line.trim())?;
        if let Some(err) = resp.get("error") {
            return Err(anyhow::anyhow!("MCP error: {}", err));
        }
        let result = resp.get("result").cloned().unwrap_or(Value::Null);
        Ok(result)
    }
}

/// Managed persistent stdio JSON-RPC client. Requests are serialized.
#[derive(Debug)]
struct ManagedClient {
    command: String,
    args: Vec<String>,
    // child process and I/O
    child: Option<Child>,
    stdin: Option<tokio::process::ChildStdin>,
    stdout: Option<tokio::process::ChildStdout>,
    next_id: u64,
}

impl ManagedClient {
    fn new(command: String, args: Vec<String>) -> Self {
        Self {
            command,
            args,
            child: None,
            stdin: None,
            stdout: None,
            next_id: 1,
        }
    }

    async fn ensure_started(&mut self) -> anyhow::Result<()> {
        if self.child.is_some() {
            return Ok(());
        }
        let mut cmd = Command::new(&self.command);
        if !self.args.is_empty() {
            cmd.args(&self.args);
        }
        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        let mut child = cmd.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("failed to open child stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("failed to open child stdout"))?;
        self.stdin = Some(stdin);
        self.stdout = Some(stdout);
        self.child = Some(child);
        Ok(())
    }

    /// Forcefully stop current process/handles (if any).
    fn teardown(&mut self) {
        // Drop I/O handles first
        self.stdin.take();
        self.stdout.take();
        if let Some(mut ch) = self.child.take() {
            let _ = ch.start_kill();
        }
    }

    /// Restart the child process with a small backoff to avoid rapid restarts.
    async fn restart_with_backoff(&mut self, attempt: u32) -> anyhow::Result<()> {
        self.teardown();
        // Exponential backoff with simple jitterless delay (avoid extra deps)
        let delay_ms = 100u64.saturating_mul((2u64).saturating_pow(attempt.min(5)));
        if delay_ms > 0 { sleep(Duration::from_millis(delay_ms)).await; }
        self.ensure_started().await
    }

    async fn call(&mut self, method: &str, params: Value) -> anyhow::Result<Value> {
        // default to 30s timeout
        self.call_with_timeout(method, params, Duration::from_secs(30)).await
    }

    /// Call with a custom timeout for reading a single response line.
    async fn call_with_timeout(
        &mut self,
        method: &str,
        params: Value,
        read_timeout: Duration,
    ) -> anyhow::Result<Value> {
        // We'll allow one transparent retry with restart on broken pipe/EOF/IO errors.
        let mut attempt: u32 = 0;
        loop {
            // (re)start if needed — serialization guaranteed by outer mutex on ManagedClient
            self.ensure_started().await?;

            // Safe to unwrap now
            let stdin = match self.stdin.as_mut() {
                Some(s) => s,
                None => {
                    // restart and retry
                    if attempt >= 1 { anyhow::bail!("stdin missing after restart"); }
                    self.restart_with_backoff(attempt).await?;
                    attempt += 1;
                    continue;
                }
            };
            let stdout = match self.stdout.as_mut() {
                Some(s) => s,
                None => {
                    if attempt >= 1 { anyhow::bail!("stdout missing after restart"); }
                    self.restart_with_backoff(attempt).await?;
                    attempt += 1;
                    continue;
                }
            };

            let id = self.next_id;
            self.next_id += 1;
            let req = json!({"id": id, "method": method, "params": params});
            let line = serde_json::to_string(&req)? + "\n";

            // Write request
            if let Err(e) = stdin.write_all(line.as_bytes()).await {
                // Broken pipe? try restart once
                if attempt < 1 {
                    self.restart_with_backoff(attempt).await?;
                    attempt += 1;
                    continue;
                }
                return Err(anyhow::anyhow!("write failed: {}", e));
            }
            if let Err(e) = stdin.flush().await {
                if attempt < 1 {
                    self.restart_with_backoff(attempt).await?;
                    attempt += 1;
                    continue;
                }
                return Err(anyhow::anyhow!("flush failed: {}", e));
            }

            // Read a single response line
            let mut reader = BufReader::new(stdout);
            let mut resp_line = String::new();
            let read_res = timeout(read_timeout, reader.read_line(&mut resp_line)).await;
            match read_res {
                Err(_) => {
                    // timeout — do not restart on behalf of caller; report timeout
                    return Err(anyhow::anyhow!("timeout waiting for MCP response"));
                }
                Ok(Err(e)) => {
                    // IO error while reading
                    if attempt < 1 {
                        self.restart_with_backoff(attempt).await?;
                        attempt += 1;
                        continue;
                    }
                    return Err(anyhow::anyhow!("read failed: {}", e));
                }
                Ok(Ok(0)) => {
                    // EOF — restart and retry once
                    if attempt < 1 {
                        self.restart_with_backoff(attempt).await?;
                        attempt += 1;
                        continue;
                    }
                    return Err(anyhow::anyhow!("unexpected EOF from MCP host"));
                }
                Ok(Ok(_n)) => {
                    // fallthrough to parse
                }
            }

            let resp: Value = serde_json::from_str(resp_line.trim())
                .map_err(|e| anyhow::anyhow!("invalid JSON from MCP host: {}", e))?;
            if let Some(err) = resp.get("error") {
                return Err(anyhow::anyhow!("MCP error: {}", err));
            }
            return Ok(resp.get("result").cloned().unwrap_or(Value::Null));
        }
    }

    /// Lightweight health check using meta.ping
    async fn health_check(&mut self, timeout_secs: u64) -> bool {
        self
            .call_with_timeout("meta.ping", json!({}), Duration::from_secs(timeout_secs))
            .await
            .is_ok()
    }

    /// Fetch capabilities via meta.capabilities; returns empty list on error.
    async fn capabilities(&mut self, timeout_secs: u64) -> Vec<Capability> {
        match self
            .call_with_timeout("meta.capabilities", json!({}), Duration::from_secs(timeout_secs))
            .await
        {
            Ok(v) => {
                if let Some(arr) = v.as_array() {
                    let mut caps = Vec::new();
                    for it in arr {
                        if let Some(name) = it.get("name").and_then(|n| n.as_str()) {
                            caps.push(Capability { name: name.to_string() });
                        }
                    }
                    if caps.is_empty() { default_capabilities() } else { caps }
                } else {
                    default_capabilities()
                }
            }
            Err(_) => default_capabilities(),
        }
    }
}

/// Simple Tool Registry that maps tool name prefixes to MCP servers (commands)
/// and routes calls via the stdio client. This is an interim registry that
/// previously spawned a fresh process per call; now upgraded to persistent clients.
#[derive(Clone, Default)]
pub struct ToolRegistry {
    inner: Arc<RwLock<RegistryInner>>,
}

#[derive(Default)]
struct RegistryInner {
    // server_id -> server
    servers: HashMap<String, McpServerInfo>,
    // prefix -> server_id
    prefix_map: HashMap<String, String>,
    // server_id -> managed client
    clients: HashMap<String, Arc<tokio::sync::Mutex<ManagedClient>>>,
}

#[derive(Clone)]
pub struct McpServerInfo {
    pub id: String,
    pub command: String,
    pub args: Vec<String>,
}

impl ToolRegistry {
    pub fn new() -> Self { Self::default() }

    /// Register a server with a set of tool prefixes (e.g., ["python.", "code."])
    pub async fn register_server(
        &self,
        id: impl Into<String>,
        command: impl Into<String>,
        args: Vec<String>,
        prefixes: Vec<String>,
    ) {
        let mut guard = self.inner.write().await;
        let id_s = id.into();
        let command_s = command.into();
        guard.servers.insert(id_s.clone(), McpServerInfo { id: id_s.clone(), command: command_s.clone(), args: args.clone() });
        guard.clients.insert(
            id_s.clone(),
            Arc::new(tokio::sync::Mutex::new(ManagedClient::new(command_s, args))),
        );
        for p in prefixes {
            guard.prefix_map.insert(p, id_s.clone());
        }
    }

    /// Find a server for the tool by longest matching prefix.
    async fn resolve(&self, tool: &str) -> Option<McpServerInfo> {
        let guard = self.inner.read().await;
        // longest prefix match
        let mut best: Option<(usize, &str)> = None;
        for (prefix, sid) in &guard.prefix_map {
            if tool.starts_with(prefix) {
                if let Some((len, _)) = best {
                    if prefix.len() > len { best = Some((prefix.len(), sid.as_str())); }
                } else {
                    best = Some((prefix.len(), sid.as_str()));
                }
            }
        }
        if let Some((_, sid)) = best {
            guard.servers.get(sid).cloned()
        } else {
            None
        }
    }

    /// Call the given tool with params by routing to the resolved server.
    pub async fn call(&self, tool: &str, params: Value) -> anyhow::Result<Value> {
        if let Some(srv) = self.resolve(tool).await {
            let mut guard = self.inner.write().await;
            let sid = srv.id.clone();
            let client = if let Some(c) = guard.clients.get(&sid) {
                c.clone()
            } else {
                // lazily create if missing
                let c = Arc::new(tokio::sync::Mutex::new(ManagedClient::new(
                    srv.command.clone(),
                    srv.args.clone(),
                )));
                guard.clients.insert(sid.clone(), c.clone());
                c
            };
            drop(guard);
            let mut c_lock = client.lock().await;
            c_lock.call(tool, params).await
        } else {
            Err(anyhow::anyhow!(
                "no server registered for tool prefix of '{}'",
                tool
            ))
        }
    }

    /// Call the given tool with a custom timeout for the response read.
    pub async fn call_with_timeout(
        &self,
        tool: &str,
        params: Value,
        timeout_secs: u64,
    ) -> anyhow::Result<Value> {
        if let Some(srv) = self.resolve(tool).await {
            let mut guard = self.inner.write().await;
            let sid = srv.id.clone();
            let client = if let Some(c) = guard.clients.get(&sid) {
                c.clone()
            } else {
                let c = Arc::new(tokio::sync::Mutex::new(ManagedClient::new(
                    srv.command.clone(),
                    srv.args.clone(),
                )));
                guard.clients.insert(sid.clone(), c.clone());
                c
            };
            drop(guard);
            let mut c_lock = client.lock().await;
            c_lock
                .call_with_timeout(tool, params, Duration::from_secs(timeout_secs))
                .await
        } else {
            Err(anyhow::anyhow!(
                "no server registered for tool prefix of '{}'",
                tool
            ))
        }
    }

    /// Health check for all registered servers via meta.ping.
    pub async fn health_check_all(&self, timeout_secs: u64) -> HashMap<String, bool> {
        let mut result = HashMap::new();
        // snapshot server ids to avoid long lock
        let ids: Vec<String> = {
            let guard = self.inner.read().await;
            guard.servers.keys().cloned().collect()
        };
        for sid in ids {
            // get client
            let client = {
                let mut guard = self.inner.write().await;
                if let Some(c) = guard.clients.get(&sid) {
                    c.clone()
                } else if let Some(srv) = guard.servers.get(&sid) {
                    let c = Arc::new(tokio::sync::Mutex::new(ManagedClient::new(
                        srv.command.clone(),
                        srv.args.clone(),
                    )));
                    guard.clients.insert(sid.clone(), c.clone());
                    c
                } else {
                    continue;
                }
            };
            let ok = {
                let mut lock = client.lock().await;
                lock
                    .call_with_timeout("meta.ping", json!({}), Duration::from_secs(timeout_secs))
                    .await
                    .is_ok()
            };
            result.insert(sid, ok);
        }
        result
    }

    /// Combined status snapshot: ok + capabilities per registered server.
    pub async fn status_all(&self, timeout_secs: u64) -> HashMap<String, serde_json::Value> {
        let mut result = HashMap::new();
        // snapshot ids
        let ids: Vec<String> = {
            let guard = self.inner.read().await;
            guard.servers.keys().cloned().collect()
        };
        for sid in ids {
            // get/create client
            let client = {
                let mut guard = self.inner.write().await;
                if let Some(c) = guard.clients.get(&sid) { c.clone() } else if let Some(srv) = guard.servers.get(&sid) {
                    let c = Arc::new(tokio::sync::Mutex::new(ManagedClient::new(srv.command.clone(), srv.args.clone())));
                    guard.clients.insert(sid.clone(), c.clone());
                    c
                } else {
                    continue;
                }
            };
            let (ok, caps): (bool, Vec<Capability>) = {
                let mut lock = client.lock().await;
                let ok = lock.health_check(timeout_secs).await;
                let caps = lock.capabilities(timeout_secs).await;
                (ok, caps)
            };
            let caps_json: Vec<Value> = caps.into_iter().map(|c| json!({"name": c.name})).collect();
            result.insert(sid, json!({"ok": ok, "capabilities": caps_json}));
        }
        result
    }
}
