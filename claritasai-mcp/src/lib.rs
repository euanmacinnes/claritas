use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::RwLock;
use tokio::time::{timeout, Duration};

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

    async fn call(&mut self, method: &str, params: Value) -> anyhow::Result<Value> {
        // (re)start if needed — serialization guaranteed by outer mutex on ManagedClient
        self.ensure_started().await?;

        // Safe to unwrap now
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("stdin missing"))?;
        let stdout = self
            .stdout
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("stdout missing"))?;

        let id = self.next_id;
        self.next_id += 1;
        let req = json!({"id": id, "method": method, "params": params});
        let line = serde_json::to_string(&req)? + "\n";
        stdin.write_all(line.as_bytes()).await?;
        stdin.flush().await?;

        let mut reader = BufReader::new(stdout);
        let mut resp_line = String::new();
        timeout(Duration::from_secs(30), reader.read_line(&mut resp_line))
            .await
            .map_err(|_| anyhow::anyhow!("timeout waiting for MCP response"))??;

        let resp: Value = serde_json::from_str(resp_line.trim())?;
        if let Some(err) = resp.get("error") {
            return Err(anyhow::anyhow!("MCP error: {}", err));
        }
        Ok(resp.get("result").cloned().unwrap_or(Value::Null))
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
}
