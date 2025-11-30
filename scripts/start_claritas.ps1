<#
 .SYNOPSIS
  Starts the ClaritasAI application and (optionally) the Clarium MCP server on Windows.

 .DESCRIPTION
  This PowerShell script builds (optional) and launches:
   - Clarium MCP server (claritas_mcp_clarium) in the background, if DSN is provided
   - ClaritasAI app (claritasai-app) with the specified config and bind address

  It writes rolling logs under .\logs\ and records PIDs to a .pid.json file for convenience.

 .PARAMETER Config
  Path to claritas YAML config. Defaults to .\configs\claritasai.yaml

 .PARAMETER Bind
  Address:port for ClaritasAI web server. Defaults to 127.0.0.1:7040

 .PARAMETER ClariumDsn
  Postgres DSN for Clarium (e.g., postgres://user:pass@localhost:5433/claritas?sslmode=disable).
  If set, the script will start claritas_mcp_clarium in the background.

 .PARAMETER ClariumSpec
  Optional path to Clarium spec root; passed to claritas_mcp_clarium --spec

 .PARAMETER Build
  When present, runs `cargo build` before launching binaries.

 .PARAMETER CheckOllama
  When present, performs a quick check against Ollama at http://127.0.0.1:11434/api/tags and reports model availability.

 .PARAMETER OllamaModel
  The LLM model tag expected in Ollama (e.g., gemma3:4b, llama3.1:8b). Only used for the check. Default: gemma3:4b

 .EXAMPLE
  ./scripts/start_claritas.ps1 -Build -Config .\configs\claritasai.yaml -Bind 127.0.0.1:7040 `
    -ClariumDsn "postgres://claritas:claritas@localhost:5433/claritas?sslmode=disable" `
    -ClariumSpec "C:\\data\\clarium-spec" -CheckOllama -OllamaModel gemma3:4b
#>

[CmdletBinding()]
param(
  [string]$Config = ".\configs\claritasai.yaml",
  [string]$Bind = "127.0.0.1:7040",
  [string]$ClariumDsn = $(if ($env:CLARIUM_DSN) { $env:CLARIUM_DSN } else { 'postgres://claritas:claritas@localhost:5433/claritas?sslmode=disable' }),
  [string]$ClariumSpec,
  [switch]$Build,
  [switch]$CheckOllama,
  [string]$OllamaModel = "gemma3:4b"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Resolve-RepoRoot {
  $here = Convert-Path .
  while ($true) {
    if (Test-Path (Join-Path $here 'Cargo.toml')) { return $here }
    $parent = Split-Path $here -Parent
    if (-not $parent -or $parent -eq $here) { throw "Could not find Cargo.toml. Run from within the repo." }
    $here = $parent
  }
}

function New-LogFolder {
  param([string]$RepoRoot)
  $ts = Get-Date -Format 'yyyyMMdd_HHmmss'
  $folder = Join-Path $RepoRoot "logs\$ts"
  New-Item -ItemType Directory -Force -Path $folder | Out-Null
  return $folder
}

function Start-ProcLog {
  param(
    [string]$FilePath,
    [string[]]$Args,
    [string]$LogPath,
    [switch]$Hidden
  )
  $startInfo = New-Object System.Diagnostics.ProcessStartInfo
  $startInfo.FileName = $FilePath
  $startInfo.Arguments = ($Args -join ' ')
  $startInfo.RedirectStandardOutput = $true
  $startInfo.RedirectStandardError = $true
  $startInfo.UseShellExecute = $false
  $startInfo.CreateNoWindow = $true
  $proc = New-Object System.Diagnostics.Process
  $proc.StartInfo = $startInfo
  $null = $proc.Start()
  $stdOut = [System.IO.StreamWriter]::new($LogPath + '.out.log')
  $stdErr = [System.IO.StreamWriter]::new($LogPath + '.err.log')
  $null = $proc.BeginOutputReadLine()
  $null = $proc.BeginErrorReadLine()
  # Manual async piping for PowerShell 5 compatibility
  Start-Job -ScriptBlock {
    param($process,$outPath,$errPath)
    $so = [System.IO.StreamWriter]::new($outPath)
    $se = [System.IO.StreamWriter]::new($errPath)
    try {
      while (-not $process.HasExited) {
        Start-Sleep -Milliseconds 200
      }
    } finally {
      $so.Dispose(); $se.Dispose()
    }
  } -ArgumentList $proc, ($LogPath + '.out.log'), ($LogPath + '.err.log') | Out-Null
  return $proc
}

$repo = Resolve-RepoRoot
Set-Location $repo
Write-Host "Repo: $repo"

if ($Build) {
  Write-Host "Building workspace (debug)..."
  & cargo build
}

$logs = New-LogFolder -RepoRoot $repo
Write-Host "Logs: $logs"

if ($CheckOllama) {
  try {
    $resp = Invoke-RestMethod -Uri 'http://127.0.0.1:11434/api/tags' -Method GET -TimeoutSec 3
    $found = $false
    if ($resp -and $resp.models) {
      foreach ($m in $resp.models) { if ($m.name -eq $OllamaModel) { $found = $true; break } }
    }
    if ($found) { Write-Host "Ollama OK. Model '$OllamaModel' is available." -ForegroundColor Green }
    else { Write-Warning "Ollama reachable, but model '$OllamaModel' not listed. Run: ollama pull $OllamaModel" }
  } catch {
    Write-Warning "Ollama check failed (http://127.0.0.1:11434). Is 'ollama serve' running? $_"
  }
}

$pids = @{}

if ($ClariumDsn) {
  $clariumExe = Join-Path $repo 'target\debug\claritas_mcp_clarium.exe'
  if (-not (Test-Path $clariumExe)) {
    Write-Host "Building claritas_mcp_clarium..."
    & cargo build -p claritas_mcp_clarium
  }
  $args = @()
  $args += '--dsn'; $args += ('"' + $ClariumDsn + '"')
  if ($ClariumSpec) { $args += '--spec'; $args += ('"' + $ClariumSpec + '"') }
  Write-Host "Starting Clarium MCP..."
  $clariumLog = Join-Path $logs 'claritas_mcp_clarium'
  $clarium = Start-Process -FilePath $clariumExe -ArgumentList $args -PassThru -NoNewWindow -RedirectStandardOutput ($clariumLog + '.out.log') -RedirectStandardError ($clariumLog + '.err.log')
  $pids.clarium_mcp = @{ pid = $clarium.Id; exe = $clariumExe; args = $args }
  Start-Sleep -Milliseconds 300
  Write-Host "Clarium MCP PID: $($clarium.Id)"
} else {
  Write-Host "Clarium DSN not provided. Skipping claritas_mcp_clarium launch. (The app may launch MCPs itself if configured)."
}

# Start ClaritasAI app
$appExe = Join-Path $repo 'target\debug\claritasai-app.exe'
if (-not (Test-Path $appExe)) {
  Write-Host "Building claritasai-app..."
  & cargo build -p claritasai-app
}

if (-not (Test-Path $Config)) { throw "Config file not found: $Config" }

$appArgs = @('--config', '"' + (Resolve-Path $Config) + '"', '--bind', $Bind)
Write-Host "Starting ClaritasAI app on http://$Bind ..."
$appLog = Join-Path $logs 'claritasai-app'
$app = Start-Process -FilePath $appExe -ArgumentList $appArgs -PassThru -NoNewWindow -RedirectStandardOutput ($appLog + '.out.log') -RedirectStandardError ($appLog + '.err.log')
$pids.claritasai_app = @{ pid = $app.Id; exe = $appExe; args = $appArgs }
Write-Host "ClaritasAI PID: $($app.Id)"

# Health check loop
Write-Host "Waiting for /health ..."
$healthOk = $false
for ($i=0; $i -lt 20; $i++) {
  try {
    $resp = Invoke-WebRequest -Uri ("http://" + $Bind + "/health") -UseBasicParsing -TimeoutSec 2
    if ($resp.StatusCode -eq 200) { $healthOk = $true; break }
  } catch { Start-Sleep -Milliseconds 500 }
}
if ($healthOk) { Write-Host "/health OK" -ForegroundColor Green }
else { Write-Warning "Health check did not respond 200 within timeout. See logs: $appLog.*.log" }

# Optional DB check
try {
  $resp = Invoke-WebRequest -Uri ("http://" + $Bind + "/db/check") -UseBasicParsing -TimeoutSec 2
  Write-Host "/db/check: $($resp.StatusCode)" }
catch { Write-Host "/db/check not available or failed: $_" }

# Persist PIDs
$pidFile = Join-Path $logs 'pids.json'
($pids | ConvertTo-Json -Depth 5) | Out-File -FilePath $pidFile -Encoding utf8
Write-Host "PID file: $pidFile"

Write-Host "Startup complete. Visit http://$Bind/chat"
