<#
 .SYNOPSIS
  Runs the ClaritasAI workspace tests on Windows.

 .DESCRIPTION
  Convenience wrapper around `cargo test` with common flags. Supports filtering by package and test name,
  running in release mode, and passing additional args to the test harness.

 .PARAMETER Package
  Optional Cargo package (crate) name to test (e.g., claritasai-web). If omitted, tests run for the whole workspace.

 .PARAMETER Test
  Optional test name filter (substring). Passed to the test harness after `--`.

 .PARAMETER Release
  When present, builds and tests in release mode.

 .PARAMETER Features
  Optional feature list to enable (comma-separated). Example: "feat1,feat2".

 .PARAMETER NoFailFast
  When present, passes `--no-fail-fast` to cargo test.

 .PARAMETER Verbose
  When present, passes `-vv` to cargo for more logs.

 .PARAMETER Color
  Force cargo color: auto|always|never. Default: auto.

 .EXAMPLE
  ./scripts/run_tests.ps1 -Package claritasai-web -Test http -Verbose
#>

[CmdletBinding()]
param(
  [string]$Package,
  [string]$Test,
  [switch]$Release,
  [string]$Features,
  [switch]$NoFailFast,
  [switch]$Verbose,
  [ValidateSet('auto','always','never')][string]$Color = 'auto'
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

$repo = Resolve-RepoRoot
Set-Location $repo
Write-Host "Repo: $repo"

# Build command
$args = @('test', '--color', $Color)
if ($Verbose) { $args += '-vv' }
if ($Release) { $args += '--release' }
if ($NoFailFast) { $args += '--no-fail-fast' }
if ($Package) { $args += @('-p', $Package) }
if ($Features) { $args += @('--features', $Features) }

# Forward the test name filter to the harness
if ($Test) {
  $args += '--'
  $args += $Test
  # Show test output
  $args += '--nocapture'
}

Write-Host "Running: cargo $($args -join ' ')" -ForegroundColor Cyan

$env:RUST_BACKTRACE = $env:RUST_BACKTRACE ?? '1'

$p = Start-Process -FilePath 'cargo' -ArgumentList $args -NoNewWindow -PassThru -Wait
if ($p.ExitCode -ne 0) {
  Write-Error "Tests failed with exit code $($p.ExitCode)."
  exit $p.ExitCode
}
Write-Host "All tests passed." -ForegroundColor Green
