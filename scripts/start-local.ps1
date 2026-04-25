# =============================================================================
# start-local.ps1 — builds and starts the full gpu-telemetry pipeline on Windows.
#
# Usage:
#   .\scripts\start-local.ps1
#   $env:STREAMER_CSV = "C:\path\to\metrics.csv"; .\scripts\start-local.ps1
#
# Stop with:  .\scripts\stop-local.ps1
# =============================================================================
param(
  [string]$StreamerCsv = $env:STREAMER_CSV,
  [string]$LogLevel    = "info"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root   = (Resolve-Path "$PSScriptRoot\..").Path
$BinDir = Join-Path $Root "bin"

if (-not $StreamerCsv) {
  $StreamerCsv = Join-Path $Root "data\sample_metrics.csv"
}

# ---- prerequisites ----------------------------------------------------------
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
  Write-Error "ERROR: go is not installed. See https://go.dev/dl/"
  exit 1
}

# ---- regenerate Swagger spec ------------------------------------------------
$GoBin   = if ($env:GOPATH) { Join-Path $env:GOPATH "bin" } else { Join-Path $env:USERPROFILE "go\bin" }
$SwagExe = Join-Path $GoBin "swag.exe"

if (Test-Path $SwagExe) {
  Write-Host "[1/5] Regenerating Swagger spec..."
  Set-Location $Root
  & $SwagExe init -g cmd/api/main.go -o docs --quiet
} else {
  Write-Host "[1/5] swag not found - skipping Swagger regeneration."
  Write-Host "      To install: go install github.com/swaggo/swag/cmd/swag@latest"
}

# ---- build ------------------------------------------------------------------
Write-Host "[2/5] Building binaries..."
Set-Location $Root
go build -trimpath -ldflags "-s -w" -o "$BinDir\broker.exe"    .\cmd\broker
go build -trimpath -ldflags "-s -w" -o "$BinDir\streamer.exe"  .\cmd\streamer
go build -trimpath -ldflags "-s -w" -o "$BinDir\collector.exe" .\cmd\collector
go build -trimpath -ldflags "-s -w" -o "$BinDir\api.exe"       .\cmd\api

# ---- validate CSV -----------------------------------------------------------
if (-not (Test-Path $StreamerCsv)) {
  Write-Error "ERROR: CSV file not found: $StreamerCsv`n       Set `$env:STREAMER_CSV to point to a valid DCGM metrics CSV."
  exit 1
}

# ---- stop any stale processes -----------------------------------------------
$PidFile = Join-Path $Root ".local-pids.json"
Write-Host "[3/5] Stopping any existing gpu-telemetry processes..."

# Always kill by process name first — catches processes started outside this script
foreach ($svc in @('broker', 'collector', 'streamer', 'api')) {
  $procs = @(Get-Process -Name $svc -ErrorAction SilentlyContinue)
  if ($procs.Count -gt 0) {
    $procs | Stop-Process -Force
    Write-Host "  killed $($procs.Count) $svc process(es)"
  }
}

# Remove stale PID file if present
if (Test-Path $PidFile) { Remove-Item $PidFile -Force }

Start-Sleep -Milliseconds 500

# ---- start services ---------------------------------------------------------
Write-Host "[4/5] Starting services..."
$pids = @{}

function Start-Svc {
  param([string]$Name, [string]$Exe, [hashtable]$EnvVars)
  $info = New-Object System.Diagnostics.ProcessStartInfo
  $info.FileName        = $Exe
  $info.UseShellExecute = $false
  $info.CreateNoWindow  = $true
  foreach ($k in $EnvVars.Keys) {
    $info.EnvironmentVariables[$k] = $EnvVars[$k]
  }
  $proc = [System.Diagnostics.Process]::Start($info)
  Write-Host "  started $Name (pid $($proc.Id))"
  return $proc.Id
}

$pids["broker"] = Start-Svc "broker" "$BinDir\broker.exe" @{
  MQ_ADDR       = ":7777"
  MQ_ADMIN_ADDR = ":7778"
  LOG_LEVEL     = $LogLevel
}

Write-Host "  Waiting for broker on :7777..."
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
  try {
    $tc = New-Object System.Net.Sockets.TcpClient
    $tc.Connect("localhost", 7777)
    $tc.Close()
    $ready = $true
    break
  } catch { Start-Sleep -Milliseconds 500 }
}
if (-not $ready) { Write-Error "Broker did not start within 15 seconds."; exit 1 }

$pids["collector"] = Start-Svc "collector" "$BinDir\collector.exe" @{
  COLLECTOR_DB = Join-Path $Root "telemetry.db"
  MQ_ADDR      = ":7777"
  LOG_LEVEL    = $LogLevel
}

$pids["streamer"] = Start-Svc "streamer" "$BinDir\streamer.exe" @{
  STREAMER_CSV   = $StreamerCsv
  STREAMER_LOOP  = "true"
  STREAMER_DELAY = "50ms"
  MQ_ADDR        = ":7777"
  LOG_LEVEL      = $LogLevel
}

$pids["api"] = Start-Svc "api" "$BinDir\api.exe" @{
  API_ADDR          = ":8080"
  API_DB            = Join-Path $Root "telemetry.db"
  BROKER_ADMIN_ADDR = "localhost:7778"
  LOG_LEVEL         = $LogLevel
}

$pids | ConvertTo-Json | Set-Content $PidFile

# ---- wait for API -----------------------------------------------------------
Write-Host "[5/5] Waiting for API on :8080..."
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
  try {
    $r = Invoke-WebRequest -Uri "http://localhost:8080/health" -UseBasicParsing -TimeoutSec 1 -ErrorAction Stop
    if ($r.StatusCode -eq 200) { $ready = $true; break }
  } catch { Start-Sleep -Milliseconds 500 }
}

Write-Host ""
Write-Host ("=" * 55)
Write-Host " gpu-telemetry is running"
Write-Host ""
Write-Host " API:          http://localhost:8080"
Write-Host " Swagger UI:   http://localhost:8080/swagger/index.html"
Write-Host " Broker admin: http://localhost:7778"
Write-Host ""
Write-Host " Quick tests:"
Write-Host "   curl http://localhost:8080/health"
Write-Host "   curl http://localhost:8080/api/v1/gpus"
Write-Host "   curl http://localhost:8080/api/v1/broker/stats"
Write-Host ""
Write-Host " To stop:  .\scripts\stop-local.ps1"
Write-Host ("=" * 55)
