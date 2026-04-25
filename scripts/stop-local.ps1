# =============================================================================
# stop-local.ps1 — stops all gpu-telemetry processes started by start-local.ps1
# =============================================================================
$Root    = (Resolve-Path "$PSScriptRoot\..").Path
$PidFile = Join-Path $Root ".local-pids.json"

if (-not (Test-Path $PidFile)) {
  Write-Host "No running processes found (.local-pids.json not present)."
  exit 0
}

$pids = Get-Content $PidFile | ConvertFrom-Json

foreach ($name in $pids.PSObject.Properties.Name) {
  $procId = $pids.$name
  try {
    Stop-Process -Id $procId -Force -ErrorAction Stop
    Write-Host "Stopped $name (pid $procId)"
  } catch {
    Write-Host "Process $name (pid $procId) was not running"
  }
}

Remove-Item $PidFile -Force
Write-Host "Done."
