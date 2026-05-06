param(
  [string]$TaskName = "SfOnpremIntegrationAgent-Updater",
  [string]$ServiceName = "SfOnpremIntegrationAgent",
  [string]$WebServiceName = "SfOnpremIntegrationWeb",
  [string]$UpdaterServiceName = "SfOnpremIntegrationUpdater",
  [int]$EveryMinutes = 15,
  [string]$AppRoot
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$script:ScriptDirectory = Split-Path -Parent $PSCommandPath

function Resolve-AppRoot {
  param([string]$InputPath)

  if ($InputPath -and $InputPath.Trim()) {
    return (Resolve-Path -Path $InputPath).Path
  }

  return (Resolve-Path -Path (Join-Path $script:ScriptDirectory "..\..\")).Path
}

$appRootResolved = Resolve-AppRoot -InputPath $AppRoot
$installerScript = Join-Path $appRootResolved "scripts\windows\install-agent-service.ps1"
if (-not (Test-Path $installerScript)) {
  throw "Service installer script not found: $installerScript"
}

Write-Host "Der fruehere Scheduled-Task-Updater wurde durch einen eigenen Windows-Dienst ersetzt." -ForegroundColor Yellow
Write-Host "Konfiguriere stattdessen den AutoUpdater-Dienst '$UpdaterServiceName'..." -ForegroundColor Cyan

& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $installerScript `
  -AppRoot $appRootResolved `
  -ServiceName $ServiceName `
  -WebServiceName $WebServiceName `
  -UpdaterServiceName $UpdaterServiceName `
  -UpdateCheckIntervalMs ($EveryMinutes * 60 * 1000) `
  -NonInteractive `
  -ForceRecreate

if ($LASTEXITCODE -ne 0) {
  throw "AutoUpdater service configuration failed (exit code $LASTEXITCODE)."
}

Write-Host "AutoUpdater-Dienst '$UpdaterServiceName' konfiguriert." -ForegroundColor Green
Write-Host "Hinweis: Der Parameter TaskName ('$TaskName') wird aus Rueckwaertskompatibilitaetsgruenden ignoriert."
