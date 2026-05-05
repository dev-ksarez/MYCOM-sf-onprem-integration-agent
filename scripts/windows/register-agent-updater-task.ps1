param(
  [string]$TaskName = "SfOnpremIntegrationAgent-Updater",
  [string]$ServiceName = "SfOnpremIntegrationAgent",
  [string]$ManifestUrl = "https://github.com/dev-ksarez/MYCOM-sf-onprem-integration-agent/releases/latest/download/update-manifest.json",
  [int]$EveryMinutes = 15,
  [string]$AppRoot
)

# The CI pipeline automatically publishes update-manifest.json on every release tag.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$script:ScriptDirectory = Split-Path -Parent $PSCommandPath

function Resolve-AppRoot {
  param([string]$InputPath)

  if ($InputPath -and $InputPath.Trim()) {
    return (Resolve-Path -Path $InputPath).Path
  }

  $scriptDir = $script:ScriptDirectory
  return (Resolve-Path -Path (Join-Path $scriptDir "..\..\")).Path
}

Write-Host "Manifest URL: $ManifestUrl" -ForegroundColor Cyan
if (-not $ManifestUrl -or -not $ManifestUrl.Trim()) {
  throw "ManifestUrl is required."
}

if ($EveryMinutes -lt 1) {
  throw "EveryMinutes must be >= 1"
}

$appRootResolved = Resolve-AppRoot -InputPath $AppRoot
$scriptPath = Join-Path $appRootResolved "scripts\windows\update-agent.ps1"

if (-not (Test-Path $scriptPath)) {
  throw "Updater script not found: $scriptPath"
}

function Invoke-Schtasks {
  param([string[]]$Arguments)

  $output = & schtasks.exe @Arguments 2>&1
  if ($LASTEXITCODE -ne 0) {
    $message = ($output | Out-String).Trim()
    throw "schtasks.exe failed (exit code $LASTEXITCODE): $message"
  }

  return $output
}

$wrapperPath = Join-Path $appRootResolved "run-agent-updater.cmd"
$wrapperContent = @(
  "@echo off",
  "setlocal",
  ('powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%%~dp0scripts\windows\update-agent.ps1" -ServiceName "{0}" -UpdateManifestUrl "{1}" -AppRoot "%%~dp0"' -f $ServiceName, $ManifestUrl),
  "endlocal"
)
Set-Content -Path $wrapperPath -Value $wrapperContent -Encoding ASCII

$taskCommand = ('"{0}"' -f $wrapperPath)

[void](Invoke-Schtasks -Arguments @(
  "/Create",
  "/TN", $TaskName,
  "/SC", "MINUTE",
  "/MO", "$EveryMinutes",
  "/TR", $taskCommand,
  "/RU", "SYSTEM",
  "/RL", "HIGHEST",
  "/F"
))

Write-Host "Scheduled updater task '$TaskName' created." -ForegroundColor Green
Write-Host "Runs every $EveryMinutes minute(s)."
