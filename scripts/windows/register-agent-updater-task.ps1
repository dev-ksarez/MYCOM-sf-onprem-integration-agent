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

function Resolve-AppRoot {
  param([string]$InputPath)

  if ($InputPath -and $InputPath.Trim()) {
    return (Resolve-Path -Path $InputPath).Path
  }

  $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
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

$argument = "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`" -ServiceName `"$ServiceName`" -UpdateManifestUrl `"$ManifestUrl`" -AppRoot `"$appRootResolved`""

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $argument
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1)
$trigger.RepetitionInterval = (New-TimeSpan -Minutes $EveryMinutes)
$trigger.RepetitionDuration = ([TimeSpan]::MaxValue)

$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 2)

Register-ScheduledTask \
  -TaskName $TaskName \
  -Action $action \
  -Trigger $trigger \
  -Principal $principal \
  -Settings $settings \
  -Force | Out-Null

Write-Host "Scheduled updater task '$TaskName' created." -ForegroundColor Green
Write-Host "Runs every $EveryMinutes minute(s)."
