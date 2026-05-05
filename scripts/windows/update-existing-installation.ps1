param(
  [string]$AppRoot = "C:\apps\sf-onprem-integration-agent",
  [string]$ServiceName = "SfOnpremIntegrationAgent",
  [string]$ReleaseVersion,
  [string]$UpdateManifestUrl,
  [int]$StartTimeoutSeconds = 60,
  [int]$KeepBackupCount = 5,
  [int]$KeepTempRunCount = 10,
  [int]$LogRetentionDays = 30,
  [switch]$ReRegisterUpdaterTask,
  [int]$UpdaterEveryMinutes = 15
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$script:ScriptDirectory = Split-Path -Parent $PSCommandPath
$repoReleaseBase = "https://github.com/dev-ksarez/MYCOM-sf-onprem-integration-agent/releases"

function Resolve-PackagedScriptPath {
  param(
    [string[]]$RelativePaths,
    [string]$ScriptName
  )

  foreach ($relativePath in $RelativePaths) {
    $candidate = Join-Path $script:ScriptDirectory $relativePath
    if (Test-Path $candidate) {
      return $candidate
    }
  }

  return $null
}

function Resolve-ManifestUrl {
  param(
    [string]$ManifestUrl,
    [string]$Version
  )

  if ($ManifestUrl -and $ManifestUrl.Trim()) {
    return $ManifestUrl.Trim()
  }

  $normalizedVersion = [string]$Version
  if ($normalizedVersion -and $normalizedVersion.Trim()) {
    $normalizedVersion = $normalizedVersion.Trim()
    if (-not $normalizedVersion.StartsWith("v", [System.StringComparison]::OrdinalIgnoreCase)) {
      $normalizedVersion = "v$normalizedVersion"
    }

    return "$repoReleaseBase/download/$normalizedVersion/update-manifest.json"
  }

  return "$repoReleaseBase/latest/download/update-manifest.json"
}

function Resolve-UpdateScriptPath {
  param([string]$Root)

  $localScript = Resolve-PackagedScriptPath -RelativePaths @(
    "update-agent.ps1",
    "sf-onprem-integration-agent\scripts\windows\update-agent.ps1"
  ) -ScriptName "update-agent.ps1"
  if ($localScript) {
    return $localScript
  }

  $installedScript = Join-Path $Root "scripts\windows\update-agent.ps1"
  if (Test-Path $installedScript) {
    return $installedScript
  }

  throw "update-agent.ps1 wurde weder unter $installedScript noch im entpackten Release-Paket gefunden."
}

function Resolve-RegisterScriptPath {
  param([string]$Root)

  $localScript = Resolve-PackagedScriptPath -RelativePaths @(
    "register-agent-updater-task.ps1",
    "sf-onprem-integration-agent\scripts\windows\register-agent-updater-task.ps1"
  ) -ScriptName "register-agent-updater-task.ps1"
  if ($localScript) {
    return $localScript
  }

  $installedScript = Join-Path $Root "scripts\windows\register-agent-updater-task.ps1"
  if (Test-Path $installedScript) {
    return $installedScript
  }

  throw "register-agent-updater-task.ps1 wurde weder unter $installedScript noch im entpackten Release-Paket gefunden."
}

$manifestUrl = Resolve-ManifestUrl -ManifestUrl $UpdateManifestUrl -Version $ReleaseVersion
$updateScript = Resolve-UpdateScriptPath -Root $AppRoot

Write-Host "Starte Update fuer bestehende Windows-Installation..." -ForegroundColor Cyan
Write-Host "AppRoot: $AppRoot"
Write-Host "Manifest: $manifestUrl"

& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $updateScript `
  -ServiceName $ServiceName `
  -AppRoot $AppRoot `
  -UpdateManifestUrl $manifestUrl `
  -StartTimeoutSeconds $StartTimeoutSeconds `
  -KeepBackupCount $KeepBackupCount `
  -KeepTempRunCount $KeepTempRunCount `
  -LogRetentionDays $LogRetentionDays

if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

if ($ReRegisterUpdaterTask) {
  $registerScript = Resolve-RegisterScriptPath -Root $AppRoot
  Write-Host "Registriere Auto-Updater mit derselben Manifest-URL neu..." -ForegroundColor Cyan
  & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $registerScript `
    -AppRoot $AppRoot `
    -ServiceName $ServiceName `
    -ManifestUrl $manifestUrl `
    -EveryMinutes $UpdaterEveryMinutes

  if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
  }
}

Write-Host "Bestehende Windows-Installation erfolgreich aktualisiert." -ForegroundColor Green