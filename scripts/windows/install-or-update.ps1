param(
  [string]$AppRoot = "C:\apps\sf-onprem-integration-agent",
  [string]$ServiceName = "SfOnpremIntegrationAgent",
  [string]$WebServiceName = "SfOnpremIntegrationWeb",
  [string]$UpdaterServiceName = "SfOnpremIntegrationUpdater",
  [string]$InstallRoles = "agent,web,updater",
  [int]$WebUiPort = 8080,
  [int]$SchedulerIntervalMs = 60000,
  [int]$UpdateCheckIntervalMs = 900000,
  [switch]$PromptForEnv,
  [switch]$ForceRecreateServices
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$scriptDirectory = Split-Path -Parent $PSCommandPath
$sourceRoot = Join-Path $scriptDirectory "sf-onprem-integration-agent"

function Test-IsElevated {
  $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
  $principal = New-Object Security.Principal.WindowsPrincipal($identity)
  return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Convert-BoundParametersToArgumentList {
  param([hashtable]$BoundParameters)

  $arguments = @()
  foreach ($entry in $BoundParameters.GetEnumerator()) {
    $key = [string]$entry.Key
    $value = $entry.Value
    if ($value -is [switch]) {
      if ($value.IsPresent) {
        $arguments += "-$key"
      }
      continue
    }

    if ($null -eq $value -or [string]::IsNullOrWhiteSpace([string]$value)) {
      continue
    }

    $arguments += "-$key"
    $arguments += [string]$value
  }

  return ,$arguments
}

function Convert-ArgumentToProcessString {
  param([string]$Value)

  if ($null -eq $Value) {
    return '""'
  }

  return '"' + ($Value -replace '"', '""') + '"'
}

function Ensure-Elevated {
  param([hashtable]$BoundParameters)

  if (Test-IsElevated) {
    return
  }

  $argumentList = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $PSCommandPath
  )
  $argumentList += Convert-BoundParametersToArgumentList -BoundParameters $BoundParameters
  $argumentString = ($argumentList | ForEach-Object { Convert-ArgumentToProcessString -Value $_ }) -join ' '

  Write-Host "Administratorrechte werden angefordert..." -ForegroundColor Yellow
  Start-Process -FilePath "powershell.exe" -Verb RunAs -ArgumentList $argumentString -ErrorAction Stop | Out-Null
  exit 0
}

Ensure-Elevated -BoundParameters $PSBoundParameters

if (-not (Test-Path (Join-Path $sourceRoot "package.json")) -or -not (Test-Path (Join-Path $sourceRoot "dist"))) {
  throw "Das entpackte Paket ist unvollstaendig. Erwartet wurde: $sourceRoot"
}

$existingPackageJson = Join-Path $AppRoot "package.json"

if (Test-Path $existingPackageJson) {
  $updateScript = Join-Path $scriptDirectory "update-existing-installation.ps1"
  if (-not (Test-Path $updateScript)) {
    throw "Update-Skript fehlt: $updateScript"
  }

  Write-Host "Bestehende Installation gefunden. Starte Offline-Update..." -ForegroundColor Cyan
  & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $updateScript `
    -AppRoot $AppRoot `
    -ServiceName $ServiceName `
    -SourceRoot $sourceRoot

  exit $LASTEXITCODE
}

$installScript = Join-Path $scriptDirectory "install-new-customer.ps1"
if (-not (Test-Path $installScript)) {
  throw "Installationsskript fehlt: $installScript"
}

Write-Host "Keine bestehende Installation gefunden. Starte Neuinstallation..." -ForegroundColor Cyan
$installArgs = @(
  "-NoProfile",
  "-ExecutionPolicy", "Bypass",
  "-File", $installScript,
  "-SourceRoot", $sourceRoot,
  "-AppRoot", $AppRoot,
  "-ServiceName", $ServiceName,
  "-WebServiceName", $WebServiceName,
  "-UpdaterServiceName", $UpdaterServiceName,
  "-InstallRoles", $InstallRoles,
  "-WebUiPort", $WebUiPort,
  "-SchedulerIntervalMs", $SchedulerIntervalMs,
  "-UpdateCheckIntervalMs", $UpdateCheckIntervalMs
)

if ($PromptForEnv) {
  $installArgs += "-PromptForEnv"
}

if ($ForceRecreateServices) {
  $installArgs += "-ForceRecreateServices"
}

& powershell.exe @installArgs
exit $LASTEXITCODE
