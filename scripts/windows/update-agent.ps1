param(
  [string]$ServiceName = "SfOnpremIntegrationAgent",
  [string]$AppRoot,
  [string]$UpdateManifestUrl,
  [string]$TempRoot = "$env:TEMP\\sf-agent-updater",
  [int]$StartTimeoutSeconds = 60,
  [int]$KeepBackupCount = 5
)

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

function Wait-ServiceState {
  param(
    [string]$Name,
    [string]$ExpectedState,
    [int]$TimeoutSeconds = 30
  )

  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

  while ((Get-Date) -lt $deadline) {
    $service = Get-Service -Name $Name -ErrorAction SilentlyContinue
    if ($service -and $service.Status.ToString().Equals($ExpectedState, [System.StringComparison]::OrdinalIgnoreCase)) {
      return $true
    }

    Start-Sleep -Seconds 1
  }

  return $false
}

function Get-CurrentVersion {
  param([string]$Root)

  $packageJsonPath = Join-Path $Root "package.json"
  if (-not (Test-Path $packageJsonPath)) {
    throw "package.json not found at $packageJsonPath"
  }

  $pkg = Get-Content -Path $packageJsonPath -Raw | ConvertFrom-Json
  return [string]$pkg.version
}

function Compare-Version {
  param(
    [string]$Left,
    [string]$Right
  )

  try {
    $l = [version]$Left
    $r = [version]$Right
    return $l.CompareTo($r)
  } catch {
    if ($Left -eq $Right) { return 0 }
    return [string]::CompareOrdinal($Left, $Right)
  }
}

function Ensure-Directory {
  param([string]$Path)

  if (-not (Test-Path $Path)) {
    New-Item -ItemType Directory -Path $Path | Out-Null
  }
}

function Backup-Path {
  param(
    [string]$Source,
    [string]$Backup
  )

  if (-not (Test-Path $Source)) {
    return
  }

  Ensure-Directory -Path (Split-Path -Parent $Backup)

  if (Test-Path $Source -PathType Container) {
    robocopy $Source $Backup /E /NFL /NDL /NJH /NJS /NP | Out-Null
  } else {
    Copy-Item -Path $Source -Destination $Backup -Force
  }
}

function Restore-Path {
  param(
    [string]$Backup,
    [string]$Target
  )

  if (-not (Test-Path $Backup)) {
    return
  }

  if (Test-Path $Backup -PathType Container) {
    if (Test-Path $Target) {
      Remove-Item -Path $Target -Recurse -Force
    }

    robocopy $Backup $Target /E /NFL /NDL /NJH /NJS /NP | Out-Null
  } else {
    Copy-Item -Path $Backup -Destination $Target -Force
  }
}

$appRootResolved = Resolve-AppRoot -InputPath $AppRoot
$currentVersion = Get-CurrentVersion -Root $appRootResolved

if (-not $UpdateManifestUrl) {
  throw "UpdateManifestUrl is required."
}

Ensure-Directory -Path $TempRoot
$runId = (Get-Date).ToString("yyyyMMdd-HHmmss")
$runRoot = Join-Path $TempRoot $runId
Ensure-Directory -Path $runRoot

$manifestPath = Join-Path $runRoot "manifest.json"
Invoke-WebRequest -Uri $UpdateManifestUrl -OutFile $manifestPath
$manifest = Get-Content -Path $manifestPath -Raw | ConvertFrom-Json

$targetVersion = [string]$manifest.version
$packageUrl = [string]$manifest.packageUrl
$sha256 = [string]$manifest.sha256

if (-not $targetVersion -or -not $packageUrl) {
  throw "Manifest must include version and packageUrl."
}

if ((Compare-Version -Left $targetVersion -Right $currentVersion) -le 0) {
  Write-Host "No update needed. Current=$currentVersion Target=$targetVersion"
  exit 0
}

Write-Host "Update available: $currentVersion -> $targetVersion" -ForegroundColor Cyan

$zipPath = Join-Path $runRoot "update.zip"
Invoke-WebRequest -Uri $packageUrl -OutFile $zipPath

if ($sha256) {
  $actualHash = (Get-FileHash -Path $zipPath -Algorithm SHA256).Hash.ToLowerInvariant()
  if ($actualHash -ne $sha256.ToLowerInvariant()) {
    throw "SHA256 mismatch for downloaded package."
  }
}

$extractRoot = Join-Path $runRoot "extract"
Expand-Archive -Path $zipPath -DestinationPath $extractRoot -Force

$payloadRoot = $extractRoot
$topDirs = Get-ChildItem -Path $extractRoot -Directory
if ($topDirs.Count -eq 1 -and (Test-Path (Join-Path $topDirs[0].FullName "package.json"))) {
  $payloadRoot = $topDirs[0].FullName
}

$requiredDist = Join-Path $payloadRoot "dist"
if (-not (Test-Path $requiredDist)) {
  throw "Update package is invalid: dist directory is missing."
}

$backupRoot = Join-Path $appRootResolved "backups\$runId"
Ensure-Directory -Path $backupRoot

$restorePlan = @(
  @{ Source = (Join-Path $appRootResolved "dist"); Backup = (Join-Path $backupRoot "dist"); Target = (Join-Path $appRootResolved "dist"); Payload = (Join-Path $payloadRoot "dist") },
  @{ Source = (Join-Path $appRootResolved "package.json"); Backup = (Join-Path $backupRoot "package.json"); Target = (Join-Path $appRootResolved "package.json"); Payload = (Join-Path $payloadRoot "package.json") },
  @{ Source = (Join-Path $appRootResolved "node_modules"); Backup = (Join-Path $backupRoot "node_modules"); Target = (Join-Path $appRootResolved "node_modules"); Payload = (Join-Path $payloadRoot "node_modules") }
)

foreach ($item in $restorePlan) {
  Backup-Path -Source $item.Source -Backup $item.Backup
}

$service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($service -and $service.Status -ne "Stopped") {
  Stop-Service -Name $ServiceName -Force
  if (-not (Wait-ServiceState -Name $ServiceName -ExpectedState "Stopped" -TimeoutSeconds 60)) {
    throw "Service $ServiceName did not stop in time."
  }
}

$updateSucceeded = $false

try {
  foreach ($item in $restorePlan) {
    if (-not (Test-Path $item.Payload)) {
      continue
    }

    if (Test-Path $item.Target) {
      Remove-Item -Path $item.Target -Recurse -Force
    }

    if (Test-Path $item.Payload -PathType Container) {
      robocopy $item.Payload $item.Target /E /NFL /NDL /NJH /NJS /NP | Out-Null
    } else {
      Copy-Item -Path $item.Payload -Destination $item.Target -Force
    }
  }

  if ($service) {
    Start-Service -Name $ServiceName
    if (-not (Wait-ServiceState -Name $ServiceName -ExpectedState "Running" -TimeoutSeconds $StartTimeoutSeconds)) {
      throw "Service $ServiceName did not return to Running state after update."
    }
  }

  $updateSucceeded = $true
  Write-Host "Update to version $targetVersion completed." -ForegroundColor Green
} finally {
  if (-not $updateSucceeded) {
    Write-Warning "Update failed. Starting rollback."

    if ($service) {
      Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue
      [void](Wait-ServiceState -Name $ServiceName -ExpectedState "Stopped" -TimeoutSeconds 30)
    }

    foreach ($item in $restorePlan) {
      Restore-Path -Backup $item.Backup -Target $item.Target
    }

    if ($service) {
      Start-Service -Name $ServiceName -ErrorAction SilentlyContinue
      [void](Wait-ServiceState -Name $ServiceName -ExpectedState "Running" -TimeoutSeconds $StartTimeoutSeconds)
    }
  }
}

$backupBase = Join-Path $appRootResolved "backups"
if (Test-Path $backupBase) {
  $backupDirs = Get-ChildItem -Path $backupBase -Directory | Sort-Object Name -Descending
  if ($backupDirs.Count -gt $KeepBackupCount) {
    $backupDirs | Select-Object -Skip $KeepBackupCount | ForEach-Object {
      Remove-Item -Path $_.FullName -Recurse -Force
    }
  }
}
