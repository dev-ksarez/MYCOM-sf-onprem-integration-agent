param(
  [string]$ServiceName = "SfOnpremIntegrationAgent",
  [string]$WebServiceName = "SfOnpremIntegrationWeb",
  [string]$UpdaterServiceName = "SfOnpremIntegrationUpdater",
  [string]$NodeExePath,
  [string]$AppRoot,
  [string]$UpdateManifestUrl,
  [string]$StatusFilePath,
  [string]$TempRoot = "$env:TEMP\\sf-agent-updater",
  [int]$StartTimeoutSeconds = 60,
  [int]$KeepBackupCount = 5,
  [int]$KeepTempRunCount = 10,
  [int]$LogRetentionDays = 30
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$script:ScriptDirectory = Split-Path -Parent $PSCommandPath

# Load required assemblies for ZIP extraction
try {
  Add-Type -AssemblyName System.IO.Compression.FileSystem
} catch {
  Write-Error "Fehler beim Laden von System.IO.Compression.FileSystem: $($_.Exception.Message)"
  exit 1
}

function Resolve-AppRoot {
  param([string]$InputPath)

  if ($InputPath -and $InputPath.Trim()) {
    return (Resolve-Path -Path $InputPath).Path
  }

  $scriptDir = $script:ScriptDirectory
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

function Get-EnvValueMap {
  param([string]$Path)

  $values = [ordered]@{}
  if (-not (Test-Path -Path $Path)) {
    return $values
  }

  foreach ($line in Get-Content -Path $Path) {
    if ($line -match '^\s*#' -or $line -notmatch '=') {
      continue
    }

    $parts = $line -split '=', 2
    $key = $parts[0].Trim()
    if (-not $key) {
      continue
    }

    $value = if ($parts.Count -gt 1) { $parts[1] } else { "" }
    $values[$key] = $value
  }

  return $values
}

function Set-OrAppendEnvValues {
  param(
    [string]$Path,
    [hashtable]$Updates
  )

  $lines = New-Object System.Collections.Generic.List[string]
  if (Test-Path -Path $Path) {
    foreach ($line in Get-Content -Path $Path) {
      $lines.Add($line)
    }
  }

  foreach ($entry in $Updates.GetEnumerator()) {
    $key = $entry.Key
    $value = [string]$entry.Value
    $replacement = "$key=$value"
    $pattern = '^\s*' + [regex]::Escape($key) + '\s*='
    $replaced = $false

    for ($index = 0; $index -lt $lines.Count; $index += 1) {
      if ($lines[$index] -match $pattern) {
        $lines[$index] = $replacement
        $replaced = $true
        break
      }
    }

    if (-not $replaced) {
      if ($lines.Count -gt 0 -and $lines[$lines.Count - 1] -ne "") {
        $lines.Add("")
      }
      $lines.Add($replacement)
    }
  }

  Set-Content -Path $Path -Value $lines -Encoding UTF8
}

function Ensure-LocalAdminBootstrap {
  param(
    [string]$AppRootPath,
    [string]$PayloadRootPath
  )

  $artifactsDir = Join-Path $AppRootPath "artifacts"
  $adminUsersFilePath = Join-Path $artifactsDir "admin-users.json"
  Ensure-Directory -Path $artifactsDir

  if (-not (Test-Path -Path $adminUsersFilePath)) {
    $payloadAdminUsers = Join-Path $PayloadRootPath "artifacts\admin-users.json"
    if (Test-Path -Path $payloadAdminUsers) {
      Copy-Item -Path $payloadAdminUsers -Destination $adminUsersFilePath -Force
    } else {
      @(
        [ordered]@{
          id = "local-admin"
          username = "admin"
          password = "admin123!"
          displayName = "Lokaler Admin"
          roles = @("admin")
          permissions = @("admin", "read", "write", "delete")
          modules = @("migration")
        }
      ) | ConvertTo-Json -Depth 5 | Set-Content -Path $adminUsersFilePath -Encoding UTF8
    }
  }

  $envPath = Join-Path $AppRootPath ".env"
  if (-not (Test-Path -Path $envPath)) {
    return
  }

  $envValues = Get-EnvValueMap -Path $envPath
  $updates = [ordered]@{}
  if (-not $envValues.Contains('ADMIN_UI_USERS_FILE')) {
    $updates['ADMIN_UI_USERS_FILE'] = "artifacts/admin-users.json"
  }
  if (-not $envValues.Contains('ADMIN_AUTH_MODE')) {
    $updates['ADMIN_AUTH_MODE'] = "local"
  }
  if ($updates.Count -gt 0) {
    Set-OrAppendEnvValues -Path $envPath -Updates $updates
  }
}

function Write-UpdateProgress {
  param(
    [string]$State,
    [string]$Message,
    [int]$ProgressPercent = -1,
    [string]$Stage = "",
    [string]$TargetVersion = ""
  )

  if (-not $script:StatusFilePathResolved) {
    return
  }

  Ensure-Directory -Path (Split-Path -Parent $script:StatusFilePathResolved)
  $payload = [ordered]@{
    state = $State
    message = $Message
    progressPercent = $(if ($ProgressPercent -ge 0) { $ProgressPercent } else { $null })
    stage = $(if ($Stage) { $Stage } else { $null })
    targetVersion = $(if ($TargetVersion) { $TargetVersion } else { $null })
    updatedAt = [DateTime]::UtcNow.ToString("o")
  }
  $payload | ConvertTo-Json | Set-Content -Path $script:StatusFilePathResolved -Encoding UTF8
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

function Cleanup-FilesByAge {
  param(
    [string]$Root,
    [int]$RetentionDays
  )

  if ($RetentionDays -lt 1 -or -not (Test-Path $Root)) {
    return
  }

  $cutoff = (Get-Date).AddDays(-$RetentionDays)
  Get-ChildItem -Path $Root -File -Recurse -ErrorAction SilentlyContinue |
    Where-Object { $_.LastWriteTime -lt $cutoff -and ($_.Extension -in @('.log', '.txt')) } |
    ForEach-Object {
      Remove-Item -Path $_.FullName -Force -ErrorAction SilentlyContinue
    }
}

function Cleanup-DirectoriesByCount {
  param(
    [string]$Root,
    [int]$KeepCount
  )

  if ($KeepCount -lt 0 -or -not (Test-Path $Root)) {
    return
  }

  $directories = @(Get-ChildItem -Path $Root -Directory -ErrorAction SilentlyContinue | Sort-Object Name -Descending)
  if ($directories.Count -le $KeepCount) {
    return
  }

  $directories | Select-Object -Skip $KeepCount | ForEach-Object {
    Remove-Item -Path $_.FullName -Recurse -Force -ErrorAction SilentlyContinue
  }
}

function Expand-ZipArchive {
  param(
    [string]$ArchivePath,
    [string]$DestinationPath
  )

  if (Test-Path $DestinationPath) {
    Remove-Item -Path $DestinationPath -Recurse -Force -ErrorAction SilentlyContinue
  }

  Ensure-Directory -Path $DestinationPath

  $zip = [System.IO.Compression.ZipFile]::OpenRead($ArchivePath)
  try {
    foreach ($entry in $zip.Entries) {
      $entryPath = $entry.FullName
      if (-not $entryPath) {
        continue
      }

      $targetPath = Join-Path $DestinationPath $entryPath
      $targetDirectory = Split-Path -Parent $targetPath
      if ($targetDirectory) {
        Ensure-Directory -Path $targetDirectory
      }

      if ($entryPath.EndsWith('/')) {
        Ensure-Directory -Path $targetPath
        continue
      }

      [System.IO.Compression.ZipFileExtensions]::ExtractToFile($entry, $targetPath, $true)
    }
  }
  finally {
    $zip.Dispose()
  }
}

function Resolve-ExtractedPayloadRoot {
  param([string]$ExtractRoot)

  $candidateRoots = @($ExtractRoot)
  $topDirs = @(Get-ChildItem -Path $ExtractRoot -Directory -ErrorAction SilentlyContinue)

  if ($topDirs.Count -eq 1) {
    $candidateRoots += $topDirs[0].FullName
    $candidateRoots += Join-Path $topDirs[0].FullName "sf-onprem-integration-agent"
  }

  foreach ($candidateRoot in $candidateRoots | Select-Object -Unique) {
    if (-not $candidateRoot) {
      continue
    }

    $candidatePackageJson = Join-Path $candidateRoot "package.json"
    $candidateDist = Join-Path $candidateRoot "dist"
    if ((Test-Path $candidatePackageJson) -and (Test-Path $candidateDist)) {
      return $candidateRoot
    }
  }

  return $ExtractRoot
}

function Invoke-MaintenanceCleanup {
  $backupBase = Join-Path $appRootResolved "backups"
  if (Test-Path $backupBase) {
    $backupDirs = @(Get-ChildItem -Path $backupBase -Directory -ErrorAction SilentlyContinue | Sort-Object Name -Descending)
    if ($backupDirs.Count -gt $KeepBackupCount) {
      $backupDirs | Select-Object -Skip $KeepBackupCount | ForEach-Object {
        Remove-Item -Path $_.FullName -Recurse -Force
      }
    }
  }

  Cleanup-DirectoriesByCount -Root $TempRoot -KeepCount $KeepTempRunCount
  Cleanup-FilesByAge -Root (Join-Path $appRootResolved "logs") -RetentionDays $LogRetentionDays
}

$appRootResolved = Resolve-AppRoot -InputPath $AppRoot
$script:StatusFilePathResolved = if ($StatusFilePath -and $StatusFilePath.Trim()) { $StatusFilePath } else { Join-Path $appRootResolved "logs\dashboard-update-status.json" }
$currentVersion = Get-CurrentVersion -Root $appRootResolved
function Read-InstallProfile {
  $profilePath = Join-Path $appRootResolved "artifacts\runtime\install-profile.json"
  if (-not (Test-Path $profilePath)) {
    return @{
      roles = @("agent", "web", "updater")
    }
  }

  try {
    $parsed = Get-Content -Path $profilePath -Raw | ConvertFrom-Json
    $roles = @()
    if ($parsed.roles) {
      $roles = @($parsed.roles | ForEach-Object { [string]$_ } | Where-Object { $_ })
    }
    if (-not $roles.Count) {
      $roles = @("agent", "web", "updater")
    }
    return @{
      roles = $roles
    }
  } catch {
    return @{
      roles = @("agent", "web", "updater")
    }
  }
}

$installProfile = Read-InstallProfile
function Get-ManagedServices {
  $services = @()
  if ($installProfile.roles -contains "agent") {
    $services += $ServiceName
  }
  if ($installProfile.roles -contains "web") {
    $services += $WebServiceName
  }
  return @($services | Where-Object { $_ -and $_.Trim() } | Select-Object -Unique)
}

function Stop-ManagedServices {
  param([string]$TargetVersion)

  foreach ($name in Get-ManagedServices) {
    $service = Get-Service -Name $name -ErrorAction SilentlyContinue
    if ($service -and $service.Status -ne "Stopped") {
      Write-UpdateProgress -State "running" -Message "Dienst '$name' wird gestoppt." -ProgressPercent 60 -Stage "stop-service" -TargetVersion $TargetVersion
      try {
        Stop-Service -Name $name -Force
        if (-not (Wait-ServiceState -Name $name -ExpectedState "Stopped" -TimeoutSeconds 60)) {
          throw "Service $name timed out while stopping (waited 60s)."
        }
      } catch {
        throw "Service-Stop fehlgeschlagen fuer '$name': $($_.Exception.Message)"
      }
    }
  }
}

function Reconfigure-ManagedServices {
  param([string]$TargetVersion)

  $installerScript = Join-Path $appRootResolved "scripts\windows\install-agent-service.ps1"
  if (-not (Test-Path $installerScript)) {
    throw "Service installer script not found: $installerScript"
  }

  $rolesToReconfigure = @($installProfile.roles | Where-Object { $_ -ne "updater" })

  if (-not $rolesToReconfigure -or $rolesToReconfigure.Count -eq 0) {
    Write-Warning "Reconfigure-ManagedServices: Keine anderen Rollen zu konfigurieren (nur Updater installiert). Ueberspringe Neustart."
    return
  }

  Write-UpdateProgress -State "running" -Message "Dienste werden auf die neue Struktur migriert." -ProgressPercent 90 -Stage "start-service" -TargetVersion $TargetVersion
  
  try {
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $installerScript `
      -AppRoot $appRootResolved `
      -ServiceName $ServiceName `
      -WebServiceName $WebServiceName `
      -UpdaterServiceName $UpdaterServiceName `
      -NodeExePath $NodeExePath `
      -InstallRoles ($rolesToReconfigure -join ",") `
      -NonInteractive `
      -ForceRecreate
  } catch {
    throw "Service-Neukonfiguration Exception: $($_.Exception.Message)"
  }

  if ($LASTEXITCODE -ne 0) {
    throw "Service reconfiguration script failed with exit code $LASTEXITCODE. Check logs in $appRootResolved\logs\ for details."
  }
}

try {
  Write-UpdateProgress -State "running" -Message "Update wird initialisiert." -ProgressPercent 5 -Stage "init"

  if (-not $UpdateManifestUrl) {
    throw "UpdateManifestUrl is required."
  }

  Ensure-Directory -Path $TempRoot
  $runId = (Get-Date).ToString("yyyyMMdd-HHmmss")
  $runRoot = Join-Path $TempRoot $runId
  Ensure-Directory -Path $runRoot

  $manifestPath = Join-Path $runRoot "manifest.json"
  Write-UpdateProgress -State "running" -Message "Update-Manifest wird geladen." -ProgressPercent 10 -Stage "manifest"
  
  try {
    Invoke-WebRequest -Uri $UpdateManifestUrl -OutFile $manifestPath -ErrorAction Stop
  } catch {
    throw "Manifest-Download fehlgeschlagen: $($_.Exception.Message)"
  }
  
  $manifest = Get-Content -Path $manifestPath -Raw | ConvertFrom-Json

  $targetVersion = [string]$manifest.version
  $packageUrl = [string]$manifest.packageUrl
  $sha256 = [string]$manifest.sha256

  if (-not $targetVersion -or -not $packageUrl) {
    throw "Manifest must include version and packageUrl."
  }

  if ((Compare-Version -Left $targetVersion -Right $currentVersion) -le 0) {
    Write-Host "No update needed. Current=$currentVersion Target=$targetVersion"
    Write-UpdateProgress -State "completed" -Message "Kein Update erforderlich." -ProgressPercent 100 -Stage "idle" -TargetVersion $targetVersion
    Invoke-MaintenanceCleanup
    exit 0
  }

  Write-Host "Update available: $currentVersion -> $targetVersion" -ForegroundColor Cyan
  Write-UpdateProgress -State "running" -Message "Updatepaket wird heruntergeladen." -ProgressPercent 20 -Stage "download" -TargetVersion $targetVersion

  $zipPath = Join-Path $runRoot "update.zip"
  
  try {
    Invoke-WebRequest -Uri $packageUrl -OutFile $zipPath -ErrorAction Stop
  } catch {
    throw "Package-Download fehlgeschlagen ($packageUrl): $($_.Exception.Message)"
  }

  if ($sha256) {
    Write-UpdateProgress -State "running" -Message "Paketintegritaet wird geprueft." -ProgressPercent 35 -Stage "verify" -TargetVersion $targetVersion
    try {
      $actualHash = (Get-FileHash -Path $zipPath -Algorithm SHA256).Hash.ToLowerInvariant()
      if ($actualHash -ne $sha256.ToLowerInvariant()) {
        throw "SHA256 mismatch for downloaded package. Expected: $sha256, Got: $actualHash"
      }
    } catch {
      throw "Paketverifizierung fehlgeschlagen: $($_.Exception.Message)"
    }
  }

  $extractRoot = Join-Path $runRoot "extract"
  Write-UpdateProgress -State "running" -Message "Updatepaket wird entpackt." -ProgressPercent 45 -Stage "extract" -TargetVersion $targetVersion
  
  try {
    Expand-ZipArchive -ArchivePath $zipPath -DestinationPath $extractRoot
  } catch {
    throw "ZIP-Entpacken fehlgeschlagen: $($_.Exception.Message)"
  }

  $payloadRoot = Resolve-ExtractedPayloadRoot -ExtractRoot $extractRoot

  $requiredDist = Join-Path $payloadRoot "dist"
  if (-not (Test-Path $requiredDist)) {
    throw "Update package is invalid: dist directory is missing. Payload root: $payloadRoot"
  }

$backupRoot = Join-Path $appRootResolved "backups\$runId"
  Ensure-Directory -Path $backupRoot

  $restorePlan = @(
    @{ Source = (Join-Path $appRootResolved "dist"); Backup = (Join-Path $backupRoot "dist"); Target = (Join-Path $appRootResolved "dist"); Payload = (Join-Path $payloadRoot "dist") },
    @{ Source = (Join-Path $appRootResolved "src\css"); Backup = (Join-Path $backupRoot "src\css"); Target = (Join-Path $appRootResolved "src\css"); Payload = (Join-Path $payloadRoot "src\css") },
    @{ Source = (Join-Path $appRootResolved "src\public"); Backup = (Join-Path $backupRoot "src\public"); Target = (Join-Path $appRootResolved "src\public"); Payload = (Join-Path $payloadRoot "src\public") },
    @{ Source = (Join-Path $appRootResolved "scripts"); Backup = (Join-Path $backupRoot "scripts"); Target = (Join-Path $appRootResolved "scripts"); Payload = (Join-Path $payloadRoot "scripts") },
    @{ Source = (Join-Path $appRootResolved "salesforce"); Backup = (Join-Path $backupRoot "salesforce"); Target = (Join-Path $appRootResolved "salesforce"); Payload = (Join-Path $payloadRoot "salesforce") },
    @{ Source = (Join-Path $appRootResolved "artifacts\templates"); Backup = (Join-Path $backupRoot "artifacts\templates"); Target = (Join-Path $appRootResolved "artifacts\templates"); Payload = (Join-Path $payloadRoot "artifacts\templates") },
    @{ Source = (Join-Path $appRootResolved "artifacts\file-examples"); Backup = (Join-Path $backupRoot "artifacts\file-examples"); Target = (Join-Path $appRootResolved "artifacts\file-examples"); Payload = (Join-Path $payloadRoot "artifacts\file-examples") },
    @{ Source = (Join-Path $appRootResolved "nssm.exe"); Backup = (Join-Path $backupRoot "nssm.exe"); Target = (Join-Path $appRootResolved "nssm.exe"); Payload = (Join-Path $payloadRoot "nssm.exe") },
    @{ Source = (Join-Path $appRootResolved "package.json"); Backup = (Join-Path $backupRoot "package.json"); Target = (Join-Path $appRootResolved "package.json"); Payload = (Join-Path $payloadRoot "package.json") },
    @{ Source = (Join-Path $appRootResolved "node_modules"); Backup = (Join-Path $backupRoot "node_modules"); Target = (Join-Path $appRootResolved "node_modules"); Payload = (Join-Path $payloadRoot "node_modules") }
  )

  foreach ($item in $restorePlan) {
    try {
      Backup-Path -Source $item.Source -Backup $item.Backup
    } catch {
      throw "Backup fehlgeschlagen fuer $($item.Source): $($_.Exception.Message)"
    }
  }

  Stop-ManagedServices -TargetVersion $targetVersion

  $updateSucceeded = $false

  try {
    Write-UpdateProgress -State "running" -Message "Update-Dateien werden eingespielt." -ProgressPercent 75 -Stage "apply" -TargetVersion $targetVersion
    foreach ($item in $restorePlan) {
      if (-not (Test-Path $item.Payload)) {
        Write-Verbose "Payload not found (optional): $($item.Payload)"
        continue
      }

      Write-Verbose "Updating: $($item.Target)"
      try {
        if (Test-Path $item.Target) {
          Remove-Item -Path $item.Target -Recurse -Force -ErrorAction Stop
        }

        if (Test-Path $item.Payload -PathType Container) {
          robocopy $item.Payload $item.Target /E /NFL /NDL /NJH /NJS /NP | Out-Null
          if ($LASTEXITCODE -gt 3) {
            throw "robocopy failed for $($item.Payload) -> $($item.Target) (exit code: $LASTEXITCODE)"
          }
        } else {
          Copy-Item -Path $item.Payload -Destination $item.Target -Force -ErrorAction Stop
        }
      } catch {
        throw "Fehler beim Kopieren von $($item.Source): $($_.Exception.Message)"
      }
    }

    Ensure-LocalAdminBootstrap -AppRootPath $appRootResolved -PayloadRootPath $payloadRoot

    Reconfigure-ManagedServices -TargetVersion $targetVersion

    $updateSucceeded = $true
    Write-UpdateProgress -State "completed" -Message "Update auf Version $targetVersion abgeschlossen." -ProgressPercent 100 -Stage "completed" -TargetVersion $targetVersion
    Write-Host "Update to version $targetVersion completed." -ForegroundColor Green
  } finally {
    if (-not $updateSucceeded) {
      Write-UpdateProgress -State "failed" -Message "Update fehlgeschlagen. Rollback wird gestartet." -ProgressPercent 95 -Stage "rollback" -TargetVersion $targetVersion
      Write-Warning "Update failed. Starting rollback."

      foreach ($name in Get-ManagedServices) {
        try {
          Stop-Service -Name $name -Force -ErrorAction SilentlyContinue
          [void](Wait-ServiceState -Name $name -ExpectedState "Stopped" -TimeoutSeconds 30)
        } catch {
          Write-Warning "Fehler beim Stoppen von $name waehrend Rollback: $($_.Exception.Message)"
        }
      }

      foreach ($item in $restorePlan) {
        try {
          Restore-Path -Backup $item.Backup -Target $item.Target
        } catch {
          Write-Warning "Restore fehlgeschlagen fuer $($item.Target): $($_.Exception.Message)"
        }
      }

      try {
        Reconfigure-ManagedServices -TargetVersion $targetVersion
      } catch {
        Write-Warning "Service rollback configuration failed: $($_.Exception.Message)"
      }

      Write-UpdateProgress -State "failed" -Message "Update fehlgeschlagen. Der vorherige Stand wurde wiederhergestellt." -ProgressPercent 100 -Stage "failed" -TargetVersion $targetVersion
    }
  }

  Invoke-MaintenanceCleanup
} catch {
  Write-Host "Update failed: $($_.Exception.Message)" -ForegroundColor Red
  Write-Error $_.Exception.Message
  Write-UpdateProgress -State "failed" -Message "Update fehlgeschlagen: $($_.Exception.Message)" -ProgressPercent 0 -Stage "error" -TargetVersion ""
  
  try {
    Invoke-MaintenanceCleanup
  } catch {
    Write-Warning "Cleanup failed: $($_.Exception.Message)"
  }
  
  exit 1
}
