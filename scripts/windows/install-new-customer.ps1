param(
  [string]$PackageZip,
  [string]$SourceRoot,
  [string]$AppRoot = "C:\apps\sf-onprem-integration-agent",
  [string]$NodeExePath,
  [string]$ServiceName = "SfOnpremIntegrationAgent",
  [string]$WebServiceName = "SfOnpremIntegrationWeb",
  [string]$UpdaterServiceName = "SfOnpremIntegrationUpdater",
  [string]$InstallRoles = "agent,web,updater",
  [int]$WebUiPort = 8080,
  [int]$SchedulerIntervalMs = 60000,
  [int]$UpdateCheckIntervalMs = 900000,
  [string]$LogFile,
  [switch]$OverwriteExisting,
  [switch]$InstallDependencies,
  [switch]$PromptForEnv,
  [switch]$ForceRecreateServices
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$script:ScriptDirectory = Split-Path -Parent $PSCommandPath
$script:TempExtractRoot = $null
$script:InstallerLogFile = if ($LogFile -and $LogFile.Trim()) { $LogFile.Trim() } else { Join-Path $env:TEMP "sf-onprem-integration-agent-new-customer-install.log" }

function Initialize-InstallerLog {
  $directory = Split-Path -Parent $script:InstallerLogFile
  if ($directory -and -not (Test-Path -Path $directory)) {
    New-Item -Path $directory -ItemType Directory -Force | Out-Null
  }

  if (-not (Test-Path -Path $script:InstallerLogFile)) {
    New-Item -Path $script:InstallerLogFile -ItemType File -Force | Out-Null
  }
}

function Write-InstallerLog {
  param(
    [string]$Message,
    [ValidateSet("INFO", "WARN", "ERROR")]
    [string]$Level = "INFO"
  )

  try {
    Initialize-InstallerLog
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff"
    Add-Content -Path $script:InstallerLogFile -Value "[$timestamp] [$Level] $Message"
  }
  catch {
  }
}

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
    Write-InstallerLog -Message "New customer installer already running with administrator rights."
    return
  }

  $scriptPath = $PSCommandPath
  if (-not $scriptPath) {
    throw "Die automatische Rechteanhebung ist fehlgeschlagen, weil der Skriptpfad nicht aufgeloest werden konnte."
  }

  $argumentList = @(
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    $scriptPath
  )
  $argumentList += Convert-BoundParametersToArgumentList -BoundParameters $BoundParameters
  $argumentString = ($argumentList | ForEach-Object { Convert-ArgumentToProcessString -Value $_ }) -join ' '

  Write-Host "Administratorrechte werden angefordert..." -ForegroundColor Yellow
  Write-InstallerLog -Message "Requesting administrator rights via UAC for new customer installer."
  Start-Process -FilePath "powershell.exe" -Verb RunAs -ArgumentList $argumentString -ErrorAction Stop | Out-Null
  exit 0
}

function Ask-YesNo {
  param(
    [string]$Prompt,
    [bool]$Default = $false
  )

  $suffix = if ($Default) { "[Y/n]" } else { "[y/N]" }
  $answer = Read-Host "$Prompt $suffix"

  if (-not $answer) {
    return $Default
  }

  return $answer.Trim().ToLowerInvariant().StartsWith("y")
}

function Resolve-SourceRoot {
  param(
    [string]$PackageZipPath,
    [string]$ProvidedSourceRoot
  )

  if ($ProvidedSourceRoot -and $ProvidedSourceRoot.Trim()) {
    $resolved = (Resolve-Path -Path $ProvidedSourceRoot.Trim()).Path
    if (-not (Test-Path -Path (Join-Path $resolved "package.json"))) {
      throw "SourceRoot enthaelt keine package.json: $resolved"
    }

    return $resolved
  }

  if ($PackageZipPath -and $PackageZipPath.Trim()) {
    $zipResolved = (Resolve-Path -Path $PackageZipPath.Trim()).Path
    $extractRoot = Join-Path $env:TEMP ("sf-agent-new-customer-extract-" + [guid]::NewGuid().ToString("N"))
    $script:TempExtractRoot = $extractRoot
    Expand-Archive -Path $zipResolved -DestinationPath $extractRoot -Force
    $candidate = Get-ChildItem -Path $extractRoot -Directory -Recurse | Where-Object {
      Test-Path (Join-Path $_.FullName "package.json")
    } | Select-Object -First 1

    if (-not $candidate) {
      throw "Im ZIP wurde kein Anwendungsordner mit package.json gefunden."
    }

    return $candidate.FullName
  }

  $localPackageCandidate = Join-Path $script:ScriptDirectory "sf-onprem-integration-agent"
  if (Test-Path -Path (Join-Path $localPackageCandidate "package.json")) {
    return $localPackageCandidate
  }

  $repoCandidate = Resolve-Path -Path (Join-Path $script:ScriptDirectory "..\..") -ErrorAction SilentlyContinue
  if ($repoCandidate -and (Test-Path -Path (Join-Path $repoCandidate.Path "package.json"))) {
    return $repoCandidate.Path
  }

  if (Test-Path -Path (Join-Path $script:ScriptDirectory "package.json")) {
    return $script:ScriptDirectory
  }

  throw "Kein Installationspaket gefunden. Bitte -PackageZip oder -SourceRoot angeben."
}

function Copy-ApplicationToTarget {
  param(
    [string]$ResolvedSourceRoot,
    [string]$ResolvedTargetRoot
  )

  if ((Test-Path -Path $ResolvedTargetRoot) -and ((Resolve-Path -Path $ResolvedTargetRoot).Path -eq (Resolve-Path -Path $ResolvedSourceRoot).Path)) {
    Write-InstallerLog -Message "SourceRoot and AppRoot are identical. Copy step skipped."
    return
  }

  if (Test-Path -Path $ResolvedTargetRoot) {
    Write-InstallerLog -Message ("Target folder already exists: " + $ResolvedTargetRoot) -Level WARN
    if (-not $OverwriteExisting) {
      $OverwriteExisting = Ask-YesNo -Prompt "Der Zielordner '$ResolvedTargetRoot' existiert bereits. Soll er ersetzt werden?" -Default $false
    }

    if (-not $OverwriteExisting) {
      throw "Installation abgebrochen, weil der Zielordner bereits existiert."
    }

    Remove-Item -Path $ResolvedTargetRoot -Recurse -Force
    Write-InstallerLog -Message "Existing target folder removed."
  }

  $targetParent = Split-Path -Parent $ResolvedTargetRoot
  if (-not (Test-Path -Path $targetParent)) {
    New-Item -Path $targetParent -ItemType Directory -Force | Out-Null
  }

  Write-Host "Kopiere Anwendung nach $ResolvedTargetRoot ..." -ForegroundColor Cyan
  Write-InstallerLog -Message ("Copying application from " + $ResolvedSourceRoot + " to " + $ResolvedTargetRoot)
  Copy-Item -Path $ResolvedSourceRoot -Destination $ResolvedTargetRoot -Recurse -Force
}

function Ensure-RuntimeFiles {
  param([string]$ResolvedAppRoot)

  $requiredPaths = @(
    "package.json",
    "dist\agent-main.js",
    "dist\web-main.js",
    "dist\updater-main.js",
    "scripts\windows\install-agent-service.ps1"
  )

  foreach ($relativePath in $requiredPaths) {
    $fullPath = Join-Path $ResolvedAppRoot $relativePath
    if (-not (Test-Path -Path $fullPath)) {
      throw "Erforderliche Installationsdatei fehlt: $fullPath"
    }
  }
}

function Ensure-NodeModules {
  param([string]$ResolvedAppRoot)

  $nodeModulesPath = Join-Path $ResolvedAppRoot "node_modules"
  if (Test-Path -Path $nodeModulesPath) {
    Write-InstallerLog -Message "node_modules found in customer package."
    return
  }

  if (-not $InstallDependencies) {
    throw "node_modules fehlt. Release-Paket mit node_modules erstellen oder Installer mit -InstallDependencies starten."
  }

  $packageLockPath = Join-Path $ResolvedAppRoot "package-lock.json"
  if (-not (Test-Path -Path $packageLockPath)) {
    throw "package-lock.json fehlt. Abhaengigkeiten koennen nicht reproduzierbar installiert werden."
  }

  Write-Host "Installiere Node.js-Abhaengigkeiten mit npm ci --omit=dev ..." -ForegroundColor Cyan
  Push-Location $ResolvedAppRoot
  try {
    & npm ci --omit=dev
    if ($LASTEXITCODE -ne 0) {
      throw "npm ci --omit=dev ist fehlgeschlagen (Exit-Code $LASTEXITCODE)."
    }
  }
  finally {
    Pop-Location
  }
}

function Ensure-EnvFile {
  param([string]$ResolvedAppRoot)

  $envPath = Join-Path $ResolvedAppRoot ".env"
  if (-not (Test-Path -Path $envPath)) {
    $envExamplePath = Join-Path $ResolvedAppRoot ".env.example"
    if (Test-Path -Path $envExamplePath) {
      Copy-Item -Path $envExamplePath -Destination $envPath -Force
      Write-InstallerLog -Message ".env created from .env.example."
    }
    else {
      New-Item -Path $envPath -ItemType File -Force | Out-Null
      Write-InstallerLog -Message "Empty .env created because .env.example was not found."
    }
  }

  if ($PromptForEnv) {
    Write-Host "Oeffne .env zur Konfiguration. Speichern und Notepad schliessen, um fortzufahren." -ForegroundColor Yellow
    Start-Process -FilePath "notepad.exe" -ArgumentList $envPath -Wait
  }
}

function Install-NssmServices {
  param([string]$ResolvedAppRoot)

  $serviceInstaller = Join-Path $ResolvedAppRoot "scripts\windows\install-agent-service.ps1"
  $arguments = @{
    AppRoot = $ResolvedAppRoot
    ServiceName = $ServiceName
    WebServiceName = $WebServiceName
    UpdaterServiceName = $UpdaterServiceName
    InstallRoles = $InstallRoles
    WebUiPort = $WebUiPort
    SchedulerIntervalMs = $SchedulerIntervalMs
    UpdateCheckIntervalMs = $UpdateCheckIntervalMs
    NonInteractive = $true
  }

  if ($NodeExePath -and $NodeExePath.Trim()) {
    $arguments.NodeExePath = $NodeExePath.Trim()
  }

  if ($ForceRecreateServices) {
    $arguments.ForceRecreate = $true
  }

  Write-Host "Erzeuge und starte NSSM-Dienste ..." -ForegroundColor Cyan
  Write-InstallerLog -Message "Starting NSSM service installation."
  & $serviceInstaller @arguments
  if ($LASTEXITCODE -ne 0) {
    throw "NSSM-Dienstinstallation fehlgeschlagen (Exit-Code $LASTEXITCODE)."
  }
}

Initialize-InstallerLog
Write-InstallerLog -Message ("New customer installer started. Log file: " + $script:InstallerLogFile)

try {
  Ensure-Elevated -BoundParameters $PSBoundParameters

  $resolvedSourceRoot = Resolve-SourceRoot -PackageZipPath $PackageZip -ProvidedSourceRoot $SourceRoot
  $resolvedTargetRoot = $AppRoot
  Write-InstallerLog -Message ("Resolved source root: " + $resolvedSourceRoot)
  Write-InstallerLog -Message ("Resolved target root: " + $resolvedTargetRoot)

  Copy-ApplicationToTarget -ResolvedSourceRoot $resolvedSourceRoot -ResolvedTargetRoot $resolvedTargetRoot
  Ensure-RuntimeFiles -ResolvedAppRoot $resolvedTargetRoot
  Ensure-NodeModules -ResolvedAppRoot $resolvedTargetRoot
  Ensure-EnvFile -ResolvedAppRoot $resolvedTargetRoot
  Install-NssmServices -ResolvedAppRoot $resolvedTargetRoot

  Write-InstallerLog -Message "New customer installation completed successfully."
  Write-Host "Neue Kundeninstallation erfolgreich abgeschlossen." -ForegroundColor Green
  Write-Host "AppRoot: $resolvedTargetRoot"
  Write-Host "Dienste: $InstallRoles"
  Write-Host "Web UI: http://localhost:$WebUiPort"
}
catch {
  Write-InstallerLog -Level ERROR -Message ("New customer installer failed: " + $_.Exception.Message)
  if ($_.ScriptStackTrace) {
    Write-InstallerLog -Level ERROR -Message ("StackTrace: " + $_.ScriptStackTrace)
  }
  throw
}
finally {
  if ($script:TempExtractRoot -and (Test-Path -Path $script:TempExtractRoot)) {
    Write-InstallerLog -Message ("Removing temporary extract folder: " + $script:TempExtractRoot)
    Remove-Item -Path $script:TempExtractRoot -Recurse -Force
  }

  Write-InstallerLog -Message "New customer installer finished."
}
