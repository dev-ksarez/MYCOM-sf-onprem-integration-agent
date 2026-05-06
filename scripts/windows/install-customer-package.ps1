param(
  [string]$PackageZip,
  [string]$SourceRoot,
  [string]$AppRoot = "C:\apps\sf-onprem-integration-agent",
  [string]$LogFile,
  [switch]$OverwriteExisting,
  [switch]$InstallDependencies,
  [switch]$PromptForEnv,
  [switch]$RunInitialSetup,
  [switch]$ActivateInitialSchedules,
  [ValidateSet("SAGE100")]
  [string]$InitialSetupMode = "SAGE100"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$tempExtractRoot = $null
$script:InstallerLogFile = if ($LogFile -and $LogFile.Trim()) { $LogFile.Trim() } else { Join-Path $env:TEMP "sf-onprem-integration-agent-installer.log" }
$script:ScriptDirectory = Split-Path -Parent $PSCommandPath

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
    Write-InstallerLog -Message "Bootstrap installer already running with administrator rights."
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
  Write-InstallerLog -Message "Requesting administrator rights via UAC for bootstrap installer."
  try {
    Start-Process -FilePath "powershell.exe" -Verb RunAs -ArgumentList $argumentString -ErrorAction Stop | Out-Null
    Write-InstallerLog -Message "UAC relaunch for bootstrap installer started successfully."
  }
  catch {
    Write-InstallerLog -Level ERROR -Message ("UAC relaunch failed: " + $_.Exception.Message)
    throw
  }
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
    return (Resolve-Path -Path $ProvidedSourceRoot.Trim()).Path
  }

  if ($PackageZipPath -and $PackageZipPath.Trim()) {
    $zipResolved = (Resolve-Path -Path $PackageZipPath.Trim()).Path
    $extractRoot = Join-Path $env:TEMP ("sf-agent-package-extract-" + [guid]::NewGuid().ToString("N"))
    $script:tempExtractRoot = $extractRoot
    Expand-Archive -Path $zipResolved -DestinationPath $extractRoot -Force
    $candidate = Get-ChildItem -Path $extractRoot -Directory -Recurse | Where-Object {
      Test-Path (Join-Path $_.FullName "package.json")
    } | Select-Object -First 1

    if (-not $candidate) {
      throw "No application folder with package.json found in package zip."
    }

    return $candidate.FullName
  }

  $scriptDir = $script:ScriptDirectory
  $localCandidate = Join-Path $scriptDir "sf-onprem-integration-agent"
  if (Test-Path -Path (Join-Path $localCandidate "package.json")) {
    return $localCandidate
  }

  if (Test-Path -Path (Join-Path $scriptDir "package.json")) {
    return $scriptDir
  }

  throw "No application source found. Provide -PackageZip or -SourceRoot."
}

Initialize-InstallerLog
Write-InstallerLog -Message ("Bootstrap installer started. Log file: " + $script:InstallerLogFile)
Write-InstallerLog -Message ("Current process elevated: " + (Test-IsElevated))

try {
  Ensure-Elevated -BoundParameters $PSBoundParameters

  $resolvedSourceRoot = Resolve-SourceRoot -PackageZipPath $PackageZip -ProvidedSourceRoot $SourceRoot
  $resolvedTargetRoot = $AppRoot
  Write-InstallerLog -Message ("Resolved source root: " + $resolvedSourceRoot)
  Write-InstallerLog -Message ("Resolved target root: " + $resolvedTargetRoot)

  if (Test-Path -Path $resolvedTargetRoot) {
    Write-InstallerLog -Message ("Target folder already exists: " + $resolvedTargetRoot) -Level WARN
    if (-not $OverwriteExisting) {
      $OverwriteExisting = Ask-YesNo -Prompt "Der Zielordner '$resolvedTargetRoot' existiert bereits. Soll er jetzt ersetzt werden?" -Default $false
    }

    if (-not $OverwriteExisting) {
      Write-InstallerLog -Level WARN -Message "Installation aborted because target folder overwrite was declined."
      throw "Installation abgebrochen, weil der Zielordner bereits existiert."
    }

    Remove-Item -Path $resolvedTargetRoot -Recurse -Force
    Write-InstallerLog -Message "Existing target folder removed."
  }

  $targetParent = Split-Path -Parent $resolvedTargetRoot
  if (-not (Test-Path -Path $targetParent)) {
    New-Item -Path $targetParent -ItemType Directory -Force | Out-Null
    Write-InstallerLog -Message ("Created target parent directory: " + $targetParent)
  }

  Write-Host "Kopiere das Installationspaket nach $resolvedTargetRoot ..." -ForegroundColor Cyan
  Write-InstallerLog -Message "Copying package to target root."
  Copy-Item -Path $resolvedSourceRoot -Destination $resolvedTargetRoot -Recurse -Force

  $installerScript = Join-Path $resolvedTargetRoot "scripts\windows\install-windows-agent.ps1"
  if (-not (Test-Path -Path $installerScript)) {
    Write-InstallerLog -Level ERROR -Message ("Nested installer script missing after copy: " + $installerScript)
    throw "Das Installationsskript wurde nach dem Kopieren nicht gefunden: $installerScript"
  }
  Write-InstallerLog -Message ("Nested installer script found: " + $installerScript)

  $effectivePromptForEnv = $PromptForEnv
  if (-not $effectivePromptForEnv) {
    $effectivePromptForEnv = $true
  }

  Write-Host "Starte die eigentliche Agent-Installation ..." -ForegroundColor Cyan
  Write-InstallerLog -Message "Starting nested Windows agent installer."
  & $installerScript `
    -AppRoot $resolvedTargetRoot `
    -LogFile $script:InstallerLogFile `
    -InstallDependencies:$InstallDependencies `
    -PromptForEnv:$effectivePromptForEnv `
    -RunInitialSetup:$RunInitialSetup `
    -InitialSetupMode $InitialSetupMode `
    -ActivateInitialSchedules:$ActivateInitialSchedules

  if ($LASTEXITCODE -ne 0) {
    Write-InstallerLog -Level ERROR -Message ("Nested installer exited with code " + $LASTEXITCODE)
    throw "Die Agent-Installation ist fehlgeschlagen (Exit-Code $LASTEXITCODE)."
  }

  Write-InstallerLog -Message "Bootstrap installer completed successfully."
  Write-Host "Die Kundeninstallation wurde erfolgreich abgeschlossen." -ForegroundColor Green
}
catch {
  Write-InstallerLog -Level ERROR -Message ("Bootstrap installer failed: " + $_.Exception.Message)
  if ($_.ScriptStackTrace) {
    Write-InstallerLog -Level ERROR -Message ("StackTrace: " + $_.ScriptStackTrace)
  }
  throw
}
finally {
  if ($tempExtractRoot -and (Test-Path -Path $tempExtractRoot)) {
    Write-InstallerLog -Message ("Removing temporary extract folder: " + $tempExtractRoot)
    Remove-Item -Path $tempExtractRoot -Recurse -Force
  }

  Write-InstallerLog -Message "Bootstrap installer finished."
}