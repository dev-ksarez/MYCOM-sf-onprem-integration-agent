param(
  [string]$ServiceName = "SfOnpremIntegrationAgent",
  [string]$DisplayName = "SF OnPrem Integration Agent",
  [string]$Description = "Runs the Salesforce On-Prem Integration Agent",
  [string]$AppRoot,
  [int]$WebUiPort = 8080,
  [int]$SchedulerIntervalMs = 60000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-AppRoot {
  param([string]$InputPath)

  if ($InputPath -and $InputPath.Trim()) {
    $candidate = $InputPath.Trim()
    if (-not (Test-Path -Path $candidate)) {
      throw "AppRoot path not found: $candidate"
    }

    return (Resolve-Path -Path $candidate).Path
  }

  $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
  return (Resolve-Path -Path (Join-Path $scriptDir "..\..\")).Path
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

function Test-IsElevated {
  $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
  $principal = New-Object Security.Principal.WindowsPrincipal($identity)
  return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Invoke-Sc {
  param(
    [string[]]$Arguments,
    [string]$Action
  )

  $output = & sc.exe @Arguments 2>&1
  if ($LASTEXITCODE -ne 0) {
    $message = ($output | Out-String).Trim()
    throw "sc.exe $Action failed (exit code $LASTEXITCODE): $message"
  }

  return $output
}

function Resolve-NssmExe {
  param([string]$AppRoot)

  # Check project root first
  if ($AppRoot) {
    $rootCandidate = Join-Path $AppRoot "nssm.exe"
    if (Test-Path -Path $rootCandidate) {
      return $rootCandidate
    }
  }

  # Check if nssm is in PATH
  $nssmCommand = Get-Command nssm -ErrorAction SilentlyContinue
  if ($nssmCommand -and $nssmCommand.Source) {
    return $nssmCommand.Source
  }

  # Check common installation locations
  $commonCandidates = @(
    "C:\\ProgramData\\chocolatey\\bin\\nssm.exe",
    "C:\\Windows\\System32\\nssm.exe"
  )

  foreach ($candidate in $commonCandidates) {
    if (Test-Path -Path $candidate) {
      return $candidate
    }
  }

  return $null
}

$appRootResolved = Resolve-AppRoot -InputPath $AppRoot
$entryPoint = Join-Path $appRootResolved "dist\main.js"
$nodeCommand = Get-Command node -ErrorAction SilentlyContinue
$nodeExe = if ($nodeCommand) { $nodeCommand.Source } else { $null }

if (-not $nodeExe) {
  throw "Node.js was not found in PATH. Install Node.js 22+ and retry."
}

$nssmExe = Resolve-NssmExe -AppRoot $appRootResolved
if (-not $nssmExe) {
  throw "NSSM (nssm.exe) is required to run Node.js as a Windows service. Ensure nssm.exe is in project root or install NSSM and ensure 'nssm' is in PATH."
}

if (-not (Test-IsElevated)) {
  throw "Administrator rights are required to install a Windows service. Start PowerShell as Administrator and retry."
}

if (-not (Test-Path -Path $entryPoint)) {
  throw "Entry point not found: $entryPoint. Run 'npm run build' before installing the service."
}

Write-Host "Service installer configuration:" -ForegroundColor Cyan
Write-Host "  ServiceName : $ServiceName"
Write-Host "  DisplayName : $DisplayName"
Write-Host "  AppRoot     : $appRootResolved"
Write-Host "  Node.exe    : $nodeExe"
Write-Host "  EntryPoint  : $entryPoint"
Write-Host "  NSSM.exe    : $nssmExe"
Write-Host "  WEB_UI_PORT : $WebUiPort"
Write-Host "  SCHED_INTMS : $SchedulerIntervalMs"

if (-not (Ask-YesNo -Prompt "Continue with installation?" -Default $true)) {
  Write-Host "Aborted."
  exit 1
}

$existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($existing) {
  if (-not (Ask-YesNo -Prompt "Service '$ServiceName' already exists. Recreate it?" -Default $false)) {
    Write-Host "No changes applied."
    exit 0
  }

  if ($existing.Status -ne "Stopped") {
    Stop-Service -Name $ServiceName -Force
    [void](Wait-ServiceState -Name $ServiceName -ExpectedState "Stopped" -TimeoutSeconds 60)
  }

  [void](Invoke-Sc -Arguments @("delete", $ServiceName) -Action "delete")
  Start-Sleep -Seconds 2
}

$logsDir = Join-Path $appRootResolved "logs"
if (-not (Test-Path -Path $logsDir)) {
  New-Item -Path $logsDir -ItemType Directory -Force | Out-Null
}

$stdoutLog = Join-Path $logsDir "agent-service.out.log"
$stderrLog = Join-Path $logsDir "agent-service.err.log"

& $nssmExe install $ServiceName $nodeExe $entryPoint | Out-Null
if ($LASTEXITCODE -ne 0) {
  throw "nssm install failed (exit code $LASTEXITCODE)."
}

& $nssmExe set $ServiceName DisplayName $DisplayName | Out-Null
& $nssmExe set $ServiceName Description $Description | Out-Null
& $nssmExe set $ServiceName AppDirectory $appRootResolved | Out-Null
& $nssmExe set $ServiceName AppEnvironmentExtra WEB_UI_ENABLED=1 | Out-Null
& $nssmExe set $ServiceName AppEnvironmentExtra WEB_UI_PORT=$WebUiPort | Out-Null
& $nssmExe set $ServiceName AppEnvironmentExtra SCHEDULER_INTERVAL_MS=$SchedulerIntervalMs | Out-Null
& $nssmExe set $ServiceName AppStdout $stdoutLog | Out-Null
& $nssmExe set $ServiceName AppStderr $stderrLog | Out-Null
& $nssmExe set $ServiceName AppRotateFiles 1 | Out-Null
& $nssmExe set $ServiceName AppRotateOnline 1 | Out-Null
& $nssmExe set $ServiceName Start SERVICE_AUTO_START | Out-Null

[void](Invoke-Sc -Arguments @("failure", $ServiceName, "reset=", "86400", "actions=", "restart/5000/restart/15000/restart/30000") -Action "failure")

$created = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if (-not $created) {
  throw "Service '$ServiceName' was not created. Verify Administrator rights and run the installer again."
}

try {
  Start-Service -Name $ServiceName
} catch {
  $queryDetails = & sc.exe queryex $ServiceName 2>&1
  $queryText = ($queryDetails | Out-String).Trim()
  throw "Failed to start service '$ServiceName'. sc queryex output:`n$queryText`nOriginal error: $($_.Exception.Message)"
}

if (-not (Wait-ServiceState -Name $ServiceName -ExpectedState "Running" -TimeoutSeconds 30)) {
  throw "Service '$ServiceName' failed to reach Running state."
}

Write-Host "Service '$ServiceName' installed and running." -ForegroundColor Green
