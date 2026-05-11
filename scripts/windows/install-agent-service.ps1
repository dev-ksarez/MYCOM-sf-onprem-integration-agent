param(
  [string]$ServiceName = "SfOnpremIntegrationAgent",
  [string]$WebServiceName = "SfOnpremIntegrationWeb",
  [string]$UpdaterServiceName = "SfOnpremIntegrationUpdater",
  [string]$NodeExePath,
  [string]$DisplayName = "SF OnPrem Integration Agent",
  [string]$Description = "Runs the Salesforce On-Prem Integration Agent scheduler runtime",
  [string]$WebDisplayName = "SF OnPrem Integration Web",
  [string]$WebDescription = "Runs the Salesforce On-Prem Integration Web dashboard",
  [string]$UpdaterDisplayName = "SF OnPrem Integration Updater",
  [string]$UpdaterDescription = "Runs the Salesforce On-Prem Integration AutoUpdater service",
  [string]$AppRoot,
  [string]$InstallRoles = "agent,web,updater",
  [int]$WebUiPort = 8080,
  [int]$SchedulerIntervalMs = 60000,
  [int]$UpdateCheckIntervalMs = 900000,
  [switch]$NonInteractive,
  [switch]$ForceRecreate
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$script:ScriptDirectory = Split-Path -Parent $PSCommandPath
$script:InstallProfileFile = $null

function Resolve-AppRoot {
  param([string]$InputPath)

  if ($InputPath -and $InputPath.Trim()) {
    $candidate = $InputPath.Trim()
    if (-not (Test-Path -Path $candidate)) {
      throw "AppRoot path not found: $candidate"
    }

    return (Resolve-Path -Path $candidate).Path
  }

  return (Resolve-Path -Path (Join-Path $script:ScriptDirectory "..\..\")).Path
}

function Ask-YesNo {
  param(
    [string]$Prompt,
    [bool]$Default = $false
  )

  if ($NonInteractive) {
    return $Default
  }

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
  param([string]$ResolvedAppRoot)

  $rootCandidate = Join-Path $ResolvedAppRoot "nssm.exe"
  if (Test-Path -Path $rootCandidate) {
    return $rootCandidate
  }

  $nssmCommand = Get-Command nssm -ErrorAction SilentlyContinue
  if ($nssmCommand -and $nssmCommand.Source) {
    return $nssmCommand.Source
  }

  foreach ($candidate in @(
    "C:\ProgramData\chocolatey\bin\nssm.exe",
    "C:\Windows\System32\nssm.exe"
  )) {
    if (Test-Path -Path $candidate) {
      return $candidate
    }
  }

  return $null
}

function Resolve-RoleSelection {
  param([string]$RawRoles)

  $rawRolesValue = if ($null -ne $RawRoles) { [string]$RawRoles } else { "" }
  $normalized = New-Object System.Collections.Generic.List[string]
  foreach ($part in $rawRolesValue.Split(",")) {
    $candidate = [string]$part
    if ($null -eq $candidate) {
      continue
    }

    $candidate = $candidate.Trim().ToLowerInvariant()
    if ($candidate) {
      [void]$normalized.Add($candidate)
    }
  }

  if (-not $normalized.Count) {
    $normalized = @("agent", "web", "updater")
  }

  if ($normalized -contains "all") {
    return @("agent", "web", "updater")
  }

  $validRoles = @("agent", "web", "updater")
  $invalid = @($normalized | Where-Object { $_ -notin $validRoles })
  if ($invalid.Count -gt 0) {
    throw "Invalid InstallRoles value(s): $($invalid -join ', '). Valid roles are: agent, web, updater, all."
  }

  return @($normalized | Select-Object -Unique)
}

function Write-InstallProfile {
  param(
    [string]$ResolvedAppRoot,
    [string[]]$Roles
  )

  $profilePath = Join-Path $ResolvedAppRoot "artifacts\runtime\install-profile.json"
  $profileDir = Split-Path -Parent $profilePath
  if (-not (Test-Path $profileDir)) {
    New-Item -Path $profileDir -ItemType Directory -Force | Out-Null
  }

  $payload = [ordered]@{
    updatedAt = [DateTime]::UtcNow.ToString("o")
    roles = $Roles
    services = @{
      agent = @{
        enabled = $Roles -contains "agent"
        name = $ServiceName
      }
      web = @{
        enabled = $Roles -contains "web"
        name = $WebServiceName
      }
      updater = @{
        enabled = $Roles -contains "updater"
        name = $UpdaterServiceName
      }
    }
    settings = @{
      webUiPort = $WebUiPort
      schedulerIntervalMs = $SchedulerIntervalMs
      updateCheckIntervalMs = $UpdateCheckIntervalMs
    }
  }

  $payload | ConvertTo-Json -Depth 6 | Set-Content -Path $profilePath -Encoding UTF8
}

function Ensure-ServiceRemoved {
  param([string]$Name)

  $existing = Get-Service -Name $Name -ErrorAction SilentlyContinue
  if (-not $existing) {
    return
  }

  if ($existing.Status -ne "Stopped") {
    Stop-Service -Name $Name -Force -ErrorAction SilentlyContinue
    [void](Wait-ServiceState -Name $Name -ExpectedState "Stopped" -TimeoutSeconds 60)
  }

  [void](Invoke-Sc -Arguments @("delete", $Name) -Action "delete")
  Start-Sleep -Seconds 2
}

function Install-NssmService {
  param(
    [string]$NssmExe,
    [string]$Name,
    [string]$Display,
    [string]$ServiceDescription,
    [string]$NodeExe,
    [string]$EntryPoint,
    [string]$ResolvedAppRoot,
    [string]$StdoutLog,
    [string]$StderrLog,
    [string]$EnvironmentExtra
  )

  & $NssmExe install $Name $NodeExe $EntryPoint | Out-Null
  if ($LASTEXITCODE -ne 0) {
    throw "nssm install failed for service '$Name' (exit code $LASTEXITCODE)."
  }

  & $NssmExe set $Name DisplayName $Display | Out-Null
  & $NssmExe set $Name Description $ServiceDescription | Out-Null
  & $NssmExe set $Name AppDirectory $ResolvedAppRoot | Out-Null
  & $NssmExe set $Name AppEnvironmentExtra $EnvironmentExtra | Out-Null
  & $NssmExe set $Name AppStdout $StdoutLog | Out-Null
  & $NssmExe set $Name AppStderr $StderrLog | Out-Null
  & $NssmExe set $Name AppRotateFiles 1 | Out-Null
  & $NssmExe set $Name AppRotateOnline 1 | Out-Null
  & $NssmExe set $Name Start SERVICE_AUTO_START | Out-Null
  [void](Invoke-Sc -Arguments @("failure", $Name, "reset=", "86400", "actions=", "restart/5000/restart/15000/restart/30000") -Action "failure")
}

$appRootResolved = Resolve-AppRoot -InputPath $AppRoot
$agentEntryPoint = Join-Path $appRootResolved "dist\agent-main.js"
$webEntryPoint = Join-Path $appRootResolved "dist\web-main.js"
$updaterEntryPoint = Join-Path $appRootResolved "dist\updater-main.js"
$selectedRoles = Resolve-RoleSelection -RawRoles $InstallRoles
$nodeExe = $null
if ($NodeExePath -and $NodeExePath.Trim() -and (Test-Path -Path $NodeExePath)) {
  $nodeExe = (Resolve-Path -Path $NodeExePath).Path
} else {
  $nodeCommand = Get-Command node -ErrorAction SilentlyContinue
  $nodeExe = if ($nodeCommand) { $nodeCommand.Source } else { $null }
}
$nssmExe = Resolve-NssmExe -ResolvedAppRoot $appRootResolved

if (-not $nodeExe) {
  throw "Node.js was not found in PATH. Install Node.js 22+ and retry."
}

if (-not $nssmExe) {
  throw "NSSM (nssm.exe) is required to run Node.js as a Windows service. Ensure nssm.exe is in project root or install NSSM and ensure 'nssm' is in PATH."
}

if (-not (Test-IsElevated)) {
  throw "Administrator rights are required to install Windows services. Start PowerShell as Administrator and retry."
}

foreach ($requiredPath in @(
  @{ role = "agent"; path = $agentEntryPoint },
  @{ role = "web"; path = $webEntryPoint },
  @{ role = "updater"; path = $updaterEntryPoint }
)) {
  if (($selectedRoles -contains $requiredPath.role) -and (-not (Test-Path -Path $requiredPath.path))) {
    throw "Entry point not found: $($requiredPath.path). Run 'npm run build' before installing the services."
  }
}

$logsDir = Join-Path $appRootResolved "logs"
if (-not (Test-Path -Path $logsDir)) {
  New-Item -Path $logsDir -ItemType Directory -Force | Out-Null
}

$services = @(
  @{
    Role = "agent"
    Name = $ServiceName
    Display = $DisplayName
    Description = $Description
    EntryPoint = $agentEntryPoint
    Stdout = (Join-Path $logsDir "agent-service.out.log")
    Stderr = (Join-Path $logsDir "agent-service.err.log")
    Environment = "ROLE=agent SCHEDULER_INTERVAL_MS=$SchedulerIntervalMs"
  },
  @{
    Role = "web"
    Name = $WebServiceName
    Display = $WebDisplayName
    Description = $WebDescription
    EntryPoint = $webEntryPoint
    Stdout = (Join-Path $logsDir "web-service.out.log")
    Stderr = (Join-Path $logsDir "web-service.err.log")
    Environment = "ROLE=web WEB_UI_PORT=$WebUiPort"
  },
  @{
    Role = "updater"
    Name = $UpdaterServiceName
    Display = $UpdaterDisplayName
    Description = $UpdaterDescription
    EntryPoint = $updaterEntryPoint
    Stdout = (Join-Path $logsDir "updater-service.out.log")
    Stderr = (Join-Path $logsDir "updater-service.err.log")
    Environment = "ROLE=updater UPDATE_CHECK_INTERVAL_MS=$UpdateCheckIntervalMs AGENT_SERVICE_NAME=$ServiceName WEB_SERVICE_NAME=$WebServiceName UPDATER_SERVICE_NAME=$UpdaterServiceName AUTO_UPDATER_ENABLED=1"
  }
)
$servicesToInstall = @($services | Where-Object { $selectedRoles -contains [string]$_.Role })

Write-Host "Windows service configuration:" -ForegroundColor Cyan
Write-Host "  Roles           : $($selectedRoles -join ', ')"
Write-Host "  Agent service   : $ServiceName"
Write-Host "  Web service     : $WebServiceName"
Write-Host "  Updater service : $UpdaterServiceName"
Write-Host "  AppRoot         : $appRootResolved"
Write-Host "  Node.exe        : $nodeExe"
Write-Host "  NSSM.exe        : $nssmExe"
Write-Host "  WEB_UI_PORT     : $WebUiPort"
Write-Host "  SCHED_INTMS     : $SchedulerIntervalMs"
Write-Host "  UPDATE_INTMS    : $UpdateCheckIntervalMs"

if (-not $NonInteractive) {
  if (-not (Ask-YesNo -Prompt "Continue with service installation?" -Default $true)) {
    Write-Host "Aborted."
    exit 1
  }
}

foreach ($service in $services) {
  if ($selectedRoles -notcontains [string]$service.Role) {
    continue
  }
  $existing = Get-Service -Name $service.Name -ErrorAction SilentlyContinue
  if ($existing -and -not $ForceRecreate -and -not $NonInteractive) {
    if (-not (Ask-YesNo -Prompt "Service '$($service.Name)' already exists. Recreate it?" -Default $true)) {
      continue
    }
  }

  if ($existing -or $ForceRecreate) {
    Ensure-ServiceRemoved -Name $service.Name
  }

  Install-NssmService `
    -NssmExe $nssmExe `
    -Name $service.Name `
    -Display $service.Display `
    -ServiceDescription $service.Description `
    -NodeExe $nodeExe `
    -EntryPoint $service.EntryPoint `
    -ResolvedAppRoot $appRootResolved `
    -StdoutLog $service.Stdout `
    -StderrLog $service.Stderr `
    -EnvironmentExtra $service.Environment
}

foreach ($serviceName in @($servicesToInstall | ForEach-Object { $_.Name })) {
  $service = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
  if (-not $service) {
    throw "Service '$serviceName' was not created."
  }

  if ($service.Status -ne "Running") {
    Start-Service -Name $serviceName
    if (-not (Wait-ServiceState -Name $serviceName -ExpectedState "Running" -TimeoutSeconds 30)) {
      throw "Service '$serviceName' failed to reach Running state."
    }
  }
}

Write-InstallProfile -ResolvedAppRoot $appRootResolved -Roles $selectedRoles

Write-Host "Services installed and running:" -ForegroundColor Green
foreach ($serviceName in @($servicesToInstall | ForEach-Object { $_.Name })) {
  Write-Host "  $serviceName"
}
