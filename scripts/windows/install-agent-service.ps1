param(
  [string]$ServiceName = "SfOnpremIntegrationAgent",
  [string]$DisplayName = "SF OnPrem Integration Agent",
  [string]$Description = "Runs the Salesforce On-Prem Integration Agent",
  [string]$AppRoot
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

$appRootResolved = Resolve-AppRoot -InputPath $AppRoot
$entryPoint = Join-Path $appRootResolved "dist\main.js"
$nodeExe = (Get-Command node -ErrorAction SilentlyContinue)?.Source

if (-not $nodeExe) {
  throw "Node.js was not found in PATH. Install Node.js 22+ and retry."
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

  & sc.exe delete $ServiceName | Out-Null
  Start-Sleep -Seconds 2
}

$binPath = '"{0}" "{1}"' -f $nodeExe, $entryPoint

& sc.exe create $ServiceName binPath= $binPath start= auto DisplayName= "$DisplayName" | Out-Null
& sc.exe description $ServiceName "$Description" | Out-Null
& sc.exe failure $ServiceName reset= 86400 actions= restart/5000/restart/15000/restart/30000 | Out-Null

Start-Service -Name $ServiceName

if (-not (Wait-ServiceState -Name $ServiceName -ExpectedState "Running" -TimeoutSeconds 30)) {
  throw "Service '$ServiceName' failed to reach Running state."
}

Write-Host "Service '$ServiceName' installed and running." -ForegroundColor Green
