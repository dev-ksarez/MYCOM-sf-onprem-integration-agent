param(
  [string]$AppRoot,
  [string]$OutputDir,
  [switch]$IncludeNodeModules
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

function Ensure-Directory {
  param([string]$Path)

  if (-not (Test-Path $Path)) {
    New-Item -ItemType Directory -Path $Path | Out-Null
  }
}

$appRootResolved = Resolve-AppRoot -InputPath $AppRoot
$packageJsonPath = Join-Path $appRootResolved "package.json"
$distPath = Join-Path $appRootResolved "dist"

if (-not (Test-Path $packageJsonPath)) {
  throw "package.json not found at $packageJsonPath"
}

if (-not (Test-Path $distPath)) {
  throw "dist directory not found at $distPath. Run 'npm run build' first."
}

if (-not $OutputDir -or -not $OutputDir.Trim()) {
  $OutputDir = Join-Path $appRootResolved "artifacts"
}

Ensure-Directory -Path $OutputDir

$pkg = Get-Content -Path $packageJsonPath -Raw | ConvertFrom-Json
$version = [string]$pkg.version
if (-not $version) {
  throw "Version missing in package.json"
}

$stagingRoot = Join-Path $env:TEMP ("sf-agent-customer-package-" + [guid]::NewGuid().ToString("N"))
$stagingAppRoot = Join-Path $stagingRoot "sf-onprem-integration-agent"
Ensure-Directory -Path $stagingAppRoot

Write-Host "Staging package at: $stagingAppRoot" -ForegroundColor Cyan

Copy-Item -Path (Join-Path $appRootResolved "dist") -Destination (Join-Path $stagingAppRoot "dist") -Recurse -Force
Copy-Item -Path (Join-Path $appRootResolved "scripts") -Destination (Join-Path $stagingAppRoot "scripts") -Recurse -Force
Copy-Item -Path (Join-Path $appRootResolved "salesforce") -Destination (Join-Path $stagingAppRoot "salesforce") -Recurse -Force
Copy-Item -Path (Join-Path $appRootResolved "package.json") -Destination (Join-Path $stagingAppRoot "package.json") -Force

$packageLockPath = Join-Path $appRootResolved "package-lock.json"
if (Test-Path $packageLockPath) {
  Copy-Item -Path $packageLockPath -Destination (Join-Path $stagingAppRoot "package-lock.json") -Force
}

$envExamplePath = Join-Path $appRootResolved ".env.example"
if (Test-Path $envExamplePath) {
  Copy-Item -Path $envExamplePath -Destination (Join-Path $stagingAppRoot ".env.example") -Force
}

$deploymentGuidePath = Join-Path $appRootResolved "WINDOWS_DEPLOYMENT.md"
if (Test-Path $deploymentGuidePath) {
  Copy-Item -Path $deploymentGuidePath -Destination (Join-Path $stagingAppRoot "WINDOWS_DEPLOYMENT.md") -Force
}

if ($IncludeNodeModules) {
  $nodeModulesPath = Join-Path $appRootResolved "node_modules"
  if (-not (Test-Path $nodeModulesPath)) {
    throw "-IncludeNodeModules was set, but node_modules is missing at $nodeModulesPath"
  }

  Write-Host "Including node_modules in package..." -ForegroundColor Yellow
  Copy-Item -Path $nodeModulesPath -Destination (Join-Path $stagingAppRoot "node_modules") -Recurse -Force
}

$zipName = "sf-onprem-integration-agent-customer-installer-$version.zip"
$zipPath = Join-Path $OutputDir $zipName
if (Test-Path $zipPath) {
  Remove-Item -Path $zipPath -Force
}

Compress-Archive -Path $stagingAppRoot -DestinationPath $zipPath -CompressionLevel Optimal

Write-Host "Package created: $zipPath" -ForegroundColor Green
if (-not $IncludeNodeModules) {
  Write-Host "Note: node_modules is not included. Customer must run 'npm ci --omit=dev'." -ForegroundColor Yellow
}

Remove-Item -Path $stagingRoot -Recurse -Force
