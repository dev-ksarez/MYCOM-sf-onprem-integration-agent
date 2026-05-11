param(
  [string]$AppRoot,
  [string]$OutputDir,
  [switch]$IncludeNodeModules
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$script:ScriptDirectory = Split-Path -Parent $PSCommandPath

function Resolve-AppRoot {
  param([string]$InputPath)

  if ($InputPath -and $InputPath.Trim()) {
    return (Resolve-Path -Path $InputPath).Path
  }

  $scriptDir = $script:ScriptDirectory
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
$packageRoot = Join-Path $stagingRoot ("sf-onprem-integration-agent-customer-installer-" + $version)
$stagingAppRoot = Join-Path $packageRoot "sf-onprem-integration-agent"
Ensure-Directory -Path $packageRoot
Ensure-Directory -Path $stagingAppRoot

Write-Host "Staging package at: $packageRoot" -ForegroundColor Cyan

Copy-Item -Path (Join-Path $appRootResolved "dist") -Destination (Join-Path $stagingAppRoot "dist") -Recurse -Force
Copy-Item -Path (Join-Path $appRootResolved "scripts") -Destination (Join-Path $stagingAppRoot "scripts") -Recurse -Force
if (Test-Path (Join-Path $appRootResolved "src\css")) {
  Copy-Item -Path (Join-Path $appRootResolved "src\css") -Destination (Join-Path $stagingAppRoot "src\css") -Recurse -Force
}
if (Test-Path (Join-Path $appRootResolved "src\public")) {
  Copy-Item -Path (Join-Path $appRootResolved "src\public") -Destination (Join-Path $stagingAppRoot "src\public") -Recurse -Force
}
if (Test-Path (Join-Path $appRootResolved "artifacts")) {
  Copy-Item -Path (Join-Path $appRootResolved "artifacts") -Destination (Join-Path $stagingAppRoot "artifacts") -Recurse -Force
}
if (Test-Path (Join-Path $appRootResolved "migrations")) {
  Copy-Item -Path (Join-Path $appRootResolved "migrations") -Destination (Join-Path $stagingAppRoot "migrations") -Recurse -Force
}
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

$nssmPath = Join-Path $appRootResolved "nssm.exe"
if (Test-Path $nssmPath) {
  Copy-Item -Path $nssmPath -Destination (Join-Path $stagingAppRoot "nssm.exe") -Force
}

$bootstrapPs1Path = Join-Path $appRootResolved "scripts\windows\install-customer-package.ps1"
if (Test-Path $bootstrapPs1Path) {
  Copy-Item -Path $bootstrapPs1Path -Destination (Join-Path $packageRoot "install-customer-package.ps1") -Force
}

$bootstrapCmdPath = Join-Path $appRootResolved "scripts\windows\install-customer-package.cmd"
if (Test-Path $bootstrapCmdPath) {
  Copy-Item -Path $bootstrapCmdPath -Destination (Join-Path $packageRoot "install-customer-package.cmd") -Force
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

Compress-Archive -Path $packageRoot -DestinationPath $zipPath -CompressionLevel Optimal

Write-Host "Package created: $zipPath" -ForegroundColor Green
if (-not $IncludeNodeModules) {
  Write-Host "Note: node_modules is not included. Customer must run 'npm ci --omit=dev'." -ForegroundColor Yellow
}
Write-Host "Bundled runtime helper included: nssm.exe" -ForegroundColor Cyan
Write-Host "Bootstrap launcher included: install-customer-package.cmd / .ps1" -ForegroundColor Cyan

Remove-Item -Path $stagingRoot -Recurse -Force
