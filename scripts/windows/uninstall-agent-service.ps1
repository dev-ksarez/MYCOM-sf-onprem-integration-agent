param(
  [string]$ServiceName = "SfOnpremIntegrationAgent",
  [string]$WebServiceName = "SfOnpremIntegrationWeb",
  [string]$UpdaterServiceName = "SfOnpremIntegrationUpdater"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

foreach ($name in @($UpdaterServiceName, $WebServiceName, $ServiceName)) {
  $service = Get-Service -Name $name -ErrorAction SilentlyContinue
  if (-not $service) {
    Write-Host "Service '$name' does not exist."
    continue
  }

  if ($service.Status -ne "Stopped") {
    Stop-Service -Name $name -Force
  }

  & sc.exe delete $name | Out-Null
  Write-Host "Service '$name' removed." -ForegroundColor Green
}
