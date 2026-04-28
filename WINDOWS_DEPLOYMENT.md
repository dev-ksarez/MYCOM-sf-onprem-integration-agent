# Windows service and update assistant

This project includes PowerShell assistants to install the agent as a Windows service and to manage automatic updates with rollback.

## Prerequisites

- Windows Server or Windows 10/11
- Node.js 22+
- Built project output in dist (run npm run build)
- Local admin privileges
- PowerShell execution policy that allows local scripts

## 1) Install as Windows service

Run:

npm run win:install-service

The installer validates node.exe and dist/main.js, creates the service, configures restart-on-failure, and starts it.

Optional parameters:

powershell -NoProfile -ExecutionPolicy Bypass -File scripts/windows/install-agent-service.ps1 -ServiceName "SfOnpremIntegrationAgent" -DisplayName "SF OnPrem Integration Agent" -Description "Runs the Salesforce On-Prem Integration Agent" -AppRoot "C:\\apps\\sf-onprem-integration-agent"

## 2) Uninstall service

Run:

npm run win:uninstall-service

## 3) Manual update with rollback

Run:

powershell -NoProfile -ExecutionPolicy Bypass -File scripts/windows/update-agent.ps1 -ServiceName "SfOnpremIntegrationAgent" -UpdateManifestUrl "https://example.org/releases/update-manifest.json" -AppRoot "C:\\apps\\sf-onprem-integration-agent"

Update behavior:

- Reads remote manifest (version, packageUrl, sha256)
- Downloads update ZIP
- Verifies SHA256 (if provided)
- Stops service
- Backs up current runtime files into backups/<timestamp>
- Applies update
- Starts service and verifies Running state
- If start fails, restores backup automatically (fallback)

## 4) Automatic update management

Register a scheduled updater task:

npm run win:register-updater -- -ManifestUrl "https://example.org/releases/update-manifest.json" -EveryMinutes 15

This creates a Windows Scheduled Task running as SYSTEM and executes update-agent.ps1 periodically.

## 5) Update package expectations

The ZIP package should contain at least:

- dist/
- package.json

Optional:

- node_modules/

Example manifest template:

scripts/windows/update-manifest.example.json

## Notes

- Backups are retained in backups/ and automatically trimmed to the latest KeepBackupCount entries.
- The service binary path uses node.exe with dist/main.js.
- If you deploy with a different folder layout, pass -AppRoot explicitly.
