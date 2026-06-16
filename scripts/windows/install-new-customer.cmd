@echo off
setlocal
set "SF_AGENT_INSTALL_LOG=%TEMP%\sf-onprem-integration-agent-new-customer-install.log"
echo Installer-Log: %SF_AGENT_INSTALL_LOG%
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0install-new-customer.ps1" %*
if errorlevel 1 (
  echo.
  echo Installation failed.
  echo Bitte Log pruefen: %SF_AGENT_INSTALL_LOG%
  pause
)
endlocal
