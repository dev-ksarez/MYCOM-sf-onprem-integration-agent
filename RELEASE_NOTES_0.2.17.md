# Release 0.2.17

- Hotfix fuer Windows PowerShell: `install-agent-service.ps1` verwendet keine nicht unterstuetzte `||`-Syntax mehr
- Bestandsupdates ueber `update-existing-installation.cmd` koennen die Dienstemigration auf Windows jetzt wieder ohne Parserfehler ausfuehren
- Die in `0.2.16` eingefuehrte Trennung der Windows-Rollen `agent-host` und `web-host` bleibt unveraendert bestehen
