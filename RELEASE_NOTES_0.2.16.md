# Release 0.2.16

- Windows-Bestandsupdates uebernehmen jetzt `nssm.exe` in den Updatepfad, damit die Migration auf die getrennten Windows-Dienste auch auf vorhandenen Installationen sauber neu konfigurieren kann
- Das Windows-Installationsprofil `agent-host` installiert jetzt nur noch den Agent-Dienst `SfOnpremIntegrationAgent`
- Das Windows-Installationsprofil `web-host` installiert jetzt die Web UI und den AutoUpdater als `SfOnpremIntegrationWeb` und `SfOnpremIntegrationUpdater`
- Installer-Hinweise und generierte Installationsanweisungen dokumentieren die getrennte Rollenverteilung fuer Windows jetzt konsistent zur Zielarchitektur `agent` / `web` / `updater`
