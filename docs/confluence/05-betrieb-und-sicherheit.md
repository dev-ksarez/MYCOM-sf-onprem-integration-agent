---
connie-publish: true
connie-title: 05 - Betrieb und Sicherheit
tags:
  - sf-onprem-integration-agent
  - betrieb
  - sicherheit
---

# Betrieb und Sicherheit

## Sicherheitsregeln

- Secrets werden ueber Environment-Variablen oder lokale Konfigurationsdateien referenziert und sollten nicht in Screenshots oder Dokumentation aufgenommen werden.
- Die Web UI muss in produktiven Umgebungen per Login geschuetzt sein.
- Mutierende Web-Requests sind durch CSRF-Token und Origin-Pruefung abgesichert.
- Externe Erreichbarkeit sollte ueber HTTPS und Reverse Proxy erfolgen.
- Die Agent API benoetigt ein Bearer Token und sollte nur intern erreichbar sein.
- MSSQL-Verbindungen nutzen sichere Defaults mit `encrypt=true` und `trustServerCertificate=false`.
- Updates sollten ueber definierte Release-Pakete und Manifest erfolgen.
- Connector-Assistenten speichern Secret-Key-Werte nur fuer passende Connector-Typen.

## Betriebsueberwachung

| Bereich | Pruefung |
| --- | --- |
| Dienste | Agent-, Web- und Updater-Dienst laufen und starten automatisch. |
| Salesforce | OAuth funktioniert, Limits sind ausreichend, Zielobjekte sind erreichbar. |
| Scheduler | Faellige Runs werden gestartet, blockierte oder ueberlappende Runs werden erkannt. |
| Connectoren | Endpunkte sind erreichbar, Secrets sind gueltig, Tests liefern nachvollziehbare Ergebnisse. |
| Logs | Fehler werden als Run-/Log-Daten dokumentiert und koennen fachlich ausgewertet werden. |
| Updates | Manifest ist erreichbar, Backups werden erstellt, Rollback ist moeglich. |
| Web-Assets | Nach Releases mit UI-Aenderungen ist der Web-Server neu gestartet und Browser-Cache bei Bedarf geleert. |

## Pflegehinweise

Screenshots aktualisieren:

```bash
WEB_UI_PORT=18081 ADMIN_UI_USERS_FILE=artifacts/admin-users.json ADMIN_AUTH_MODE=local node dist/web-main.js
node scripts/capture-doc-screenshots.js
```

Netzwerkdiagramm aktualisieren:

- Quelldatei: `docs/systemdiagramm-netzwerk.svg`
- Publizierte Anlage: `docs/confluence/assets/systemdiagramm-netzwerk.svg`

## Anlagen

| Datei | Zweck |
| --- | --- |
| `systemdiagramm-netzwerk.svg` | Technisches Netzwerkdiagramm |
| `01-login.png` | Login-Screenshot |
| `02-dashboard.png` | Dashboard-Screenshot |
| `03-scheduler.png` | Scheduler-Screenshot |
| `04-connectoren.png` | Connectoren-Screenshot |
| `05-monitoring.png` | Monitoring-Screenshot |
| `06-installation.png` | Installations-Screenshot |
| `07-migration.png` | Migrations-Screenshot |
