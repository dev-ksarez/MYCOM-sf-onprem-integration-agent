# Release 0.2.15

- Die Web-UI nutzt jetzt standardmaessig lokale Anmeldung mit persistenter Benutzerverwaltung unter `artifacts/admin-users.json`
- Die Benutzerverwaltung wurde in ein Modal verschoben und um Modulrechte fuer das Migrationsmodul sowie eine Aenderungshistorie erweitert
- Konfigurationsaenderungen an Benutzern, Instanzen, Templates, Schedulern, Connectoren und Migrationen werden mit Benutzer und Zeitstempel in `artifacts/audit-history.json` protokolliert
- Windows-Installationen und bestehende Windows-Agenten werden bei Installation bzw. Update auf die lokale Benutzerdatei vorbereitet, ohne vorhandene Betriebsdaten zu ueberschreiben
- Das Kundenpaket enthaelt jetzt die Bootstrap-Benutzerdatei und die aktuellen Release Notes
