# Drei Dienste, Release-Workflow und Updater-Migration

- Spec-ID: 2026-05-06-drei-dienste-release-und-updater-zielarchitektur
- Status: draft
- Owner: Codex
- Reviewers: 
- Verknuepfte Tickets: 

## Kontext

Das Projekt wird fachlich bereits in drei Teile gedacht:

1. Agent-Laufzeit fuer Scheduler und Datentransfers
2. Web-Dashboard fuer Admin- und Betriebsfunktionen
3. Installer/Updater fuer Bereitstellung und Bestandsupdates

Die aktuelle Laufzeit- und Betriebsstruktur bildet das nur teilweise ab. Auf Windows laeuft heute ein einzelner Dienst, der Agent und Web UI gemeinsam startet. Der Auto-Updater ist aktuell ein Scheduled Task und kein eigener Dienst. Auf Linux existiert ebenfalls nur ein einzelner systemd-Service. Gleichzeitig soll das Refactoring kuenftig die fachlichen Grenzen in Code, Packaging, Release und Upgrade widerspiegeln.

## Problem

Die bestehende Struktur erschwert Wartbarkeit und Upgrade-Sicherheit:

- Code-Struktur und Betriebsstruktur folgen nicht derselben fachlichen Aufteilung.
- Ein einzelner Prozess koppelt Agent und Web-Server enger als betrieblich gewuenscht.
- Release-Pakete und Update-Mechanik kennen keine expliziten Rollen `agent`, `web` und `updater`.
- Bestandsinstallationen benoetigen bei Einfuehrung der neuen Struktur eine kontrollierte Migration statt eines harten Bruchs.

## Zielbild

1. Die Projektstruktur spiegelt die drei fachlichen Teile `agent`, `web-dashboard` und `installer` klar wider.
2. Die Betriebsstruktur spiegelt drei getrennte technische Rollen wider:
   - Agent-Dienst
   - WebServer-Dienst
   - AutoUpdater-Dienst
3. Release-Artefakte enthalten eine versionsklare Dienst- und Rollenstruktur statt einer impliziten Ein-Prozess-Annahme.
4. Der Updater migriert vorhandene Installationen schrittweise von der bisherigen Ein-Dienst-Struktur auf die neue Drei-Dienste-Struktur.
5. Windows und Linux verwenden jeweils diensttypische Mechanismen, aber mit derselben fachlichen Rollentrennung.
6. Alte Installationen bleiben updatefaehig; die Migration ist idempotent und rollback-faehig.

## Nicht-Ziele

- Sofortige Einfuehrung einer vollstaendig neuen Prozessaufsicht oder Container-Orchestrierung.
- Austausch des bestehenden Update-Manifests durch ein voellig anderes Release-System.
- Gleichzeitige komplette Neuschreibung der Web UI.

## Akzeptanzkriterien

- [ ] Verhalten ist fuer den Nutzer oder Operator eindeutig beobachtbar.
- [ ] Erfolgs- und Fehlerfall sind beschrieben.
- [ ] Konfiguration, Migration oder Deployment-Folgen sind dokumentiert.
- [ ] Es ist festgelegt, wie Bestandsinstallationen von 1 Dienst auf 3 Dienste migriert werden.
- [ ] Release-Paket, Installer und Updater benoetigen dieselbe Rollenbeschreibung und dieselben Dienstnamen.

## Umsetzungsskizze

Betroffene Bereiche im Repo:

- `src/agent/`
- `src/web-dashboard/` oder Nachfolgebereich fuer heutiges `src/server/`
- `src/installer/`
- `scripts/windows/`
- `scripts/linux/`
- `artifacts/`
- `README.md`

Technische Leitplanken:

- Agent und Web-Server erhalten getrennte Startpunkte, z. B. `dist/agent-main.js` und `dist/web-main.js`.
- Gemeinsame Fachlogik bleibt in neutralen Core-/Infrastructure-Modulen und wird nicht in die Dienst-Entrypoints kopiert.
- Installer und Updater arbeiten rollenbasiert:
  - Dienstdefinitionen
  - Start-/Stop-Reihenfolge
  - Env-Dateien
  - Log-Dateien
  - Health- und Statusdateien
- Die Update-Migration muss idempotent sein:
  - alte Ein-Dienst-Installation erkennen
  - Konfiguration sichern
  - neue Dienstdefinitionen erzeugen
  - alten kombinierten Dienst sauber abloesen
  - Rollback bei Fehlschlag
- Windows:
  - heute `SfOnpremIntegrationAgent`
  - Zielbild: drei getrennte Einheiten, z. B. `SfOnpremIntegrationAgent`, `SfOnpremIntegrationWeb`, `SfOnpremIntegrationUpdater`
  - der bisherige Scheduled Task fuer Updates wird in eine dienstnahe Updater-Rolle ueberfuehrt oder mindestens als eigenstaendige Betriebsrolle mit eigener Migration behandelt
- Linux:
  - heute ein systemd-Service
  - Zielbild: getrennte systemd-Units fuer Agent, Web und Updater
- Release-Manifest muss zusaetzlich eine Strukturversion oder Migrationsversion tragen, damit der Updater Schemawechsel erkennen kann.

## Migrationsstrategie fuer Bestandsinstallationen

### Phase 1

- Code fachlich in `agent`, `web-dashboard`, `installer` schneiden.
- Noch keine harte Umstellung der laufenden Dienste.
- Release-Artefakte vorbereiten, damit zwei Startpunkte parallel moeglich sind.

### Phase 2

- Neue Dienstdefinitionen einfuehren.
- Installer erzeugt Neuinstallationen bereits mit drei getrennten Rollen.
- Updater erkennt Altinstallation und fuehrt strukturierte Migration aus:
  - Backup
  - neue Dateien deployen
  - neue Dienstdefinitionen anlegen
  - alten kombinierten Dienst deaktivieren oder entfernen
  - neue Rollen starten und pruefen

### Phase 3

- Kombinierte Altstruktur wird nur noch fuer Legacy-Updates toleriert.
- Nach ausreichend Releases kann die Ein-Dienst-Logik entfernt werden.

## Offene Architekturentscheidungen

1. Soll der Updater auf Windows ein echter Dienst werden oder bleibt er technisch ein Scheduled Task mit eigener Betriebsrolle?
2. Sollen Agent und Web getrennte `.env`-Dateien erhalten oder eine gemeinsame Basiskonfiguration plus rollenspezifische Overrides?
3. Wo liegt die gemeinsame Status- und Log-Ablage, damit Agent, Web und Updater konsistent beobachten und aufraeumen koennen?
4. Welche Strukturversion wird im Update-Manifest und lokal auf dem Zielsystem abgelegt?

## Aufgaben

- [ ] Zielverzeichnisstruktur fuer `src/agent`, `src/web-dashboard` und `src/installer` festlegen.
- [ ] Getrennte Entrypoints fuer Agent und Web definieren.
- [ ] Windows-Dienstmodell fuer drei Rollen spezifizieren.
- [ ] Linux-systemd-Modell fuer drei Rollen spezifizieren.
- [ ] Release-Paketformat um Rollen-/Strukturversion erweitern.
- [ ] Updater-Migrationsschritte fuer Bestandsinstallationen implementierungsnah ausarbeiten.
- [ ] Rollback- und Recovery-Pfade fuer fehlgeschlagene Strukturmigration dokumentieren.

## Verifikation

- Build oder schmaler Smoke-Test:
  - Build mit getrennten Entrypoints erfolgreich
- Manuelle Checks in Web UI oder Agent:
  - Dashboard zeigt getrennten Status fuer Agent, Web und Updater
- Betriebsrelevante Beobachtung nach Deploy:
  - Altinstallation wird auf Zielstruktur migriert, ohne Konfiguration oder Logs zu verlieren

## Status

- Status: draft
- Letzte Entscheidung: Die fachliche Dreiteilung muss sich auch in Release, Installer und Upgrade widerspiegeln.
- Naechster Schritt: Dienstnamen, Entrypoints und Migrationsschritte pro Plattform konkretisieren.
