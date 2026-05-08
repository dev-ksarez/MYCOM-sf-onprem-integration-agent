---
connie-publish: true
connie-title: SF On-Prem Integration Agent - Projektdokumentation
connie-page-id: "4399431685"
connie-dont-change-parent-page: true
tags:
  - sf-onprem-integration-agent
  - projektdokumentation
  - docs-as-code
---

# SF On-Prem Integration Agent - Projektdokumentation

Diese Seite ist der Einstiegspunkt fuer die Projektdokumentation. Die Detaildokumentation ist in einzelne Themen-Seiten aufgeteilt und wird ueber GitHub Actions nach Confluence publiziert.

## Hauptthemen

| Seite | Inhalt |
| --- | --- |
| [Allgemeine Beschreibung](./01-allgemeine-beschreibung.md) | Zielbild, Einsatzszenarien und fachlicher Nutzen |
| [Technische Beschreibung](./02-technische-beschreibung.md) | Architektur, Netzwerkdiagramm, Salesforce-Komponenten und Datenfluss |
| [Installationsbeschreibung](./03-installation.md) | Voraussetzungen, Windows-/Linux-Installation, Salesforce-Metadaten und Updates |
| [Funktionsdokumentation](./04-funktionsdokumentation.md) | Bedienoberflaeche, Dashboard, Monitoring und Bildschirmfotos |
| [Betrieb und Sicherheit](./05-betrieb-und-sicherheit.md) | Betriebsregeln, Security-Vorgaben, Monitoring und Pflege |
| [Assistenten](./06-assistenten.md) | Uebersicht der Assistenten mit Unterthemen fuer Connector, Scheduler und Migration |
| [Release Historie](./07-release-historie.md) | Versionen, Release Notes, Betriebswirkung und Upgrade-Hinweise |

## Systemueberblick

![Systemdiagramm Netzwerk](./assets/systemdiagramm-netzwerk.svg)

Der Agent laeuft im Kundennetz und baut alle Salesforce-Verbindungen ausgehend ueber HTTPS auf. Lokale Systeme wie MSSQL, Dateiablagen und REST-Endpunkte bleiben im On-Prem-Netz angebunden. Salesforce dient als Steuerungsebene fuer Scheduler, Connector-Konfigurationen, Runs, Logs und Checkpoints.

## Pflegehinweise

Screenshots aktualisieren:

```bash
WEB_UI_PORT=18081 ADMIN_UI_USERS_FILE=artifacts/admin-users.json ADMIN_AUTH_MODE=local node dist/web-main.js
node scripts/capture-doc-screenshots.js
```

Netzwerkdiagramm aktualisieren:

- Quelldatei: `docs/systemdiagramm-netzwerk.svg`
- Publizierte Anlage: `docs/confluence/assets/systemdiagramm-netzwerk.svg`
