# Confluence Publishing

Diese Dokumentation wird als Docs-as-Code gepflegt:

1. Markdown und Assets im Git Repository bearbeiten.
2. Aenderung per Pull Request reviewen.
3. Merge nach `main` oder manueller Start der GitHub Action.
4. GitHub Action publiziert nach Confluence.

## Quelle

- Startseite: `docs/confluence/projektdokumentation.md`
- Hauptthemen: `docs/confluence/01-*.md`, `docs/confluence/04-funktionsdokumentation/04-funktionsdokumentation.md`, `docs/confluence/05-*.md`, `docs/confluence/ReleaseNotes.md`
- Unterthemen: `docs/confluence/04-funktionsdokumentation/assistenten/*.md`
- Netzwerkdiagramm: `docs/confluence/assets/systemdiagramm-netzwerk.svg`
- Screenshots: `docs/confluence/assets/screenshots/`
- Workflow: `.github/workflows/publish-confluence-docs.yml`
- GitHub Action: `markdown-confluence/publish-action@v5`

Die Markdown-Datei enthaelt Confluence-Frontmatter:

```yaml
connie-publish: true
connie-title: SF On-Prem Integration Agent - Projektdokumentation
connie-page-id: "4399431685"
connie-dont-change-parent-page: true
```

Damit wird die bestehende Seite mit der ID `4399431685` aktualisiert.

Weitere Markdown-Dateien unter `docs/confluence/` besitzen keine feste `connie-page-id`. Sie werden durch die GitHub Action als Confluence-Seiten angelegt bzw. anhand des Titels aktualisiert. `CONFLUENCE_PARENT_ID` sollte deshalb auf die Projektdokumentationsseite `4399431685` zeigen, damit die Themen-Seiten darunter erscheinen.

Aktuelle Seitenstruktur:

| Datei | Confluence-Seite |
| --- | --- |
| `projektdokumentation.md` | SF On-Prem Integration Agent - Projektdokumentation |
| `01-allgemeine-beschreibung.md` | 01 - Allgemeine Beschreibung |
| `02-technische-beschreibung.md` | 02 - Technische Beschreibung |
| `03-installation.md` | 03 - Installationsbeschreibung |
| `04-funktionsdokumentation/04-funktionsdokumentation.md` | 04 - Funktionsdokumentation |
| `04-funktionsdokumentation/assistenten/assistenten.md` | Assistenten |
| `04-funktionsdokumentation/assistenten/01-connector.md` | 06.1 - Assistent Connector |
| `04-funktionsdokumentation/assistenten/02-scheduler.md` | 06.2 - Assistent Scheduler |
| `04-funktionsdokumentation/assistenten/03-migration.md` | 06.3 - Assistent Migration |
| `05-betrieb-und-sicherheit.md` | 05 - Betrieb und Sicherheit |
| `ReleaseNotes.md` | ReleaseNotes |

Screenshot-Anlagen:

| Datei | Verwendung |
| --- | --- |
| `01-login.png` bis `07-migration.png` | Funktionsdokumentation |
| `08-assistent-connector.png` | Assistent Connector |
| `09-assistent-scheduler.png` | Assistent Scheduler |
| `10-assistent-migration.png` | Assistent Migration |

## GitHub Secrets

In GitHub unter `Settings -> Secrets and variables -> Actions -> Secrets` anlegen:

| Secret | Zweck |
| --- | --- |
| `ATLASSIAN_USERNAME` | Atlassian-Benutzer, meistens die E-Mail-Adresse |
| `ATLASSIAN_API_TOKEN` | Atlassian API Token des Benutzers |

Der Atlassian-Benutzer benoetigt Schreibrechte auf die Confluence-Seite bzw. den Space.

## GitHub Variables

In GitHub unter `Settings -> Secrets and variables -> Actions -> Variables` anlegen:

| Variable | Empfohlener Wert | Zweck |
| --- | --- | --- |
| `CONFLUENCE_BASE_URL` | `https://mycom.atlassian.net` | Base URL der Confluence Cloud Site ohne `/wiki` |
| `CONFLUENCE_PARENT_ID` | `4399431685` | Parent-Seite fuer neue Themen-Seiten |

Hinweis: Die aktuelle Dokumentationsseite hat bereits `connie-page-id: "4399431685"` und `connie-dont-change-parent-page: true`. Die Parent-ID wird vom Tool trotzdem als Pflichtparameter erwartet. Wenn die echte Parent-ID bekannt ist, sollte sie als `CONFLUENCE_PARENT_ID` gesetzt werden.

Der Workflow normalisiert `CONFLUENCE_BASE_URL` automatisch. Falls die Variable versehentlich mit `/wiki` hinterlegt wurde, wird dieser Suffix vor dem Publish entfernt, weil die verwendete Confluence-Client-Bibliothek fuer Atlassian Cloud den Host in der Form `https://<tenant>.atlassian.net` erwartet.

## Workflow starten

Automatisch:

- Push nach `main` oder `master`, wenn Dateien unter `docs/confluence/**` geaendert wurden.

Manuell:

- GitHub -> Actions -> `Publish Confluence Docs` -> `Run workflow`.

## Screenshots aktualisieren

Lokale Web UI starten:

```bash
WEB_UI_PORT=18081 ADMIN_UI_USERS_FILE=artifacts/admin-users.json ADMIN_AUTH_MODE=local node dist/web-main.js
```

Screenshots neu erzeugen:

```bash
node scripts/capture-doc-screenshots.js
```

Danach die geaenderten PNG-Dateien unter `docs/confluence/assets/screenshots/` aktualisieren und committen.
