# Confluence Publishing

Diese Dokumentation wird als Docs-as-Code gepflegt:

1. Markdown und Assets im Git Repository bearbeiten.
2. Aenderung per Pull Request reviewen.
3. Merge nach `main` oder manueller Start der GitHub Action.
4. GitHub Action publiziert nach Confluence.

## Quelle

- Markdown-Seite: `docs/confluence/projektdokumentation.md`
- Netzwerkdiagramm: `docs/confluence/assets/systemdiagramm-netzwerk.svg`
- Screenshots: `docs/confluence/assets/screenshots/`
- Workflow: `.github/workflows/publish-confluence-docs.yml`

Die Markdown-Datei enthaelt Confluence-Frontmatter:

```yaml
connie-publish: true
connie-title: SF On-Prem Integration Agent - Projektdokumentation
connie-page-id: "4399431685"
connie-dont-change-parent-page: true
```

Damit wird die bestehende Seite mit der ID `4399431685` aktualisiert.

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
| `CONFLUENCE_BASE_URL` | `https://mycom.atlassian.net/wiki` | Base URL der Confluence Cloud Site |
| `CONFLUENCE_PARENT_ID` | Parent-Page-ID der Dokumentation | Parent-Seite fuer neue Seiten |

Hinweis: Die aktuelle Dokumentationsseite hat bereits `connie-page-id: "4399431685"` und `connie-dont-change-parent-page: true`. Die Parent-ID wird vom Tool trotzdem als Pflichtparameter erwartet. Wenn die echte Parent-ID bekannt ist, sollte sie als `CONFLUENCE_PARENT_ID` gesetzt werden.

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
