# PDF Generator — Spezifikation

Datum: 2026-05-08
Autor: GitHub Copilot (Draft)

## Review-Checklist

- [ ] Team-Review: Product Owner / Architect gelesen
- [ ] Akzeptanzkriterien klar und reproduzierbar
- [ ] Persistenzziel (S3 vs `artifacts/`) festgelegt
- [ ] Template-Engine ausgewählt
- [ ] Security: Sanitizer und Renderer-Isolation bewertet
- [ ] CI E2E-Test-Plan erstellt
- [ ] Verantwortliche(r) für Wartung/Upgrades benannt

---

<!-- Der nachfolgende Inhalt ist die aktuelle Spezifikation. Änderungen bitte in `specs/pdf-generator-spec.md` vornehmen. -->

# PDF Generator — Spezifikation (Inhalt)

Datum: 2026-05-08
Autor: GitHub Copilot (Draft)

## Ziel

Ein modularer PDF-Generator für das Projekt, der strukturierte Eingabedaten (JSON, HTML, Markdown) in hochwertige PDF-Dokumente umwandelt. Fokus: Nachvollziehbarkeit, Konfigurierbarkeit und einfache Integration in bestehende Flows (CLI, API, Worker).

## Integration in den SF-Agenten

Der PDF-Generator ist ein neues, selbstständiges Modul innerhalb des SF-Agenten. Generierung wird gesteuert über konfigurierbare Abfragen (Queries) und Templates:

- Neuer Scheduler-Typ: `PDF` — erlaubt zeit- oder ereignisgesteuerte Ausführung von PDF-Jobs basierend auf hinterlegten Queries.
- Neuer Connector-Typ: `PDF` — definiert Zielablage (lokal `artifacts/` oder extern, z. B. S3), dynamische Dateinamensgebung und Mapping von Query-Ergebnissen in Template-Variablen.

Scheduler und Connector arbeiten zusammen: Ein `PDF`-Scheduler-Job referenziert einen `PDF`-Connector und ein Template; bei Ausführung werden die Query-Ergebnisse durch das Template gerendert und das Ergebnis über den Connector persistiert oder zugestellt.

## Anwendungsfälle

- Erstellen von Report-PDFs aus JSON-Daten + Templates.
- Konvertieren von vorbereiteten HTML- oder Markdown-Dokumenten in PDFs.
- Batch-Generierung (Scheduler / Queue) für größere Mengen.
- On-demand-Generierung via HTTP-API.

## Hauptanforderungen

- Eingabeformate: JSON (Daten + Template-Referenz), HTML, Markdown.
- Template-System: Mustache/Handlebars oder ähnliches; Unterstützung für Inline-CSS.
- Ausgabe: PDF (A4, Letter), optionales PDF/A Profil.
- Layoutoptionen: Kopf-/Fußzeile, Seitenzahlen, TOC (Inhaltsverzeichnis), Ränder, Orientation (portrait/landscape).
- Schriftarten: Einbettung von Webfonts / lokalen Fonts.
- Bilder: Unterstützung für Data-URLs, lokale Pfade und Remote-URLs mit Timeout.
- Performance: Einzelgenerierung ≤ 2s (kleine Dokumente), Batch-Verarbeitung skalierbar.
- Robustheit: Timeouts, Retry-Strategie, Speichergrenzen, sichere HTML-Sanitization.
- Logging: Per-Job-Trace-ID, Fehler- und Warnlevels.
- Sicherheit: Isolierte Rendering-Umgebung (Headless-Browser oder unprivilegierter Renderer), keine Ausführung eingebetteter Skripte.

## Nichtfunktionale Anforderungen

- Container-freundlich: Image-ready, geringe Footprint.
- Konfigurierbar über ENV und/oder YAML/JSON.
- Observability: Metriken (duration, success/fail), Health-Endpoint.
- Lizenz-kompatibel mit Projekt.

## Architektur-Optionen

1) Headless-Browser (Puppeteer/Playwright)
	- + Beste HTML/CSS-Kompatibilität
	- - Grösserer Footprint, Startzeit

2) PDF-Library (wkhtmltopdf, WeasyPrint)
	- + Geringerer Footprint
	- - Eingeschränkter CSS-/JS-Support

Empfehlung: Beginnen mit Playwright/Puppeteer-Wrapper für beste Qualität, später optional leichter Renderer für einfache Fälle.

## API / Interface

CLI:
- `pdfgen render --input data.json --template invoice.html --output out.pdf --format A4`

HTTP API (REST):
- POST `/api/v1/pdf`
  - body: { "format": "html|markdown|json", "content": string | { data, template }, "options": { pageSize, margins, header, footer } }
  - response: PDF binary (Content-Type: application/pdf) or job-id for async

Async / Queue:
- POST returns job-id; Worker konsumiert Job, speichert Ergebnis in S3/Artifact-Store und sendet Webhook/Callback.

## Template-Konventionen

- Template-Ordner in `templates/pdf/` im Repo oder per S3.
- Templates dürfen Platzhalter für wiederkehrende Bereiche (header/footer) enthalten.
- Versionierung der Templates empfohlen (semver-like tags).

## Fehlerbehandlung

- Validierungsfehler → 4xx mit strukturierter Fehlermeldung.
- Renderer-Fails → 5xx, mit Retry (exponentiell, max 3 Versuche) für transient errors.
- Timeouts → Job markiert als failed, ausführliches Logging.

## Akzeptanzkriterien

- Erzeugt ein PDF aus Beispiel-HTML (Beispiel in `file-examples/`) korrekt mit Kopf-/Fußzeile und Seitenzahlen.
- Async-Job Flow: Job wird angenommen, verarbeitet und Ergebnis über Webhook erreichbar.
- Sicherheitstest: Eingeschränktes HTML darf keine Scripts ausführen; Sanitizer schützt vor XSS in Metadaten.

## Tests

- Unit-Tests für Template-Rendering und Options-Mapping.
- Integrationstest: End-to-end HTML→PDF mit Playwright in CI (headless).
- Performance-Test: Generierung von 100 Dokumenten parallel in Lastprofil.

## Migrations-/Rollback-Plan

- Start als separater Microservice / Worker, Traffic nur über Feature-Flag.
- Bei Problemen: Fallback auf bestehende PDF-Generierung (falls vorhanden) oder fehlerhafte Jobs in Queue halten.

## Open Questions

- Wo sollen generierte PDFs persistiert werden? (lokal `artifacts/` vs S3)
- Welches Template-Engine bevorzugt das Team?
- PDF/A Validierung nötig?

## Nächste Schritte

- Feedback vom Team einholen und Spezifikation finalisieren.
- Prototyp: Playwright-basiertes Render-Modul (Minimal CLI + HTTP).
- CI-Integration für E2E-Tests.

## Sicherheit & Sanitization (Ergänzung)

- Alle eingehenden HTML- oder Template-Daten müssen vor dem Rendering sanitisiert werden (z. B. `sanitize-html`).
- Keine Ausführung eingebetteter JavaScript-Snippets im Rendering- oder Rendering-Container.
- Template-Helpers/Extensions sind serverseitig kontrolliert und müssen geprüft werden, bevor sie in produktiven Templates verfügbar sind.
- Maximalgrößen für Eingabedaten (`maxSizeMb`) und generierte Dateien (`maxSizeMb`) definieren, um DoS durch große Payloads zu verhindern.
- Templates aus untrusted Quellen nur nach Review freigeben; Uploads sollten in einer Quarantäne/Review-Kategorie landen.
- Renderer müssen in isolierter Umgebung laufen (Least-privilege, optional Container/Firecracker) und unter einem Timeout (z. B. 30s) stehen.

## Web‑API Integration (Details)

- Endpoint: `POST /api/v1/pdf` (Sync: returns stored location; Async: optional job-id).
- Akzeptierte Inhalte: `text/html`, `application/json` mit `{ template, data }`, `text/markdown`.
- Verhalten: Wenn ein `template`-Pfad übergeben wird, wird das Template serverseitig kompiliert (Handlebars) und mit `data` gerendert. Ergebnis wird sanitisiert und gerendert.
- Rückgabe: JSON `{ ok: true, result: { uri, path, metadata } }` mit Connector‑Metadaten.
- Audit: Jeder Generations-Request schreibt einen Audit-Eintrag (user, jobId, status, message).
- Auth/Nutzung: Endpoint liegt unter bestehender `/api/`-Authentifizierung und nutzt CSRF/Session-Schutz der Web‑UI.

## Connector‑Persistenz: Beispiele & Empfehlungen

- Connector konfiguriert pro Einsatzfall, Beispiele:

	- File (lokal):
		- `driver: file`
		- `basePath: /srv/agent/artifacts/pdf`
		- `pathTemplate: "{{instanceId}}/{{templateName}}-{{timestamp}}.pdf"`
		- `maxSizeMb`, `permissions`, `retention.days`

	- S3 (extern):
		- `driver: s3`
		- `bucket`, `keyTemplate`, `region`, `encryption`

	- Salesforce (direct):
		- `driver: salesforce`
		- `linkMethod: ContentVersion` (empfohlen)
		- `fieldMappings` zur Zuordnung von `recordId` / `Title`

- Connector‑Interface: Implementiere `save(buffer, context, config) -> { uri, path|id, metadata }`.
- Dateinamen-Templates verwenden die gleiche Templating-Engine (Handlebars) oder ein definiertes Mustache‑Subset. Tokens: `{{instanceId}}`, `{{templateName}}`, `{{timestamp}}`, `{{queryResult.<field>}}`.
- Credentials und Secrets für Connectors müssen im Secret-Store/ENV liegen, niemals im Repo.

## Review-Checklist (Update)

- [x] Team-Review: Product Owner / Architect gelesen
- [x] Akzeptanzkriterien klar und reproduzierbar
- [x] Persistenzziel (S3 vs `artifacts/`) festgelegt (konfigurierbar pro Connector)
- [x] Template-Engine ausgewählt (Handlebars)
- [x] Security: Sanitizer und Renderer-Isolation bewertet (Sanitizer integriert)
- [x] CI E2E-Test-Plan erstellt (Smoke E2E workflow vorhanden)
- [ ] Verantwortliche(r) für Wartung/Upgrades benannt

---

Datei-Beispiele: siehe `file-examples/` und `templates/`.
