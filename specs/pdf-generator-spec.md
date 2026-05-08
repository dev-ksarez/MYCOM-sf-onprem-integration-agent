# PDF Generator — Spezifikation

Datum: 2026-05-08
Autor: GitHub Copilot (Draft)

## Ziel

Ein modularer PDF-Generator für das Projekt, der strukturierte Eingabedaten (JSON, HTML, Markdown) in hochwertige PDF-Dokumente umwandelt. Fokus: Nachvollziehbarkeit, Konfigurierbarkeit und einfache Integration in bestehende Flows (CLI, API, Worker).

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

---

Datei-Beispiele: siehe `file-examples/` und `templates/`.
