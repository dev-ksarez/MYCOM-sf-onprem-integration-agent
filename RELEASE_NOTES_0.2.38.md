# Release Notes 0.2.38

## Highlights

- Monitoring erweitert: Fehlgeschlagene Datensätze pro Run sind im Fehlerdaten-Modal direkt exportierbar.
- Exportformate: CSV und JSON für schnellere Analyse und Weitergabe.
- Buttons werden nur aktiviert, wenn tatsächlich Fehlerdatensätze für den gewählten Run vorliegen.

## Technische Änderungen

- UI: Footer im Fehlerdaten-Modal um Export-Buttons ergänzt.
- Client-Logik: Export-State für zuletzt geladene Run-Fehlerdaten eingeführt.
- CSV-Serializer implementiert (inkl. Escaping und Serialisierung komplexer Payloads).
- JSON-Export über Blob-Download mit Dateinamen auf Basis der Run-ID.

## Verifikation

- TypeScript-Build erfolgreich (`npm run build`).