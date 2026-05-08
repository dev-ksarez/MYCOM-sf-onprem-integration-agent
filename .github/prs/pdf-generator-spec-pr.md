Titel: spec(pdf): add PDF Generator spec

Kurzbeschreibung:
Diese PR fügt die Spezifikation für den neuen PDF-Generator hinzu (Kopie in `docs/specs/` und Original in `specs/`). Ziel ist, Anforderungen, API-Vorschläge und Akzeptanzkriterien für einen Playwright-basierten Prototyp festzuhalten.

Geänderte Dateien:
- `specs/pdf-generator-spec.md` (neu)
- `docs/specs/pdf-generator-spec.md` (neu, mit Review-Checklist)

Akzeptanzkriterien:
- Die Spec beschreibt Eingabeformate (JSON, HTML, Markdown) und Mindest-API (CLI + HTTP).
- Kopf-/Fußzeile, Seitenzahlen und Seitengrößen sind als Optionen dokumentiert.
- Review-Checklist ist vorhanden und umsetzbare nächste Schritte genannt.
- Abbildung 1:n Datenquellen, wie Rechnungen und Angebote
- Einbettung von Bildern und Logos
- Beispielvorlagen für Briefe, Rechnungen, Listen, Editketten
- Vorlagen sollen auch selbt hinzugefügt werden können

Architektur:
- Neuer Connector TYP PDF
- Konfiguration Ablagepfad der Datei mit dynamischen Pfaden und Dateinamen
- Anbindung der Vorlagen erfolgt auf Schedulerbasis
- Anpassung des Scheduler Assistenten für PDF notwedendig
- Allgemein mit Zielpfad (relativ zum Connector)
- Dateiname mit dynamischen Werten (AccountID od Datum od Rechnungsnummer etc)
- Darstellung Datenmapping (Kopfdaten, Detaildaten), je nach PDF Typ (1:n, oder listen, oder Ediketten)
- PDF Designer
- Im Scheduler wird die Quelle definiert (Salesforce 1:n) .z.B. Rechungskopf / Items
- Vorschau


Review-Anweisungen:
- Bitte prüfen: Sind die Anforderungen vollständig für euren Bereich (Security, Storage, CI)?
- Entscheidet über Persistenzziel: `artifacts/` vs S3.
- Vorschlag für Template-Engine (Handlebars/Mustache) oder Gegenargumente bitte als Kommentar.
- Markiert die Checklist-Punkte in `docs/specs/pdf-generator-spec.md` wenn abgenommen.

Nächste Schritte nach Merge (Empfehlung):
- Prototyp-Branch `feature/pdf-generator` anlegen und Playwright-Renderer minimal implementieren.
- CI: E2E-Job mit headless Playwright einrichten.

Weitere Hinweise:
- Open Questions sind im Spec gelistet; bitte vor Review ergänzen falls nötig.

Aktualisierung: Spec wurde am 2026-05-08 angepasst und die `docs`-Kopie auf den aktuellen Stand gebracht. Bitte Review auf `docs/specs/pdf-generator-spec.md` durchführen.
