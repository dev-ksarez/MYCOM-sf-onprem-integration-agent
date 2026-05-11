# Release 0.2.30

## Schwerpunkt

Dieses Release stabilisiert den Scheduler-Mapping-Flow bei Lookup-Szenarien und verbessert die Sichtbarkeit von Versionsinformationen im UI.

## Aenderungen

### Scheduler Mapping und Vorschau

- Mapping-Vorschau verarbeitet `LOOKUP`-Transforms jetzt robust, auch wenn kein echter Lookup-Resolver verfuegbar ist.
- Lookup-Feldlisten im Scheduler werden wieder befuellt:
  - bevorzugt mit External-ID-Feldern,
  - mit Fallback auf geeignete Felder, wenn keine External IDs vorhanden sind.
- Der Lookup-Editor verwendet klarere Feldbezeichnungen fuer die Auswahl.

### UI-Anpassungen

- Die auffaellige hellblaue Trennlinie im Header wurde entfernt.
- Die Versionsanzeige im Header wurde wieder eingeblendet.
- Die Versionsanzeige im Login wurde dezenter gestaltet und unten mittig positioniert.
- Asset-Versionierung wurde aktualisiert, damit Browser-Caches die UI-Aenderungen sofort laden.

## Migration Path

1. Neue Version deployen.
2. Dienst neu starten, damit Server- und UI-Aenderungen aktiv werden.
3. Browser-Cache leeren oder Seite hart neu laden, damit aktualisierte CSS/JS-Assets verwendet werden.
4. Im Scheduler-Mapping pruefen, ob Lookup-Feldauswahl und Vorschau fuer Ansprechpartner/Lookup-Felder wie erwartet funktionieren.

## Verifikation

- `npm run build`
- Scheduler Mapping Vorschau mit `LOOKUP`-Regeln oeffnen und Ergebnis pruefen
- Login und Header visuell auf Versionsanzeige und Trennlinie pruefen
