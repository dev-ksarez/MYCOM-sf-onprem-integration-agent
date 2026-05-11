# Release 0.2.31

## Schwerpunkt

Hotfix: Fehlende Release-Notes-Datei fuer 0.2.30 nachgezogen und Confluence-Workflow robuster gemacht.

## Aenderungen

### Confluence-Workflow

- Fehlende Release-Notes-Datei fuer `0.2.30` angelegt.
- Confluence-Workflow bricht nicht mehr ab, wenn keine Release-Notes-Datei vorhanden ist – er gibt stattdessen eine Warnung aus und setzt die Ausfuehrung fort.

## Migration Path

Kein manueller Schritt erforderlich. Reine Build-/Workflow-Korrektur.

## Verifikation

- Release-Workflow laeuft erfolgreich durch, auch ohne passende Release-Notes-Datei.
