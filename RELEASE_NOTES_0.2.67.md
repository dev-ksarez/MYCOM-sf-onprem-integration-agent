# Release 0.2.67

## Dashboard

- Agentenanalyse kompakter in den oberen Dashboard-Bereich verschoben.
- Aktive Scheduler und Connectoren in einer gemeinsamen KPI-Kachel zusammengefuehrt.
- Salesforce-Org-Kachel zeigt jetzt Datenzuwachs und durchschnittliche API Calls pro Stunde statt Lizenzen und Dateispeicher.
- Navigationsbereich folgt jetzt der aktiven Instanzumgebung: Test, Produktion oder kein Kontext.
- Asset-Version aktualisiert, damit Browser die neuen Dashboard-Styles sicher laden.

## Monitoring

- Verknuepfungsuebersicht wird nach Datensatz-Summary-Refresh erneut gerendert.
- Datensatzanzeige in Scheduler-Knoten beruecksichtigt vorab ermittelte `totalRecords` bzw. `recordCount`.
