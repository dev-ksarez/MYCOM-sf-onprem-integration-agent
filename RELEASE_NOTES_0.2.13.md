# Release 0.2.13

- Produktpreise-Importe fuer `PricebookEntry` loesen `Product2Id` und bestehende Eintraege jetzt gesammelt vorab auf, statt pro Datensatz wiederholt serielle Salesforce-Abfragen auszufuehren
- Ein festhaengender Produktpreise-Run konnte sauber abgebrochen und direkt neu gestartet werden; der anschliessende Lauf war wieder erfolgreich in etwa 20 Sekunden abgeschlossen
- Die neue Release behebt damit die juengste Laufzeitregression nach dem 0.2.12-Stand, ohne die zuvor eingefuehrte `PricebookEntry`-Composite-Key-Logik zurueckzunehmen