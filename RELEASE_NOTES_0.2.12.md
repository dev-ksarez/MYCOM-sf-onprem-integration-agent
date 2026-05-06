# Release 0.2.12

- Scheduler mit Import-Profile-Regeln beruecksichtigen jetzt `lastRunAt`, damit Faelligkeiten nach Polling-Luecken nicht verloren gehen
- Salesforce-Target-Definitionen mit `selectedImportProfileName` und verschachtelten `target`-Objekten werden in Runtime und Validierung konsistent aufgeloest
- Produktpreis-Scheduler fuer `PricebookEntry` validieren Pflichtfelder und Pricebook-Konfiguration robuster, inklusive auswaehlbarem Pricebook im UI
- Connector-Task-Benachrichtigungen unterstuetzen Salesforce-Benutzerauswahl, Mehrfachauswahl der Fehlerklassen und zeigen Empfaenger plus Klassen direkt im Connector-Panel an