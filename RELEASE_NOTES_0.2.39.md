# Release Notes 0.2.39

## Fixes

- Behebt den Startfehler der Web-UI durch korrektes Escaping im inline gerenderten Admin-Skript.
- CSV-Export für fehlgeschlagene Datensätze ist damit wieder syntaktisch sicher in der Browser-Ausgabe eingebettet.

## Verifikation

- Inline-Skript mit `vm.Script` geprüft.
- Web-UI im lokalen Dev-Server erfolgreich gestartet und geladen.