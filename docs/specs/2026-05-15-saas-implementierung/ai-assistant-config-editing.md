# KI-Assistent fuer Konfigurationspruefung und -anpassung

## Zweck

Der KI-Assistent soll nicht nur neue Scheduler- oder Mapping-Vorschlaege erzeugen, sondern auch bestehende Konfigurationen pruefen und gezielt anpassen koennen. Ein typischer Prompt ist:

```text
Passe Scheduler SCH-0062 an und ergaenze die Rechnungsadresse und die Kundennummer.
```

Der Assistent muss daraus eine nachvollziehbare, versionierte Konfigurationsaenderung erzeugen, die vor Aktivierung validiert und vom berechtigten Nutzer freigegeben wird.

## Grundprinzip

- Bestehende Konfiguration wird zuerst geladen, nicht neu geraten.
- Der Assistent arbeitet auf einer Draft-ConfigVersion.
- Aktive Scheduler werden nie direkt ueberschrieben.
- Vorschlaege muessen als Diff sichtbar sein.
- Der lokale Agent fuehrt nur freigegebene ConfigVersions aus.
- Fehlende Metadaten oder unklare Felder fuehren zu Rueckfragen oder Warnungen, nicht zu stillen Annahmen.

## Eingaben

Der Assistent nutzt:

- Projektkontext.
- Scheduler-ID oder Scheduler-Name.
- Aktive ConfigVersion.
- Bestehende Connectoren.
- Bestehende SourceDefinition und TargetDefinition.
- Bestehendes Mapping.
- Salesforce-Metadaten.
- SAGE100-/MSSQL-Dokumentation und Tabellen-/Feldmetadaten.
- Historische erfolgreiche Konfigurationen als allgemeine Beispiele, nicht als projektspezifische Pflicht.
- Nutzeranweisung.

## Workflow

1. Intent erkennen
   - Beispiel: `Scheduler SCH-0062 anpassen`.
   - Operation: bestehende Konfiguration aendern.
   - Gewuenschte Erweiterungen: Rechnungsadresse, Kundennummer.

2. Zielkonfiguration laden
   - Scheduler `SCH-0062` im aktuellen Projekt suchen.
   - Aktive ConfigVersion und Mapping laden.
   - Zugehoerige Connectoren, Quelle, Ziel und Operation laden.

3. Bestehende Konfiguration pruefen
   - SQL/SourceDefinition syntaktisch pruefen.
   - Verwendete Quellfelder gegen lokale Metadaten/Dokumentation pruefen.
   - Zielobjekt und Zielfelder gegen Salesforce-Metadaten pruefen.
   - Mapping auf entfernte, veraltete oder ungueltige Felder pruefen.

4. Feldbedarf ableiten
   - Rechnungsadresse muss auf Billing-Felder gemappt werden.
   - Lieferadresse muss auf Shipping-Felder gemappt werden.
   - Kundennummer muss auf ein projektspezifisches External-ID- oder ERP-Account-Feld gemappt werden, sofern vorhanden.
   - Bei mehreren moeglichen Zielfeldern muss der Assistent die beste Option begruenden oder Rueckfrage erzeugen.

5. Draft erzeugen
   - Neue Draft-ConfigVersion aus aktiver Version ableiten.
   - SourceDefinition nur um benoetigte Quellfelder erweitern.
   - Mapping um benoetigte Zielzuordnungen erweitern oder vorhandene falsche Zuordnungen korrigieren.
   - Keine bestehenden funktionierenden Mappings entfernen, ausser der Nutzer fordert es oder die Validierung markiert sie als ungueltig.

6. Diff anzeigen
   - Geaenderte SQL-/SourceDefinition.
   - Neue/geaenderte Mapping-Zeilen.
   - Begruendung je Feld.
   - Risiken, offene Fragen, fehlende Metadaten.

7. Validieren
   - SQL/Testabfrage ueber lokalen Agenten.
   - Salesforce-Feldvalidierung.
   - Pflichtfeldpruefung.
   - Typ- und Transformationspruefung.
   - Optional Testlauf mit begrenztem Datensatz.

8. Freigabe
   - Berechtigter Nutzer gibt die Draft-ConfigVersion frei.
   - Agent erhaelt neue Version erst nach Freigabe.
   - Audit enthaelt Prompt, Diff, Validierungsergebnis, Nutzer und Zeitpunkt.

## Beispiel: Rechnungsadresse und Kundennummer

Prompt:

```text
Passe Scheduler SCH-0062 an und ergaenze die Rechnungsadresse und die Kundennummer.
```

Erwartetes Verhalten:

- Scheduler `SCH-0062` wird im aktuellen Projekt geladen.
- Der Assistent erkennt, ob die Quelle SAGE100/MSSQL und das Ziel Salesforce Account ist.
- Rechnungsadresse wird auf Salesforce Billing-Felder gemappt.
- Kundennummer wird auf das konfigurierte Kundennummernfeld gemappt, z.B. `ERP_Account_Number__c`, wenn dieses Feld in den Salesforce-Metadaten existiert.
- Lieferfelder wie `LieferStrasse`, `LieferPLZ`, `LieferOrt`, `LieferLand` werden nicht fuer Billing-Felder genutzt, sondern fuer Shipping-Felder.
- Falls SAGE100 getrennte Rechnungsadressfelder bereitstellt, werden diese bevorzugt.
- Falls nur allgemeine Adressfelder existieren, wird die Annahme im Diff markiert.

Beispiel-Diff:

```text
SourceDefinition:
+ Adresse
+ Name1
+ Strasse
+ PLZ
+ Ort
+ Land

Mapping:
+ ERP_Account_Number__c;string=Adresse;TRIM
+ BillingStreet;string=Strasse;TRIM
+ BillingPostalCode;string=PLZ;TRIM
+ BillingCity;string=Ort;TRIM
+ BillingCountry;string=Land;TRIM
```

Wenn die bestehende Konfiguration bereits Lieferfelder enthaelt:

```text
Mapping:
+ ShippingStreet;string=LieferStrasse;TRIM
+ ShippingPostalCode;string=LieferPLZ;TRIM
+ ShippingCity;string=LieferOrt;TRIM
+ ShippingCountry;string=LieferLand;TRIM
```

Der Assistent darf `LieferStrasse` nicht still auf `BillingStreet` mappen.

## Sicherheitsregeln

- Keine direkte Aktivierung durch KI ohne Nutzerfreigabe.
- Keine Aenderung an aktiver ConfigVersion.
- Keine lokalen Secrets im Prompt, Diff oder Audit speichern.
- Kein SQL-Feld erfinden, wenn es nicht in Metadaten oder Dokumentation gefunden wurde.
- Kein Salesforce-Feld verwenden, wenn es im Ziel nicht existiert oder nicht beschreibbar ist.
- Bei fehlender Eindeutigkeit Rueckfrage oder Warnung erzeugen.

## Akzeptanzkriterien

- Ein Nutzer kann einen bestehenden Scheduler per Prompt referenzieren.
- Der Assistent laedt die aktive Scheduler-Konfiguration und zeigt den aktuellen Stand an.
- Der Assistent erzeugt eine Draft-ConfigVersion mit Diff.
- Der Assistent unterscheidet Rechnungsadresse und Lieferadresse korrekt.
- Der Assistent validiert Quellfelder und Zielfelder gegen verfuegbare Metadaten.
- Der Assistent blockiert Freigabe bei ungueltiger SQL, fehlenden Zielfeldern oder nicht aufloesbaren Pflichtfeldern.
- Der Nutzer kann die vorgeschlagene Aenderung annehmen, weiter bearbeiten oder verwerfen.
- Prompt, Diff, Validierungsergebnis und Freigabe werden auditiert.
