---
connie-publish: true
connie-title: 06.1 - Assistent Connector
tags:
  - sf-onprem-integration-agent
  - assistent
  - connector
---

# Assistent Connector

![Assistent Connector](../../assets/screenshots/08-assistent-connector.png)

Der Connector-Assistent dient zur Pflege technischer Datenquellen und Zielsysteme. Er fuehrt Administratoren durch Typauswahl, Parameterpflege, Secret-Referenzen, Verbindungstest und Zuordnung zu Schedulern.

## Ablauf

```mermaid
flowchart TD
  A[Connector oeffnen] --> B[Connector-Typ waehlen]
  B --> C[Parameter und Secret Keys pflegen]
  C --> D[Verbindung testen]
  D --> E{Test erfolgreich?}
  E -->|Ja| F[Connector speichern]
  E -->|Nein| G[Fehlerdetails pruefen]
  G --> C
  F --> H[Scheduler zuordnen]
  H --> I[Monitoring und Runs beobachten]
```

## Unterstuetzte Connector-Typen

| Typ | Zweck | Typische Parameter |
| --- | --- | --- |
| MSSQL | Lesen und Schreiben lokaler SQL-Daten | Host, Port, Datenbank, Benutzer, Secret Key, TLS-Optionen |
| REST API | Anbindung HTTP(S)-basierter Systeme | Base URL, Authentifizierung, Header, Timeout, Pagination |
| File | Import und Export von CSV, XLSX oder JSON | Pfad, Dateityp, Encoding, Trennzeichen, Archivierung |
| Salesforce | Salesforce als Quelle oder Ziel | Objekt, SOQL/REST-Optionen, Mapping, Upsert-Key |

## Validierung

- Pflichtfelder werden je Connector-Typ geprueft.
- Secrets werden nicht im Klartext dokumentiert, sondern ueber Secret Keys referenziert.
- Verbindungstests liefern technische Fehlermeldungen fuer Netzwerk, Authentifizierung und Berechtigungen.
- Zugeordnete Scheduler zeigen, welche Jobs von einer Connector-Aenderung betroffen sind.

## Ergebnis

Nach erfolgreicher Pflege steht der Connector der Scheduler-Konfiguration zur Verfuegung. Laufende oder geplante Jobs verwenden die Connector-ID, um Source- und Target-Adapter in der Runtime zu initialisieren.
