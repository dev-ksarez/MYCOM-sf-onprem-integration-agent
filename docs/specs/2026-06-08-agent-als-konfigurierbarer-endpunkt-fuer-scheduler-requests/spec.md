# Agent als konfigurierbarer Endpunkt fuer Scheduler Requests

- Spec-ID: 2026-06-08-agent-als-konfigurierbarer-endpunkt-fuer-scheduler-requests
- Status: draft
- Owner:
- Reviewers:
- Verknuepfte Tickets:

## Kontext

Der Agent kann heute Daten aktiv aus Quellen lesen und in Ziele schreiben, zum Beispiel MSSQL, REST, Dateien, FileMaker und Salesforce. Fuer Integrationen, bei denen ein Fremdsystem Ereignisse oder Datensaetze aktiv an den Agenten schicken soll, fehlt eine produktnahe inbound Variante: der Agent soll selbst einen HTTP-Endpunkt bereitstellen, Request-Daten entgegennehmen, authentifizieren, validieren und ueber die bestehende Scheduler-, Mapping- und Salesforce-Zielstrecke verarbeiten.

Die Anforderung fuehrt zwei Ebenen ein:

- Connector: definiert den Root-Endpunkt des Agenten inklusive Authentifizierung, insbesondere OAuth2.
- Scheduler: neuer Typ `ENDPOINT`, der einen konkreten Request unterhalb dieses Root-Endpunkts beschreibt und die empfangenen Payloads ueber Mapping und Zieldefinition mit Salesforce verbindet.

## Problem

Ohne Agent-eigene Endpunkte muessen externe Systeme entweder direkt Salesforce erreichen oder es wird eine separate Middleware benoetigt. Das ist betrieblich unguenstig, weil Authentifizierung, Logging, Fehlerdaten, Retry-Verhalten und Mapping nicht zentral im Agenten sichtbar sind.

Konkret fehlen:

- konfigurierbare Agent-HTTP-Routen, die als Integrationsendpunkte betrieben werden koennen
- Authentifizierung pro Endpoint-Connector, mindestens OAuth2 fuer maschinenlesbare Clients
- Request-Konfiguration pro Scheduler, inklusive Methode, Pfad, Payload-Typ und optionaler Validierung
- Uebergabe des Request-Payloads an die bestehende Mapping-Engine
- Salesforce-Schreibstrecke mit Run, Logs, Fehlerdaten und Monitoring wie bei bestehenden Inbound-Schedulern

## Zielbild

1. Operatoren koennen einen Connector vom Typ `ENDPOINT` oder `AGENT_ENDPOINT` anlegen. Der Connector beschreibt den Root-Pfad, Authentifizierung, erlaubte Clients und betriebliche Limits.
2. Operatoren koennen Scheduler mit Source Type `ENDPOINT` erstellen. Ein Scheduler definiert Methode und relativen Pfad, zum Beispiel `POST /orders`, sowie Request-Parsing und optionale Validierungsregeln.
3. Der Agent registriert aus aktiven Endpoint-Schedulern HTTP-Routen unterhalb des Connector-Root-Pfads und nimmt Requests ohne Polling entgegen.
4. Ein eingehender Request erzeugt einen normalen Run. Request-Body, Query-Parameter, Header-Auswahl und Metadaten stehen der Mapping-Engine als Source Record zur Verfuegung.
5. Das Ziel wird wie bei bestehenden Inbound-Flows ueber `targetType=SALESFORCE` oder `SALESFORCE_GLOBAL_PICKLIST` und `targetDefinition` beschrieben.
6. Erfolg und Fehler sind fuer den Aufrufer und im Monitoring beobachtbar. Erfolgreiche Requests liefern eine konfigurierte HTTP-Antwort, fehlerhafte Requests liefern passende Statuscodes und Run-/Korrelationsinformationen.
7. Bestehende REST-Source-Adapter fuer outbound HTTP bleiben rueckwaertskompatibel; der neue Endpoint-Typ ist fachlich inbound.

## Nicht-Ziele

- Kein Ersatz fuer den bestehenden outbound `REST_API` Source Adapter.
- Kein generischer API-Gateway mit Routing zu beliebigen Zielsystemen ausserhalb des Scheduler-Modells.
- Keine vollstaendige OpenAPI-Management-Loesung mit Developer Portal, Quotas pro Consumer oder Self-Service Keys.
- Kein asynchroner Message-Broker in dieser Spec. Requests werden zunaechst synchron angenommen und direkt oder kontrolliert im Agent-Prozess verarbeitet.
- Keine direkte Salesforce-Expose-API. Externe Systeme sprechen den Agenten an, nicht Salesforce.

## Akzeptanzkriterien

- [ ] Ein Connector kann als Agent-Endpunkt-Root mit eindeutigem Root-Pfad konfiguriert werden, zum Beispiel `/api/inbound/acme`.
- [ ] OAuth2 ist als Authentifizierungsmodus dokumentiert und technisch vorgesehen. Minimal unterstuetzt wird Bearer-Token-Validierung gegen konfigurierbaren Issuer/JWKS oder Introspection Endpoint.
- [ ] Ein Scheduler kann Source Type `ENDPOINT` verwenden und Methode, relativen Pfad, Content-Type, Body-Pfad und Antwortverhalten konfigurieren.
- [ ] Ein gueltiger Request erzeugt genau einen Run mit `sourceType=ENDPOINT`; Run, Logs, Records Read/Processed/Succeeded/Failed und Fehlerdaten sind im Monitoring sichtbar.
- [ ] Mapping kann auf Body, Query, Header-Auswahl und Request-Metadaten zugreifen.
- [ ] Salesforce-Zielverarbeitung nutzt die bestehenden Target Adapter und respektiert bestehende Importprofile, External-ID-Upsert-Regeln und Fehleraggregation.
- [ ] Authentifizierungsfehler erzeugen keinen Ziel-Run und liefern `401` oder `403`.
- [ ] Validierungs- oder Mappingfehler erzeugen einen fehlgeschlagenen oder partiell erfolgreichen Run und liefern einen konfigurierten Fehlerstatus, standardmaessig `400` oder `422`.
- [ ] Deployment- und Sicherheitsfolgen sind dokumentiert: erreichbarer Agent-Port, TLS/Reverse Proxy, Secrets, Log-Redaction und Rate Limits.

## Umsetzungsskizze

Betroffene Bereiche im Repo:

- `src/server/`: HTTP-Routing fuer Endpoint-Connectoren und aktive Endpoint-Scheduler
- `src/agent/`: Run-Erzeugung aus eingehenden Requests, Run-Kontext, Logging und Fehlerbehandlung
- `src/core/job-runner/`: Source-Adapter oder Executor fuer bereits empfangene Request-Records
- `src/source-adapters/endpoint/`: neuer Adapter fuer Request-Payloads als Source Records
- `src/core/scheduler/job-executor-factory.ts`: neuer Executor-Pfad fuer `sourceType=ENDPOINT`
- `src/client/`: Connector- und Scheduler-UI fuer Endpoint-Konfiguration
- `src/server/admin-data-service.ts`: Persistenz und Validierung der neuen Connector-/Scheduler-Definitionen
- `salesforce/metadata/`: Picklist-Werte fuer Connector Type und Source Type, falls in Salesforce-Metadaten gepflegt
- `docs/`: Betriebsdoku fuer Reverse Proxy, OAuth2 und Beispielkonfigurationen

### Connector-Konfiguration

Connector Type:

- Vorschlag: `AGENT_ENDPOINT`
- Alternativ: `ENDPOINT`, falls die UI bewusst kurze Typnamen bevorzugt

Connector-Parameter:

```json
{
  "rootPath": "/api/inbound/acme",
  "authType": "oauth2",
  "oauth2": {
    "mode": "jwks",
    "issuer": "https://login.example.com/",
    "audience": "sf-onprem-agent",
    "jwksUrl": "https://login.example.com/.well-known/jwks.json",
    "requiredScopes": ["integration.write"]
  },
  "limits": {
    "maxBodyBytes": 1048576,
    "timeoutMs": 30000,
    "maxConcurrentRequests": 10
  },
  "trustedHeaders": ["x-request-id", "x-correlation-id"]
}
```

OAuth2-Modi:

- `jwks`: lokale JWT-Pruefung mit Issuer, Audience, Expiry und Signatur
- `introspection`: Token-Pruefung gegen Introspection Endpoint, wenn JWT lokal nicht pruefbar ist
- `none`: nur fuer lokale Entwicklung oder explizit als unsicher markierte Test-Connectoren

Secrets:

- Client Secrets, Introspection Credentials oder API Secrets duerfen nicht im Klartext in Salesforce-Konfigurationen landen.
- Bestehendes `MSD_SecretKey__c`-Muster wird weiterverwendet, wo Secrets benoetigt werden.

### Scheduler-Konfiguration

Source Type:

- `ENDPOINT`

Source Definition:

```json
{
  "method": "POST",
  "path": "/orders",
  "contentType": "application/json",
  "recordMode": "single",
  "bodyPath": "",
  "queryFields": ["sourceSystem", "tenant"],
  "headerFields": ["x-request-id"],
  "response": {
    "successStatus": 202,
    "successBody": {
      "status": "accepted",
      "runId": "{{runId}}",
      "correlationId": "{{correlationId}}"
    },
    "errorStatus": 422
  },
  "validation": {
    "requiredBodyFields": ["orderNumber", "customerNumber"]
  }
}
```

Record-Modi:

- `single`: kompletter Body ist ein Source Record
- `array`: Body oder `bodyPath` muss ein Array liefern; jedes Element wird ein Source Record
- `envelope`: Body enthaelt Metadaten und ein Array unter `bodyPath`

Source Record Shape fuer Mapping:

```json
{
  "body": {
    "orderNumber": "4711",
    "customerNumber": "100200"
  },
  "query": {
    "tenant": "prod"
  },
  "headers": {
    "x-request-id": "abc-123"
  },
  "request": {
    "method": "POST",
    "path": "/api/inbound/acme/orders",
    "receivedAt": "2026-06-08T10:00:00.000Z",
    "remoteAddress": "10.0.0.5"
  },
  "auth": {
    "subject": "client-id",
    "scopes": ["integration.write"]
  }
}
```

Mapping-Beispiele:

```json
[
  {
    "sourceField": "body.orderNumber",
    "targetField": "External_Order_Number__c",
    "targetType": "string",
    "transform": { "type": "NONE" }
  },
  {
    "sourceField": "body.customerNumber",
    "targetField": "Account__c",
    "targetType": "string",
    "transform": {
      "type": "LOOKUP",
      "lookupObject": "Account",
      "lookupField": "CustomerNumber__c"
    }
  }
]
```

### Routing und Konfliktregeln

- Root-Pfade muessen eindeutig pro aktivem Endpoint-Connector sein.
- Relative Scheduler-Pfade muessen innerhalb eines Root-Pfads pro HTTP-Methode eindeutig sein.
- Inaktive Connectoren oder Scheduler registrieren keine Route.
- Pfadparameter koennen spaeter ergaenzt werden; fuer die erste Umsetzung reicht statisches Routing.
- Bei Konfigurationskonflikten startet der Agent weiter, registriert die betroffene Route nicht und schreibt einen klaren WARN-/ERROR-Logeintrag.

### Run-Verhalten

- Jeder akzeptierte Request erzeugt einen Run mit eigener `correlationId`.
- Wenn ein Header wie `x-correlation-id` konfiguriert und vorhanden ist, kann er als externe Korrelations-ID uebernommen werden.
- `recordsRead` entspricht der Anzahl erzeugter Source Records.
- `recordsProcessed`, `recordsSucceeded` und `recordsFailed` folgen dem bestehenden `DataTransferJob`.
- Failed Records enthalten den relevanten Source Record, aber sensible Header und Tokens werden redacted.
- Scheduler-Timing ist fuer Endpoint-Scheduler optional. Aktive Endpoint-Scheduler sind nicht due-basiert, sondern event-basiert.

### Antwortverhalten

- Standard bei komplettem Erfolg: `202 Accepted` mit `runId` und `correlationId`.
- Standard bei fachlichen Fehlern: `422 Unprocessable Entity` mit `runId`, `correlationId` und einer gekuerzten Fehlermeldung.
- Standard bei Auth-Fehlern: `401 Unauthorized` oder `403 Forbidden`, ohne Run.
- Standard bei unbekannter Route: `404 Not Found`, ohne Run.
- Wenn synchrone Zielverarbeitung zu lange dauert, soll die Spec-Umsetzung eine spaetere Option fuer `asyncAccepted=true` vorbereiten, aber nicht zwingend implementieren.

### Sicherheit und Betrieb

- Der Endpoint-Betrieb erfordert TLS vor dem Agenten, entweder direkt oder ueber Reverse Proxy.
- OAuth2-Token und Authorization Header duerfen nie in Logs, failed-records oder UI-Exports sichtbar sein.
- Request-Body-Groessenlimit ist Pflicht.
- Rate-/Concurrency-Limits sind pro Connector vorzusehen.
- Admin-UI muss anzeigen, welche Endpoint-Routen aktiv sind.
- Health/Readiness sollte Konfigurationsfehler fuer Endpoint-Routen sichtbar machen.

## Aufgaben

- [x] Spec mit Domainenwissen vervollstaendigen.
- [x] Namen final entscheiden: `AGENT_ENDPOINT` als Connector Type, `ENDPOINT` als Scheduler Source Type.
- [x] OAuth2-Modus fuer erste Umsetzung festlegen: Bearer Token via Introspection Endpoint oder Secret/Env-Token; JWKS bleibt Folgeausbau.
- [ ] Salesforce-Metadaten fuer neue Picklist-Werte pruefen und ergaenzen.
- [x] Endpoint-Connector-Validierung im Admin Data Service definieren.
- [x] Endpoint-Scheduler-SourceDefinition parser/validator implementieren.
- [x] Request-Source-Adapter und Executor in die Job Factory integrieren.
- [x] Server-Routing fuer aktive Endpoint-Connectoren/Scheduler implementieren.
- [x] UI-Felder fuer Connector und Scheduler ergaenzen.
- [ ] Redaction fuer Header, Tokens und konfigurierte Body-Felder ergaenzen.
- [x] Beispielkonfiguration und Testszenarien dokumentieren.
- [x] Automatisierten Source-Adapter-Smoke-Test fuer Endpoint-Requests anlegen.
- [x] Manuellen Endpoint-Smoke-Test mit Test-Connector und Salesforce-Ziel in der Annaburger Test-Sandbox ausfuehren.

## Verifikation

- Build oder schmaler Smoke-Test: `npm run build`
- Spec-Check: `npm run spec:validate`
- Beispielkonfiguration: `docs/specs/2026-06-08-agent-als-konfigurierbarer-endpunkt-fuer-scheduler-requests/example-order-endpoint.md`
- Testplan: `docs/specs/2026-06-08-agent-als-konfigurierbarer-endpunkt-fuer-scheduler-requests/test-scenarios.md`
- Source-Adapter-Smoke-Test: `npm run endpoint:smoke`
- Automatisierter Smoke-Test: lokaler POST auf konfigurierten Endpoint mit gueltigem OAuth2-Testtoken oder Testmodus, danach Run und Salesforce-Zielergebnis pruefen.
- Fehler-Smoke-Test: Request ohne Token liefert `401`/`403` und erzeugt keinen Run.
- Validierungs-Smoke-Test: Request ohne Pflichtfeld liefert `422`, erzeugt einen Run mit failed-records und redacted Headern.
- Manuelle Checks in Web UI oder Agent: Connector sichtbar, Scheduler Type `ENDPOINT` waehlbar, aktive Route im Monitoring erkennbar.
- Betriebsrelevante Beobachtung nach Deploy: Reverse Proxy/TLS, Body-Limit, Auth-Fehler, Route-Konflikte und Run-Logs pruefen.

## Offene Fragen

- Soll der erste produktive Modus Requests synchron bis Salesforce verarbeitet haben oder standardmaessig nur annehmen und intern asynchron ausfuehren?
- Muss der Agent Pfadparameter wie `/orders/:id` in der ersten Umsetzung unterstuetzen?
- Soll OAuth2 ausschliesslich JWT/JWKS validieren oder auch Token Introspection anbieten?
- Werden mehrere Tenants ueber einen Connector oder ueber getrennte Connectoren modelliert?
- Muss die Antwort Payload-Daten aus Salesforce enthalten, zum Beispiel erzeugte Salesforce IDs?

## Status

- Status: draft
- Letzte Entscheidung: MVP verwendet `AGENT_ENDPOINT` Connectoren, `ENDPOINT` Scheduler-Quellen und OAuth2-Bearer-Validierung per Introspection oder Secret/Env-Token.
- Naechster Schritt: Salesforce-Metadaten/Picklist-Werte pruefen und Redaction ergaenzen.
