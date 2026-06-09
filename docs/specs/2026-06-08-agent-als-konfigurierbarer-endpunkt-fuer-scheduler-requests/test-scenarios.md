# Testszenarien: Agent-Endpunkt-Scheduler

## Automatisierter Smoke-Test

Ziel: Parser, Pflichtfeldvalidierung und Source-Record-Erzeugung ohne Salesforce-Verbindung pruefen.

Command:

```bash
npm run endpoint:smoke
```

Erwartung:

- `single` Request erzeugt einen Source Record mit `body`, `query`, `headers`, `request` und `auth`.
- `array` Request mit `bodyPath` erzeugt mehrere Source Records.
- Fehlendes Pflichtfeld wird als Fehler erkannt.
- Ungueltiger Array-Body wird als Fehler erkannt.

## Manueller Smoke-Test mit Agent und Salesforce

Vorbedingungen:

- Salesforce-Konfiguration ist im Web-/Agent-Prozess gesetzt.
- Connector `AGENT_ENDPOINT` ist aktiv.
- Env-Variable aus `MSD_SecretKey__c`, zum Beispiel `ORDER_ENDPOINT_BEARER_TOKEN`, ist gesetzt.
- Scheduler mit `sourceType=ENDPOINT`, `targetType=SALESFORCE`, Mapping und Target Definition ist aktiv.

Schritte:

1. Web-Service starten: `npm run dev:web`
2. Request senden:

```bash
curl -i \
  -X POST "http://localhost:8080/api/inbound/orders/v1?tenant=test" \
  -H "Authorization: Bearer ${ORDER_ENDPOINT_BEARER_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "x-request-id: smoke-order-1" \
  -d '{"orderNumber":"smoke-order-1","customerNumber":"100200","amount":42.5}'
```

Erwartung:

- HTTP Status ist `202`, wenn der Salesforce-Import erfolgreich war.
- Response enthaelt `runId`, `correlationId` und `scheduleId`.
- Monitoring zeigt einen neuen Run fuer den Endpoint-Scheduler.
- Failed Records sind leer.
- Salesforce enthaelt den upserteten Datensatz.

## Connector-Test ueber Postman

Vorbedingung:

- `AGENT_API_TOKEN` ist im Web-Prozess gesetzt.

Postman-Konfiguration:

- Method: `POST`
- URL: `http://localhost:8080/api/connectors/<connectorId>/test`
- Authorization: `Bearer Token`
- Token: Wert aus `AGENT_API_TOKEN`
- Body: leer

Alternativ per curl:

```bash
curl -i \
  -X POST "http://localhost:8080/api/connectors/<connectorId>/test" \
  -H "Authorization: Bearer ${AGENT_API_TOKEN}"
```

Erwartung:

- HTTP Status ist `200`, wenn die Connector-Konfiguration valide ist.
- Der Request benoetigt kein Admin-Cookie und keinen CSRF-Token.
- Ohne gueltigen Bearer Token bleibt der normale Admin-Schutz aktiv.

## Postman Collection aus Endpoint-Connector exportieren

Vorbedingungen:

- Connector `AGENT_ENDPOINT` ist angelegt.
- Mindestens ein Scheduler mit `sourceType=ENDPOINT` ist diesem Connector zugeordnet.

Schritte:

1. Web UI oeffnen.
2. Im Connector-Panel beim Endpoint-Connector `Postman` klicken.
3. Die heruntergeladene Collection in Postman importieren.
4. Collection-Variablen `baseUrl` und `bearerToken` setzen.

Erwartung:

- Die Collection enthaelt einen Request pro zugeordnetem Endpoint-Scheduler.
- Methode, Root-Pfad, Scheduler-Pfad, Query-Felder, Header-Felder und Beispiel-Body stammen aus der Scheduler SourceDefinition.
- Die Collection-Variable `baseUrl` ist mit `publicBaseUrl`, `externalBaseUrl`, fallbackweise `baseUrl` aus dem Connector oder der aktuellen Web-UI-Origin vorbelegt.
- Jeder Request enthaelt ein vollstaendiges Postman-URL-Objekt mit `{{baseUrl}}`, Host und Pfad.
- Bearer Token wird nur als Variable referenziert und nicht exportiert.

## Auth-Fehler

Request ohne Bearer Token:

```bash
curl -i \
  -X POST "http://localhost:8080/api/inbound/orders/v1" \
  -H "Content-Type: application/json" \
  -d '{"orderNumber":"auth-fail","customerNumber":"100200"}'
```

Erwartung:

- HTTP Status ist `401`.
- Es wird kein Run erzeugt.
- Kein Token erscheint in Logs oder UI.

Request mit falschem Bearer Token:

```bash
curl -i \
  -X POST "http://localhost:8080/api/inbound/orders/v1" \
  -H "Authorization: Bearer wrong-token" \
  -H "Content-Type: application/json" \
  -d '{"orderNumber":"auth-fail","customerNumber":"100200"}'
```

Erwartung:

- HTTP Status ist `403`.
- Es wird kein Run erzeugt.

## Validierungsfehler

Request ohne Pflichtfeld `customerNumber`:

```bash
curl -i \
  -X POST "http://localhost:8080/api/inbound/orders/v1" \
  -H "Authorization: Bearer ${ORDER_ENDPOINT_BEARER_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "x-request-id: validation-fail-1" \
  -d '{"orderNumber":"validation-fail-1"}'
```

Erwartung:

- HTTP Status ist `422`.
- Fehlertext benennt das fehlende Pflichtfeld.
- Im MVP wird dieser Fehler vor dem Run erkannt; Zieladapter wird nicht aufgerufen.

## Mapping- oder Salesforce-Fehler

Request mit gueltiger Authentifizierung, aber fachlich nicht importierbarem Wert:

```bash
curl -i \
  -X POST "http://localhost:8080/api/inbound/orders/v1" \
  -H "Authorization: Bearer ${ORDER_ENDPOINT_BEARER_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "x-request-id: sf-fail-1" \
  -d '{"orderNumber":"sf-fail-1","customerNumber":"unknown","amount":42.5}'
```

Erwartung:

- HTTP Status ist `422`.
- Response enthaelt `runId`.
- Monitoring zeigt Run mit `Failed` oder `Partial Success`.
- Failed Records enthalten den betroffenen Source Record ohne Authorization Header.

## Route-Konflikt

Zwei aktive Scheduler verwenden dieselbe Kombination aus Connector Root, Methode und Pfad.

Erwartung:

- Der erste passende Scheduler wird verwendet.
- Folgeausbau: Konfigurationskonflikt soll im Health-/Readiness-Status sichtbar werden.

## Betriebscheck

- Reverse Proxy leitet `/api/inbound/...` an den Web-/Agent-Port weiter.
- TLS ist vor dem Agenten aktiv.
- Body-Limit blockiert uebergrosse Requests.
- `Authorization` Header wird nicht in `failed-records` exportiert.
- Endpoint-Scheduler laufen nicht im normalen Polling, sondern nur ereignisbasiert.
