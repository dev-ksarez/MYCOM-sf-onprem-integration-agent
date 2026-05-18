# Agent API Contract

## Zweck

Dieses Dokument definiert den ersten versionierten Vertrag zwischen lokalem Agent und SaaS-Control-Plane. Der Vertrag ist bewusst stabil und rueckwaertskompatibel geschnitten, damit bestehende Agenten im Legacy-Modus weiterlaufen koennen.

## Grundsaetze

- API-Prefix: `/api/agent/v1`
- Transport: HTTPS
- Richtung: Agent ruft SaaS ausgehend auf.
- Auth: Registrierung ueber kurzlebiges Registration Token; danach Agent-Credentials.
- Mandantentrennung: jede Agent-Credential ist genau einem Tenant und mindestens einem Projekt zugeordnet.
- Idempotenz: schreibende Agent-Requests senden `idempotencyKey`.
- Offline-Faehigkeit: Agent puffert Heartbeat-Status, Runs, Logs und Fehlerdaten lokal begrenzt und uebertraegt spaeter nach.
- Datenschutz: keine lokalen Secrets; Fehlerdaten nur minimiert und nach Projektpolicy.

## Auth Flow

### 1. Registration Token erzeugen

Ein SaaS-Benutzer erzeugt im Projekt ein kurzlebiges Registration Token. Dieses Token wird im lokalen Agenten eingegeben.

### 2. Agent claimt Registrierung

`POST /api/agent/v1/registrations/claim`

Request:

```json
{
  "registrationToken": "rt_...",
  "agentInstallationId": "local-stable-installation-id",
  "agentVersion": "0.2.53",
  "hostFingerprint": "sha256:...",
  "capabilities": [
    "heartbeat",
    "run-reporting",
    "failed-records",
    "config-fetch"
  ],
  "preferredMode": "hybrid"
}
```

Response:

```json
{
  "agentId": "agt_01H...",
  "tenantId": "ten_01H...",
  "projectId": "prj_01H...",
  "mode": "hybrid",
  "credential": {
    "type": "bearer",
    "accessToken": "aat_...",
    "expiresAt": "2026-05-15T12:00:00Z",
    "refreshToken": "art_..."
  }
}
```

Regeln:

- Registration Token ist einmalig nutzbar.
- SaaS legt Agent-Zuordnung und Audit-Event an.
- Bereits registrierte `agentInstallationId` darf nur mit expliziter Rebind-Freigabe neu geclaimt werden.

## Heartbeat

`POST /api/agent/v1/heartbeats`

Request:

```json
{
  "idempotencyKey": "hb-agt_01H-20260515T101500Z",
  "agentId": "agt_01H...",
  "projectId": "prj_01H...",
  "mode": "hybrid",
  "agentVersion": "0.2.53",
  "startedAt": "2026-05-15T08:00:00Z",
  "sentAt": "2026-05-15T10:15:00Z",
  "status": "online",
  "capabilities": [
    "heartbeat",
    "run-reporting",
    "failed-records"
  ],
  "config": {
    "desiredVersion": null,
    "appliedVersion": null,
    "lastRunVersion": "legacy-local"
  },
  "runtime": {
    "os": "windows",
    "nodeVersion": "20.x",
    "schedulerCount": 12
  }
}
```

Response:

```json
{
  "accepted": true,
  "serverTime": "2026-05-15T10:15:02Z",
  "commandsAvailable": false,
  "minimumSupportedAgentVersion": "0.2.50"
}
```

## Config Fetch

Nur fuer Agenten im SaaS-Modus oder vorbereiteten Hybrid-Modus.

`GET /api/agent/v1/projects/{projectId}/config-bundle?knownVersion=cfg_42`

Response wenn aktuell:

```json
{
  "changed": false,
  "version": "cfg_42"
}
```

Response bei neuer Version:

```json
{
  "changed": true,
  "version": "cfg_43",
  "mode": "saas",
  "releasedAt": "2026-05-15T10:00:00Z",
  "schedulers": [
    {
      "id": "sch_060",
      "name": "SAGE Kunden -> Salesforce Account",
      "enabled": true,
      "sourceConnectorRef": "con_sage_mssql",
      "targetConnectorRef": "con_salesforce",
      "operation": "upsert",
      "timing": {
        "days": [1, 2, 3, 4, 5],
        "intervalMinutes": 15,
        "startTime": "08:00"
      },
      "sourceDefinition": {
        "type": "mssql_sql",
        "query": "SELECT Adresse, Matchcode, Name1 FROM dbo.KHKAdressen WHERE Aktiv = -1"
      },
      "targetDefinition": {
        "type": "salesforce_sobject",
        "sobject": "Account",
        "externalIdField": "ERP_Account_Number__c"
      },
      "mapping": [
        {
          "target": "Name",
          "source": "Name1",
          "type": "string",
          "transform": "TRIM"
        }
      ]
    }
  ]
}
```

Regeln:

- Der Agent darf eine neue Version erst als `appliedVersion` melden, nachdem lokale Validierung erfolgreich war.
- Bei Validierungsfehler bleibt die vorige Version aktiv; Agent meldet Fehler per Run/Log.
- SaaS zeigt Drift, wenn `desiredVersion` und `appliedVersion` auseinanderlaufen.

## Run Reporting

### Run starten

`POST /api/agent/v1/runs`

Request:

```json
{
  "idempotencyKey": "run-agt_01H-sch_060-20260515T101500Z",
  "agentId": "agt_01H...",
  "projectId": "prj_01H...",
  "schedulerId": "sch_060",
  "configVersion": "cfg_43",
  "startedAt": "2026-05-15T10:15:00Z",
  "direction": "inbound",
  "source": "sage",
  "target": "salesforce",
  "status": "running"
}
```

Response:

```json
{
  "runId": "run_01H...",
  "accepted": true
}
```

### Run abschliessen

`PATCH /api/agent/v1/runs/{runId}`

Request:

```json
{
  "finishedAt": "2026-05-15T10:15:22Z",
  "status": "failed",
  "counters": {
    "read": 100,
    "written": 92,
    "skipped": 0,
    "failed": 8
  },
  "errorSummary": {
    "category": "technical",
    "message": "Salesforce field validation failed"
  }
}
```

## Logs

`POST /api/agent/v1/runs/{runId}/logs:batch`

Request:

```json
{
  "idempotencyKey": "logs-run_01H-001",
  "events": [
    {
      "occurredAt": "2026-05-15T10:15:10Z",
      "level": "error",
      "code": "RECORD_ERROR",
      "message": "FIELD_INTEGRITY_EXCEPTION for ERP_CONTACT_NUMBER__c",
      "recordKey": "kontakt1@example.org"
    }
  ]
}
```

Logging-Regeln:

- Keine periodischen Erfolg-ohne-Wirkung-Ereignisse in SaaS-Monitor schreiben.
- Uebertragen werden Fehler, Warnungen, Konfigurationswechsel, Starts/Stops mit Datensatzwirkung und relevante Diagnoseereignisse.
- Salesforce-Logobjekte erhalten diese rein operativen SaaS-Monitorereignisse nicht mehr, sofern sie keine Integrationsfachdaten darstellen.

## Fehlerdaten

`POST /api/agent/v1/runs/{runId}/failed-records`

Request:

```json
{
  "idempotencyKey": "failed-run_01H-001",
  "records": [
    {
      "key": "kontakt1@example.org",
      "status": "TECHNICAL_ERROR",
      "errorCode": "FIELD_INTEGRITY_EXCEPTION",
      "message": "ERP_CONTACT_NUMBER__c exceeds maximum length",
      "source": {
        "Nummer": "AP0001",
        "Email": "kontakt1@example.org"
      },
      "mapped": {
        "ERP_CONTACT_NUMBER__c": "kontakt1@example.org",
        "Email": "kontakt1@example.org"
      }
    }
  ]
}
```

Regeln:

- `source` und `mapped` muessen in der SaaS-UI als formatierte JSON-Bloecke angezeigt werden.
- Projektpolicy kann Payload-Felder maskieren oder nur Schluesselfelder erlauben.
- Grosse Mengen koennen in Batches gesendet werden.

## Commands

`GET /api/agent/v1/commands/poll?agentId=agt_01H...`

Response:

```json
{
  "commands": [
    {
      "commandId": "cmd_01H...",
      "type": "refresh-config",
      "createdAt": "2026-05-15T10:20:00Z",
      "payload": {
        "targetVersion": "cfg_43"
      }
    }
  ]
}
```

`POST /api/agent/v1/commands/{commandId}/ack`

Request:

```json
{
  "agentId": "agt_01H...",
  "status": "accepted",
  "message": "Config refresh queued"
}
```

Initial erlaubte Commands:

- `refresh-config`
- `run-scheduler-now`
- `rotate-credential`
- `disable-saas-mode`

## Fehler und Statuscodes

| Status | Bedeutung |
| --- | --- |
| 200 | gelesen oder aktualisiert |
| 201 | Ressource erstellt |
| 202 | angenommen, asynchrone Verarbeitung |
| 400 | ungueltiger Request |
| 401 | nicht authentifiziert |
| 403 | Agent nicht fuer Tenant/Projekt berechtigt |
| 409 | Versionskonflikt oder Idempotenzkonflikt |
| 410 | Agent-Registrierung widerrufen |
| 422 | fachlich nicht validierbare Konfiguration |
| 429 | Rate Limit |
| 500 | Serverfehler |

## Retry-Regeln

- 5xx und 429 werden mit Backoff erneut versucht.
- 401 fuehrt zu Credential Refresh; bei erneutem Fehler geht Agent in `hybrid-degraded`.
- 403/410 deaktivieren SaaS-Kommunikation fuer das betroffene Projekt und erzeugen lokalen Diagnoseeintrag.
- Schreibende Requests duerfen durch `idempotencyKey` wiederholt werden.

## Mindestkompatibilitaet

Ein Agent gilt als SaaS-faehig, wenn er diese Funktionen unterstuetzt:

- Registrierung claimen.
- Heartbeat senden.
- Run starten und abschliessen melden.
- Logs in Batches senden.
- Fehlerdaten in Batches senden.
- Konfigurationsversion melden.
- SaaS-Verbindung verlieren, ohne Legacy-Ausfuehrung zu brechen.
