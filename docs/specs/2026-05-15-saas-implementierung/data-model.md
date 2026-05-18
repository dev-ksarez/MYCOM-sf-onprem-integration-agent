# SaaS Datenmodell

## Zweck

Dieses Dokument beschreibt das fachliche Startmodell fuer die SaaS-Control-Plane. Es ist fuer PostgreSQL im 1Blue-Pilot geeignet und soll spaeter ohne fachlichen Umbau auf AWS migrierbar bleiben.

## Mandant und Benutzer

### Tenant

Repraesentiert einen Kunden oder Betreiber-Mandanten.

Kernfelder:

- `id`
- `name`
- `status`: `active`, `suspended`, `deleted`
- `createdAt`
- `updatedAt`

Regeln:

- Jede projektbezogene Ressource gehoert zu genau einem Tenant.
- Loeschung erfolgt zunaechst logisch, damit Audit und historische Runs erhalten bleiben.

### User

Repraesentiert einen SaaS-Benutzer.

Kernfelder:

- `id`
- `email`
- `displayName`
- `status`
- `lastLoginAt`
- `createdAt`
- `updatedAt`

### TenantMembership

Verknuepft Benutzer mit Mandanten.

Kernfelder:

- `tenantId`
- `userId`
- `role`
- `projectScope`: optional eingeschraenkte Projektliste

Rollen initial:

- `owner`
- `admin`
- `operator`
- `viewer`
- `support`

## Lizenzierung

### License

Definiert gebuchte Funktionen und Limits eines Tenants. Fuehrende Quelle fuer Anlage und Parameter ist der MYCOM-Salesforce-Kundenbereich; SaaS haelt eine synchronisierte Kopie fuer Portal, Berechtigungen und Limitpruefung.

Kernfelder:

- `id`
- `tenantId`
- `contractReference`
- `plan`
- `status`: `trial`, `active`, `expired`, `suspended`
- `validFrom`
- `validUntil`
- `limitsJson`
- `featuresJson`
- `sourceSystem`: `mycom_salesforce`
- `sourceRecordId`
- `syncedAt`

Beispiel fuer Limits:

```json
{
  "projects": 5,
  "agents": 20,
  "schedulers": 200,
  "users": 25,
  "failedRecordRetentionDays": 30
}
```

Regeln:

- Lizenzverletzungen blockieren neue Aktivierungen oder neue Ressourcen.
- Bereits laufende Scheduler werden nicht hart mitten in einer Uebertragung abgebrochen.
- Lizenzentscheidungen werden im Audit protokolliert.
- Manuelle Aenderungen im SaaS sind nur fuer technische Korrekturen mit Audit erlaubt; fachliche Vertragsparameter werden in MYCOM Salesforce gepflegt.

### ContractSnapshot

Repraesentiert die im Kundenportal sichtbare Vertragssicht. Die Daten stammen aus dem MYCOM-Salesforce-Kundenbereich.

Kernfelder:

- `id`
- `tenantId`
- `contractReference`
- `customerNumber`
- `displayName`
- `status`
- `validFrom`
- `validUntil`
- `plan`
- `modulesJson`
- `parametersJson`
- `sourceSystem`: `mycom_salesforce`
- `sourceRecordId`
- `syncedAt`
- `syncStatus`: `ok`, `stale`, `failed`
- `notes`
- `updatedAt`

Regeln:

- Vertragssicht ist fuer Kunden lesbar, aber nicht durch Kunden selbst aenderbar.
- Fachliche Aenderungen erfolgen im MYCOM-Salesforce-Kundenbereich.
- Synchronisationen und technische Korrekturen werden auditiert.
- Rechnungs- und Paymentdaten sind nicht Bestandteil des ersten Modells.

### ContractSyncEvent

Protokolliert die Synchronisation aus dem MYCOM-Salesforce-Kundenbereich.

Kernfelder:

- `id`
- `tenantId`
- `sourceSystem`: `mycom_salesforce`
- `sourceRecordId`
- `status`: `success`, `warning`, `failed`
- `startedAt`
- `finishedAt`
- `message`
- `detailsJson`

## Projekte und Instanzen

### Project

Repraesentiert ein Integrationsprojekt innerhalb eines Tenants.

Kernfelder:

- `id`
- `tenantId`
- `name`
- `mode`: `legacy`, `hybrid`, `saas`
- `status`
- `defaultTimezone`
- `createdAt`
- `updatedAt`

### Instance

Repraesentiert eine fachliche Ziel-/Quellinstanz wie Salesforce Sandbox, Salesforce Production oder lokale ERP-Umgebung als Metadatum.

Kernfelder:

- `id`
- `tenantId`
- `projectId`
- `type`: `salesforce`, `sage100`, `mssql`, `file`, `rest`, `other`
- `name`
- `environment`: `dev`, `test`, `prod`, `sandbox`
- `metadataJson`
- `secretStatus`: `local-only`, `missing`, `available-local`

Regeln:

- Secrets selbst werden nicht zentral gespeichert.
- `secretStatus` beschreibt nur, ob der lokale Agent eine noetige Secret-Konfiguration hat.

## Agenten

### Agent

Repraesentiert eine registrierte lokale Agent-Installation.

Kernfelder:

- `id`
- `tenantId`
- `projectId`
- `installationId`
- `name`
- `mode`
- `status`: `pending`, `online`, `offline`, `revoked`
- `agentVersion`
- `capabilitiesJson`
- `lastHeartbeatAt`
- `desiredConfigVersionId`
- `appliedConfigVersionId`
- `lastRunConfigVersionId`

Regeln:

- Ein Agent kann fuer den ersten Schritt genau einem Projekt zugeordnet werden.
- Mehrprojektfaehigkeit kann spaeter ueber AgentProjectAssignment ergaenzt werden.
- Widerruf stoppt SaaS-Kommunikation, aber nicht automatisch lokale Legacy-Ausfuehrung.

### AgentCredential

Speichert gehashte/verschluesselte Agent-Credential-Metadaten.

Kernfelder:

- `id`
- `agentId`
- `credentialHash`
- `status`
- `expiresAt`
- `rotatedAt`
- `createdAt`

## Downloads und Installation

### InstallerArtifact

Repraesentiert ein downloadbares Agent-Setup.

Kernfelder:

- `id`
- `version`
- `platform`: `windows`, `linux`, `macos`
- `channel`: `stable`, `preview`
- `fileName`
- `storageKey`
- `sha256`
- `signatureStatus`
- `releaseNotesUrl`
- `createdAt`

Regeln:

- Downloads sind authentifiziert.
- Artefakte werden nicht mandantenspezifisch veraendert.
- Mandantenspezifische Bootstrap-Daten werden getrennt erzeugt.

### BootstrapConfig

Repraesentiert eine kurzlebige projektspezifische Bootstrap-Konfiguration fuer die Installation.

Kernfelder:

- `id`
- `tenantId`
- `projectId`
- `agentId`
- `createdBy`
- `expiresAt`
- `payloadJson`
- `downloadedAt`

Regeln:

- `payloadJson` enthaelt keine lokalen Secrets.
- Registration Tokens sind kurzlebig und koennen separat vom Installer erzeugt werden.
- Bootstrap-Downloads werden auditiert.

### DownloadAudit

Protokolliert Installer- und Bootstrap-Downloads.

Kernfelder:

- `id`
- `tenantId`
- `userId`
- `artifactId`
- `bootstrapConfigId`
- `downloadType`: `installer`, `bootstrap`
- `ipAddress`
- `userAgent`
- `downloadedAt`

## Konfiguration

### Connector

Beschreibt einen Connector ohne lokale Secrets.

Kernfelder:

- `id`
- `tenantId`
- `projectId`
- `instanceId`
- `type`
- `name`
- `metadataJson`
- `secretPolicy`
- `createdAt`
- `updatedAt`

### ConfigVersion

Versionierter Container fuer freigegebene Projektkonfiguration.

Kernfelder:

- `id`
- `tenantId`
- `projectId`
- `versionNumber`
- `status`: `draft`, `released`, `active`, `retired`
- `createdBy`
- `releasedBy`
- `releasedAt`
- `checksum`
- `notes`
- `validationStatus`: `not_run`, `valid`, `warning`, `invalid`
- `validationJson`

Regeln:

- Nur eine Version je Projekt ist `active`.
- Jede Aktivierung erzeugt Audit.
- Rollback aktiviert eine fruehere oder neu erzeugte Version kontrolliert.
- Kundenbearbeitung erzeugt zunaechst `draft` Versionen.
- Nur validierte Versionen duerfen `released` oder `active` werden.
- Fehlerhafte Entwuerfe bleiben speicherbar, aber nicht aktivierbar.

### Scheduler

Fachlicher Scheduler innerhalb einer ConfigVersion.

Kernfelder:

- `id`
- `tenantId`
- `projectId`
- `configVersionId`
- `schedulerKey`
- `name`
- `enabled`
- `operation`: `insert`, `update`, `upsert`, `delete`
- `direction`: `inbound`, `outbound`
- `sourceConnectorId`
- `targetConnectorId`
- `timingJson`
- `sourceDefinitionJson`
- `targetDefinitionJson`
- `batchSize`

### Mapping

Feldmapping eines Schedulers.

Kernfelder:

- `id`
- `tenantId`
- `projectId`
- `configVersionId`
- `schedulerId`
- `targetField`
- `sourceExpression`
- `dataType`
- `transform`
- `required`
- `order`

Regeln:

- Mapping validiert gegen bekannte Zielmetadaten, soweit vorhanden.
- Alte Zielspalten duerfen nach Konfigurationsaenderung nicht aus Cache-Versionen in die Ausfuehrung gelangen.

### ConfigChangeRequest

Repraesentiert eine vom Kunden gestartete Konfigurationsaenderung fuer Connectoren, Scheduler oder Mapping.

Kernfelder:

- `id`
- `tenantId`
- `projectId`
- `baseConfigVersionId`
- `draftConfigVersionId`
- `createdBy`
- `status`: `draft`, `validating`, `ready`, `rejected`, `released`, `cancelled`
- `summary`
- `createdAt`
- `updatedAt`

Regeln:

- Aenderungen werden niemals direkt auf aktiver Konfiguration gespeichert.
- Freigabe erzeugt oder aktiviert eine ConfigVersion.
- Jede Statusaenderung wird auditiert.

### AiConfigSuggestion

Repraesentiert einen KI-Vorschlag zur Pruefung oder Anpassung bestehender Konfigurationen.

Kernfelder:

- `id`
- `tenantId`
- `projectId`
- `schedulerId`
- `baseConfigVersionId`
- `draftConfigVersionId`
- `prompt`
- `intent`: `inspect`, `modify`, `create`, `validate`
- `status`: `draft`, `validated`, `needs_input`, `rejected`, `accepted`
- `diffJson`
- `assumptionsJson`
- `validationJson`
- `createdBy`
- `createdAt`
- `updatedAt`

Regeln:

- Vorschlaege referenzieren immer eine Ausgangsversion.
- Prompt und Diff werden auditiert, duerfen aber keine lokalen Secrets enthalten.
- Akzeptieren eines Vorschlags uebernimmt ihn in eine Draft-ConfigVersion, aktiviert ihn aber nicht direkt.

## Runs, Logs und Fehlerdaten

### RunSummary

Repraesentiert einen Schedulerlauf.

Kernfelder:

- `id`
- `tenantId`
- `projectId`
- `agentId`
- `schedulerId`
- `schedulerKey`
- `configVersionId`
- `status`: `running`, `success`, `warning`, `failed`, `cancelled`
- `startedAt`
- `finishedAt`
- `durationMs`
- `readCount`
- `writtenCount`
- `skippedCount`
- `failedCount`
- `errorCategory`
- `errorMessage`

Anzeige-Regel:

- Im Monitor werden standardmaessig nur Runs mit Datensatzwirkung, Warnung oder Fehler angezeigt.
- Erfolg-ohne-Wirkung kann aggregiert fuer Health/Graphen erhalten bleiben, muss aber nicht als einzelne Logzeile erscheinen.

### LogEvent

Relevantes Ereignis zu Agent, Projekt oder Run.

Kernfelder:

- `id`
- `tenantId`
- `projectId`
- `agentId`
- `runId`
- `occurredAt`
- `level`: `info`, `warn`, `error`
- `code`
- `message`
- `contextJson`

Regeln:

- Keine reine Heartbeat-Spam-Protokollierung als sichtbarer Monitor-Log.
- Technische Fehler und Datensatzwirkung werden zentral sichtbar.

### FailedRecord

Diagnosedatensatz fuer fehlgeschlagene Datensaetze.

Kernfelder:

- `id`
- `tenantId`
- `projectId`
- `runId`
- `recordKey`
- `status`
- `errorCode`
- `message`
- `sourceJson`
- `mappedJson`
- `createdAt`

Regeln:

- JSON wird formatiert in der UI dargestellt.
- Payload kann nach Projektpolicy maskiert werden.
- Aufbewahrung folgt Lizenz-/Tenant-Policy.

## Audit

### AuditEvent

Unveraenderliche Historie fuer relevante Plattformereignisse.

Kernfelder:

- `id`
- `tenantId`
- `actorType`: `user`, `agent`, `system`
- `actorId`
- `action`
- `resourceType`
- `resourceId`
- `occurredAt`
- `detailsJson`

Auditpflichtige Aktionen:

- Login und fehlgeschlagene Loginversuche.
- Agent-Registrierung, Rotation, Widerruf.
- Lizenz- und Rollenveraenderungen.
- ConfigVersion-Erstellung, Freigabe, Aktivierung und Rollback.
- Projektmodus-Wechsel.
- Manuelle Scheduler-Ausfuehrung ueber SaaS.

## Index- und Mandantenregeln

- Jede fachliche Tabelle enthaelt `tenantId`.
- Hauefige Filter benoetigen zusammengesetzte Indizes, z.B. `(tenantId, projectId, startedAt)`.
- IDs duerfen nicht als alleinige Berechtigungspruefung verwendet werden.
- API prueft Tenant, Projekt und Rolle vor jeder Ressource.
- Soft Delete fuer Tenant, Project, Agent und ConfigVersion bevorzugen.

## Initiale Tabellenliste

- `tenants`
- `users`
- `tenant_memberships`
- `licenses`
- `projects`
- `instances`
- `agents`
- `agent_credentials`
- `connectors`
- `config_versions`
- `schedulers`
- `mappings`
- `run_summaries`
- `log_events`
- `failed_records`
- `audit_events`
