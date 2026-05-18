# Release 0.2.57

## Salesforce Health Pulse und Remote Commands

- Der Agent schreibt seinen Health Pulse jetzt periodisch in die Produktions-Salesforce-Org.
- Zielobjekt ist `MSD_AgentHealth__c`; Legacy-Objekte `MSD_AgentPulse__c` und `MSD_Heartbeat__c` bleiben kompatibel.
- Der Agent pollt Remote Commands aus `MSD_AgentCommand__c`; Legacy-Objekte `MSD_RemoteCommand__c` und `MSD_AgentInstruction__c` bleiben kompatibel.
- Unterstuetzte Command-Typen:
  - `request-update`
  - `upload-error-log`
  - `restart-agent`
- Command-Ergebnisse werden nach Salesforce zurueckgeschrieben:
  - `Accepted`
  - `Done`
  - `Failed`
  - `Ignored`
- Commands koennen ueber das JSON-Payload gezielt auf `agentId`, `projectId` und `instanceId` eingeschraenkt werden.
- Wenn `AGENT_COMMAND_SHARED_SECRET` gesetzt ist, werden Salesforce-Commands nur mit gueltiger HMAC-Signatur akzeptiert.
- `restart-agent` wird nur ausgefuehrt, wenn `AGENT_REMOTE_COMMAND_RESTART_ENABLED=1` gesetzt ist.

## Konfiguration

- `AGENT_SALESFORCE_CONTROL_PLANE_ENABLED=0` deaktiviert die Salesforce-Control-Plane.
- `AGENT_HEALTH_PULSE_INTERVAL_MS` steuert den Health-Pulse-Intervall, Default: `300000`.
- `AGENT_COMMAND_POLL_INTERVAL_MS` steuert den Command-Poll-Intervall, Default: `60000`.
- `AGENT_CONTROL_INSTANCE_ID` waehlt explizit die Salesforce-Instanz fuer Health/Commands.
- Ohne explizite Instanz wird die Produktionsinstanz verwendet; falls keine Produktion markiert ist, die erste konfigurierte Instanz.

## Hinweise

- Vor Nutzung muessen die Salesforce-Agent-Metadaten in der Produktions-Org vorhanden sein.
- Setup/Readiness in der Web-UI prueft weiterhin `MSD_AgentHealth__c`, `MSD_AgentCommand__c` und den Permission Set `MSD_Integration_Agent`.
