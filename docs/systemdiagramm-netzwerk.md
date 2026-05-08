# Systemdiagramm: Netzwerktechnische Anbindung

Dieses Diagramm zeigt die Betriebs- und Netzwerksicht des SF On-Prem Integration Agent: Salesforce bleibt die Steuerungsebene, der Agent fuehrt Scheduler und Connectoren im Kundennetz aus, und die Web UI kann lokal oder ueber die Remote Agent API angebunden werden.

![Systemdiagramm Netzwerk](./systemdiagramm-netzwerk.svg)

## Mermaid-Quelle

```mermaid
flowchart LR
  user[Admin Browser]

  subgraph sf[Salesforce Cloud]
    oauth[Connected App / OAuth<br/>Client Credentials]
    config[Schedules & Connector Config<br/>MSD_Schedule__c<br/>MSD_Connector__c]
    runs[Runs, Logs, Checkpoints<br/>MSD_Run__c / Logs]
    targets[Salesforce Zielobjekte<br/>Objects / Picklists]
  end

  subgraph web[Web-/DMZ-Host]
    webui[Web UI<br/>Admin, Dashboard, Installer<br/>WEB_UI_PORT 8080]
    oidc[Salesforce OIDC Login<br/>optional]
  end

  subgraph agenthost[On-Prem Agent-Host]
    api[Agent API<br/>AGENT_API_PORT 8090<br/>Bearer Token]
    agent[Agent-Dienst<br/>Job Runner & Mapping Engine]
    scheduler[Scheduler<br/>Zeitfenster, Intervalle,<br/>Overlap-Schutz]
    registry[Connector Registry<br/>MSSQL, REST, File,<br/>Salesforce Adapter]
    artifacts[Lokale Artefakte<br/>Health, Audit, Installer,<br/>Secrets]
  end

  subgraph onprem[On-Prem Systeme]
    mssql[(MSSQL / Sage<br/>TCP 1433)]
    files[(Dateien<br/>CSV / XLSX / JSON)]
    rest[REST API<br/>HTTP(S)]
  end

  user -->|HTTPS| webui
  webui -.->|OIDC optional<br/>HTTPS 443| oidc
  oidc -.->|Login / Token| oauth

  webui -.->|Remote Health / Update / Instanzen<br/>Bearer Token| api
  api --> agent

  agent -->|OAuth Token<br/>HTTPS 443| oauth
  agent -->|Schedules & Connectoren lesen<br/>HTTPS 443| config
  scheduler -->|due Run ausloesen| agent
  agent -->|Run/Log/Checkpoint schreiben<br/>HTTPS 443| runs
  agent -->|Salesforce Target Adapter<br/>HTTPS 443| targets

  agent --> scheduler
  agent --> registry
  registry -->|SQL| mssql
  registry -->|Datei I/O| files
  registry -->|HTTP(S)| rest
  agent --> artifacts

  classDef salesforce fill:#e8f3ff,stroke:#2563eb,color:#0f172a,stroke-width:1.5px;
  classDef web fill:#f8fafc,stroke:#64748b,color:#0f172a,stroke-width:1.5px;
  classDef agent fill:#eefbf3,stroke:#16a34a,color:#0f172a,stroke-width:1.5px;
  classDef data fill:#fff7ed,stroke:#d97706,color:#0f172a,stroke-width:1.5px;
  classDef external fill:#fef2f2,stroke:#b91c1c,color:#0f172a,stroke-width:1.5px;

  class oauth,config,runs,targets salesforce;
  class webui,oidc web;
  class api,agent,scheduler,registry,artifacts agent;
  class mssql,files,rest data;
  class user external;
```

## Netzwerkpunkte

| Verbindung | Richtung | Protokoll / Port | Zweck |
| --- | --- | --- | --- |
| Agent -> Salesforce | ausgehend | HTTPS 443 | OAuth, Schedule-/Connector-Config lesen, Runs/Logs/Checkpoints schreiben, Salesforce-Ziele aktualisieren |
| Web UI -> Agent API | intern | HTTP 8090 oder HTTPS via Reverse Proxy | Health, Update-Status, Update-Anforderung, Instanz-Synchronisierung |
| Browser -> Web UI | eingehend | HTTPS extern, intern `WEB_UI_PORT` 8080 | Admin UI, Dashboard, Installer- und Betriebsansichten |
| Web UI -> Salesforce OIDC | ausgehend | HTTPS 443 | optionaler Admin-Login ueber Salesforce als Identity Provider |
| Agent -> MSSQL | intern | TCP 1433 | Lesen oder Schreiben ueber MSSQL-Connector |
| Agent -> Dateiablage | intern | Filesystem, Share oder SFTP | CSV/XLSX/JSON Import und Export |
| Agent -> REST-System | intern oder ausgehend | HTTP(S) | REST-Quellen oder Zielsysteme |

## Betriebsannahmen

- Salesforce wird nicht direkt aus dem Kundennetz angerufen; der Agent nutzt ausgehende HTTPS-Verbindungen.
- Die Agent API ist nur fuer interne Betriebsfunktionen gedacht und benoetigt `AGENT_API_TOKEN`.
- Bei getrennten Hosts nutzt die Web UI `AGENT_REMOTE_BASE_URL` und `AGENT_REMOTE_TOKEN`.
- Externe TLS-Terminierung sollte am Reverse Proxy erfolgen; intern kann der Dienstport getrennt abgesichert werden.
