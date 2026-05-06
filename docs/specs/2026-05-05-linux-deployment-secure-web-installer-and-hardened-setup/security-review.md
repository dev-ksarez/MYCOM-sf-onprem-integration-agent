# Initial Security Review

- Datum: 2026-05-05
- Scope: Web UI, Admin-API, Installer-/Update-Pfade, Secrets, Dependencies, Betriebsdefaults
- Status: initial review

## Priorisierte Findings

### 1. Fehlende Authentifizierung auf Admin-API

- Severity: critical
- Fundstelle: [src/server/app.ts](src/server/app.ts#L9097)
- Befund: Die HTTP-Serverlogik exponiert Admin- und Mutationsendpunkte ohne erkennbare Authentifizierungs- oder Autorisierungspruefung.
- Risiko: Jeder Netzwerkzugriff auf die Web UI kann Scheduler, Connectoren, Instanzen und Update-Aktionen ausloesen oder veraendern.
- Massnahme: Token- oder Session-basierte Authentifizierung fuer alle Admin-Endpunkte einfuehren und standardmaessig erzwingen.

### 2. Klartext-Secrets im Setup-Pfad

- Severity: critical
- Fundstellen: [scripts/windows/install-windows-agent.ps1](scripts/windows/install-windows-agent.ps1#L424), [scripts/windows/install-windows-agent.ps1](scripts/windows/install-windows-agent.ps1#L441)
- Befund: Installer-Eingaben fuer Secrets werden als Klartext in .env-Dateien geschrieben.
- Risiko: Geheimnisse liegen lokal leicht auslesbar vor und werden bei schwachen Dateirechten oder Backups mitkopiert.
- Massnahme: Secret-Referenzen, OS-Credential-Stores oder verschluesselte Secret-Dateien verwenden; .env nur fuer nicht-sensitive Konfiguration.

### 3. Unsichere Datenbank-TLS-Defaults

- Severity: critical
- Fundstelle: [src/infrastructure/db/mssql.ts](src/infrastructure/db/mssql.ts#L38)
- Befund: MSSQL-Verbindungen verwenden unsichere Defaults fuer TLS und Zertifikatsvertrauen.
- Risiko: Verbindungen koennen auf unsicheren Pfaden mitgelesen oder ueber unsichere Zertifikate kompromittiert werden.
- Massnahme: encrypt=true und trustServerCertificate=false als sichere Defaults setzen.

### 4. Keine HTTPS-Absicherung fuer die Web UI

- Severity: high
- Fundstellen: [src/server/app.ts](src/server/app.ts#L9097), [src/main.ts](src/main.ts)
- Befund: Die Web UI basiert auf einem nackten HTTP-Server ohne integrierte TLS- oder Proxy-Erzwingung.
- Risiko: Zugangsdaten, Session-Tokens und Admin-Aktionen koennen ueber oeffentliche Netze abgegriffen werden.
- Massnahme: Linux-Zielbild mit Reverse-Proxy und TLS als Pflichtpfad definieren; HTTP nur intern oder lokal binden.

### 5. Keine CSRF-/Origin-Absicherung fuer Schreiboperationen

- Severity: high
- Fundstelle: [src/server/app.ts](src/server/app.ts#L9097)
- Befund: POST- und DELETE-Endpunkte pruefen weder CSRF-Token noch Origin/Referer.
- Risiko: Nach Einfuehrung einer Session-basierten Anmeldung waeren Mutationen direkt CSRF-gefaehrdet.
- Massnahme: Bei Auth-Einfuehrung Origin-Checks und CSRF-Schutz verbindlich mitliefern.

### 6. Inline-Credentials in Connector-Konfigurationen

- Severity: high
- Fundstelle: [src/connectors/mssql/mssql-connector.ts](src/connectors/mssql/mssql-connector.ts#L75)
- Befund: Connectoren akzeptieren Passwoerter direkt in der Konfiguration und nicht nur ueber Secret Keys.
- Risiko: Secrets koennen in exportierten Setups, Logs oder Admin-Datenstrukturen auftauchen.
- Massnahme: Inline-Passwoerter abkuendigen und Secret-Referenzen als Standard erzwingen.

### 7. Dependency-Risiken laut npm audit

- Severity: mixed
- Fundstelle: [package.json](package.json)
- Befund: npm audit meldet unter anderem Befunde im Umfeld von sqlite3, lodash und mssql-Transitiven.
- Risiko: Bekannte Schwachstellen bleiben in der Lieferkette bestehen und muessen gegen Kompatibilitaetsrisiken abgewogen werden.
- Massnahme: Abhaengigkeiten priorisiert aktualisieren, beginnend mit den High-Severity-Ketten und einem schmalen Regressionstest pro Upgrade.

## Sofortprioritaeten

- Authentifizierung und Autorisierung fuer Web UI und Admin-API definieren
- Linux-Deploy-Pfad mit TLS-Proxy und gehaerteten Prozessrechten festlegen
- Secret-Speicher fuer Installer und Connectoren haerten
- Dependency-Upgrades und sichere MSSQL-Defaults als eigenes Paket nachziehen