# Linux deployment, secure web installer and hardened setup

- Spec-ID: 2026-05-05-linux-deployment-secure-web-installer-and-hardened-setup
- Status: ready
- Owner: 
- Reviewers: 
- Verknuepfte Tickets: 

## Kontext

Der Agent ist aktuell stark auf lokale oder Windows-nahe Betriebsfaelle ausgerichtet. Die Web UI ist ueber [src/server/app.ts](src/server/app.ts) direkt per ungesichertem HTTP-Server erreichbar, und der aktuelle Installationspfad basiert vor allem auf PowerShell-Skripten unter [scripts/windows](scripts/windows). Fuer einen Betrieb auf einer oeffentlichen Linux-VM fehlen damit ein abgesicherter Zielzustand, ein webbasierter Installationsassistent, eine robuste Authentifizierung fuer die Admin-Oberflaeche und ein gehaerteter Setup-Prozess.

Der erste Security-Review fuer diese Spec zeigt bereits konkrete Risiken:

- Die Admin- und Update-Endpunkte werden direkt aus einem nackten HTTP-Server bedient, ohne erkennbare Zugriffspruefung in [src/server/app.ts](src/server/app.ts#L9097).
- In Connectoren sind neben Secret-Keys auch Inline-Credentials vorgesehen, zum Beispiel fuer MSSQL in [src/connectors/mssql/mssql-connector.ts](src/connectors/mssql/mssql-connector.ts#L75).
- Die REST-Connector-UI erlaubt mehrere Authentifizierungsmodi, aber die Admin-Weboberflaeche selbst hat noch keinen Login- oder Token-Schutz.
- npm audit meldet bekannte Schwachstellen in der aktuellen Abhaengigkeitskette, unter anderem im Umfeld von sqlite3, lodash und mssql-Transitiven.

## Problem

Der Agent kann auf einer oeffentlichen Linux-VM derzeit nicht als gehaertetes Produkt betrieben werden. Es fehlt eine dokumentierte und implementierte Linux-Ziellandschaft mit sicherem Prozessmodell, TLS-Einbindung, Login-Schutz und Setup-Hardening. Die aktuelle Web UI ist fuer Administrationszwecke maechig, aber sicherheitstechnisch noch zu offen. Gleichzeitig sind Installation und Update fuer Windows skriptbasiert, jedoch noch nicht als browsergestuetzter Assistent verfuegbar und fuer Linux praktisch nicht vorhanden.

Zusatzlich sind die Header-Aktionen in der Web UI auf kleinen Geraeten zu breit. Die Bedienung soll in ein Hamburger-Menue ueberfuehrt werden, ohne die Desktop-Bedienung zu verschlechtern.

## Zielbild

- Die Header-Aktionen der Web UI sind auf kleinen Viewports in einem Hamburger-Menue gebuendelt und bleiben auf Desktop direkt sichtbar.
- Der Agent kann auf einer oeffentlichen Linux-VM ueber einen dokumentierten und reproduzierbaren Zielpfad betrieben werden, inklusive systemd-Service, Reverse-Proxy und TLS.
- Die Admin-Weboberflaeche ist ueber Login und tokenbasierte Sitzung abgesichert; administrative API-Aufrufe sind ohne gueltige Authentifizierung nicht mehr zugaenglich.
- Der Erstinstallationsprozess wird als webbasierter Installationsassistent bereitgestellt, der Konfiguration, Secrets, Health-Checks und Haertungsschritte gefuehrt abfragt.
- Der Setup- und Update-Prozess erzwingt sichere Defaults, minimiert Inline-Secrets und dokumentiert den sicheren Betrieb auf Windows und Linux.
- Bekannte Sicherheitsluecken aus Code-Review und Dependency-Scan sind bewertet, priorisiert und in konkrete Remediationsschritte zerlegt.
- Der Agent unterstuetzt Linux als offiziell dokumentierten und testbaren Betriebsmodus fuer oeffentliche VM-Server.

## Nicht-Ziele

- Kein kompletter Ersatz der bestehenden Serverarchitektur durch Kubernetes, Docker-Only-Betrieb oder ein neues Frontend-Framework in derselben Stufe.
- Keine Einfuehrung eines vollwertigen IAM-Systems mit SSO, SCIM oder Mandantenverwaltung in diesem Paket.
- Keine sofortige Migration aller bestehenden Windows-Skripte in einen einzigen plattformneutralen Installer ohne Zwischenstufen.
- Keine vollautomatische Zertifikatsverwaltung fuer jede denkbare Infrastruktur ausser dem zuerst definierten Linux-Zielpfad.
- Keine sofortige Beseitigung aller transativen Dependency-Probleme ohne Risikobewertung und Regressionstests.

## Akzeptanzkriterien

- [ ] Die Header-Aktionen sind auf Mobile hinter einem Hamburger-Menue erreichbar und auf Desktop unveraendert direkt bedienbar.
- [ ] Es existiert ein definierter Linux-Betriebspfad mit systemd-Service, Reverse-Proxy, TLS-Terminierung und eingeschraenkten Laufzeitrechten.
- [ ] Die Web-UI und die Admin-API verlangen Authentifizierung; unautorisierte Requests auf Admin-Funktionen liefern einen klaren Fehlerfall.
- [ ] Der Installationsassistent kann initiale Konfiguration, Secret-Pruefung, HTTPS-/Proxy-Hinweise und Health-Checks webgestuetzt abarbeiten.
- [ ] Setup und Update vermeiden neue Inline-Secrets, dokumentieren sichere Speicherorte und fuehren Preflight- sowie Hardening-Pruefungen aus.
- [ ] Die wichtigsten Sicherheitsbefunde aus dem initialen Review sind dokumentiert, priorisiert und jeweils mit Umsetzungs- oder Rest-Risiko versehen.
- [ ] Die Betriebsdoku fuer Linux, HTTPS, Token, Login und abgesichertes Update ist vorhanden.

## Umsetzungsskizze

Betroffene Bereiche im Repo:

- [src/server/app.ts](src/server/app.ts)
- [src/css/agent-ui.css](src/css/agent-ui.css)
- [scripts/windows](scripts/windows)
- neu: Linux-Betriebsskripte und/oder systemd-Templates unter [scripts](scripts)
- Installations- und Betriebsdoku in [README.md](README.md) und neuen Linux-/Security-Dokumenten

Technische Leitplanken:

- Der Web-Login fuer die Admin-UI soll auf serverseitig validierten Token- oder Session-Cookies basieren; reine Client-Checks reichen nicht aus.
- TLS wird fuer Linux ueber einen klar dokumentierten Reverse-Proxy-Pfad terminiert; der Node-Prozess selbst bleibt intern und nicht direkt oeffentlich exponiert.
- Installations- und Update-Assistenten muessen Preflight-Checks fuer Dateirechte, Secret-Vollstaendigkeit, Port-Konflikte, HTTPS-Konfiguration und Health-Endpunkte enthalten.
- Secrets duerfen nicht mehr als empfohlener Standard inline in Connector-Konfigurationen gespeichert werden; Secret-Referenzen und geschuetzte Speicherorte werden bevorzugt.
- Sicherheitspruefung umfasst mindestens: Dependency-Scan, Review der Admin-Endpunkte, Secret-Handling, Update-Pfad, Logging, Input-Validierung und Betriebsrechte.
- Linux-Unterstuetzung wird mit einem minimalen, gehaerteten Betriebsprofil gestartet: dedizierter User, systemd, Firewall-/Proxy-Hinweise, kein Root-Dauerbetrieb.

## Aufgaben

- [ ] Mobile Header-Aktionen als Hamburger-Menue umsetzen und verifizieren.
- [ ] Sicherheitsreview des aktuellen Projekts mit priorisierten Findings dokumentieren.
- [ ] Zielarchitektur fuer Linux-Betrieb auf oeffentlicher VM spezifizieren.
- [ ] Authentifizierungs- und Token-Modell fuer Admin-UI und Admin-API festlegen.
- [ ] Webbasierten Installationsassistenten in Lieferpakete zerlegen.
- [ ] Setup-Hardening fuer Windows- und Linux-Pfade mit Preflight-Checks und sicheren Defaults definieren.
- [ ] Dependency- und Code-Sicherheitsbefunde priorisieren und in konkrete Remediation-Tickets aufteilen.

## Verifikation

- Build oder schmaler Smoke-Test: `npm run build`, `npm run spec:validate`
- Manuelle Checks in Web UI oder Agent: Mobile Viewport testen, Hamburger-Menue oeffnen, Header-Aktionen ausfuehren
- Security-Checks: `npm audit --omit=dev`, Review der Admin-Endpunkte und Secret-Pfade
- Betriebsrelevante Beobachtung nach Deploy: Linux-Service startet reproduzierbar hinter TLS-Proxy und Admin-UI ist ohne Login nicht erreichbar

## Status

- Status: ready
- Letzte Entscheidung: Der Umfang wird als gemeinsame Plattform- und Sicherheits-Spec gefuehrt, weil Linux-Betrieb, Authentifizierung, Web-Installer und Setup-Hardening denselben Admin- und Betriebsrahmen betreffen.
- Naechster Schritt: Erstes Lieferpaket mit mobilem Hamburger-Menue und dokumentiertem initialen Security-Review abschliessen, danach Auth-/Linux-Zielbild in umsetzbare Pakete schneiden.