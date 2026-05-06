# Spec-Driven Development einfuehren

Diese Anleitung fuehrt das Projekt auf einen einfachen, praxistauglichen Spec-Driven-Development-Ablauf um, ohne bestehende TypeScript- oder Betriebsprozesse zu stoeren.

## Zielbild

Vor jeder groesseren Aenderung existiert zuerst eine beschriebene Entscheidung in `docs/specs/`. Erst danach folgen Implementierung, Verifikation und Abschluss.

## Was eingerichtet wurde

- Ablage fuer Specs unter `docs/specs/`
- Standardvorlage unter `docs/specs/_template/feature-spec.md`
- Generator fuer neue Specs ueber `npm run spec:new -- "Titel"`
- Validator fuer Mindeststruktur ueber `npm run spec:validate`

## Schritt-fuer-Schritt

### 1. Teamregel festlegen

Definiert im Team, dass jede nicht-triviale Aenderung zuerst eine Spec braucht. Fuer dieses Repo betrifft das vor allem Adapter, Scheduler, Web UI, Metadaten, Deploy- und Migrationslogik.

### 2. Neue Spec erzeugen

```bash
npm run spec:new -- "OAuth Diagnose im Web UI vereinheitlichen"
```

Ergebnis: Eine neue Datei unter `docs/specs/<datum>-oauth-diagnose-im-web-ui-vereinheitlichen/spec.md`.

### 3. Spec inhaltlich fuellen

Fuellt mindestens diese Bereiche sauber aus:

- Kontext
- Problem
- Zielbild
- Nicht-Ziele
- Akzeptanzkriterien
- Umsetzungsskizze
- Aufgaben
- Verifikation
- Status

Faustregel: Wenn ein Dritter nach dem Lesen noch fragen muss, was gebaut wird oder woran Erfolg gemessen wird, ist die Spec noch nicht bereit.

### 4. Akzeptanzkriterien konkret machen

Gute Kriterien sind beobachtbar. Beispiel:

- Statt: "Fehlerbehandlung verbessern"
- Besser: "Bei OAuth-Fehlern zeigt die Web UI den Fehlertyp, den naechsten Schritt und die betroffene Salesforce-Instanz an."

### 5. Technische Leitplanken festhalten

In der Umsetzungsskizze gehoeren fuer dieses Projekt typischerweise hinein:

- betroffene Module unter `src/`
- benoetigte Skripte unter `scripts/`
- Anpassungen an Salesforce-Metadaten unter `salesforce/metadata/`
- Risiken fuer Scheduler, Retry, Logging, Migration oder Windows-Deployment

### 6. Erst dann implementieren

Die Spec ist die Arbeitsgrundlage. Implementierungsschritte werden in `## Aufgaben` zerlegt und waehrend der Umsetzung abgehakt.

### 7. Spec validieren

```bash
npm run spec:validate
```

Der Validator prueft, ob jede Spec die Pflichtsektionen und eine gueltige Status-Markierung enthaelt.

### 8. Verifikation dokumentieren

Tragt in `## Verifikation` ein, was wirklich ausgefuehrt wurde, zum Beispiel:

- `npm run build`
- gezielter Smoke-Test im Agentenlauf
- manueller Check in der Web UI
- Metadata-Deploy-Test in Sandbox

### 9. Abschluss sauber markieren

Wenn die Aenderung fertig ist:

- Aufgaben abhaken
- Status auf `done` setzen
- Letzte Entscheidung und Restpunkte dokumentieren

## Empfohlener Arbeitsmodus fuer dieses Repo

### Kleine Aenderungen

Ein kurzer Spec-Eintrag reicht, wenn die Aenderung lokal, risikoarm und klar verifizierbar ist.

### Mittlere und grosse Aenderungen

Die Spec sollte vor dem ersten Code-Commit reviewed werden, besonders bei Auswirkungen auf:

- Scheduler und Run-Status
- Salesforce-Authentifizierung
- Datenmapping und Migrationen
- Deployment-, Packaging- oder Windows-Service-Logik

## Minimaler Review-Check

Vor Implementierungsstart sollte mindestens beantwortet sein:

1. Welches Problem wird geloest?
2. Woran erkennt man den Erfolg?
3. Was ist explizit nicht im Scope?
4. Welche Module, Datenfluesse und Betriebsfolgen sind betroffen?
5. Wie wird die Aenderung verifiziert?

## Beispielablauf

1. Spec anlegen
2. Fachliche Luecken schliessen
3. Akzeptanzkriterien fixieren
4. Aufgaben schneiden
5. Implementieren
6. Verifizieren
7. Status auf `done`

Damit ist Spec-Driven Development im Projekt nicht nur dokumentiert, sondern direkt ueber npm-Skripte nutzbar.