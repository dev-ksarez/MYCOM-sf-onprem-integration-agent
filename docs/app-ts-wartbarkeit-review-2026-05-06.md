# Wartbarkeits-Review `app.ts`

Stand: 2026-05-11

## Ausgangslage

- `src/server/app.ts` war der zentrale Wartbarkeits-Hotspot mit `13.492` Zeilen.
- Die kuenftige Architektur wird nicht nur nach Modulen, sondern nach drei Diensten gedacht:
  - Agent-Dienst
  - WebServer-Dienst
  - AutoUpdater-Dienst
- Die Datei mischt aktuell mehrere Verantwortlichkeiten:
  - HTTP-Server und Routing
  - Auth-/Session-Handling
  - Installer-Generierung
  - Health-Metriken
  - HTML-Rendering
  - sehr umfangreiches Inline-Frontend-JavaScript

## Bereits umgesetzt auf dem Branch

- Extraktion von Health-Logik nach `src/server/health-snapshot.ts`
- Extraktion von Auth-/Session-/CSRF-/OAuth-Helfern nach `src/server/admin-auth.ts`
- Extraktion der Installer-Erzeugung nach `src/server/installer-generator.ts`
- Anpassung von `src/main.ts` auf das neue `HealthSnapshot`-Modul
- Extraktion statischer Asset-Auslieferung nach `src/server/asset-server.ts`
- Extraktion Audit-Historie nach `src/server/audit-history-service.ts`
- Extraktion HTML-Dokumentrahmen nach `src/server/ui-template.ts`
- Extraktion Admin-UI-JavaScript nach `src/server/admin-ui-script.ts`
- Einfuehrung einer App-Modulregistrierung in `src/server/app-modules.ts`
- Auslagerung Login- und Callback-JavaScript nach `src/public/`
- Auslagerung Login-CSS nach `src/css/login.css`

Ergebnis:

- `src/server/app.ts` wurde von `13.492` auf unter `3.000` Zeilen reduziert.
- Nicht-HTTP-Fachlogik, UI-Assets und wiederverwendbare Template-Bausteine sind klarer gekapselt und separat wartbar.

## Hauptbefunde

1. `app.ts` ist weiterhin der zentrale HTTP-Einstieg.
   Die Datei enthaelt noch Routing und Request-Orchestrierung, aber keine grosse Admin-UI-JavaScript-Implementierung mehr.

2. Das HTML wird als sehr großer String gebaut.
   Das erschwert Navigation, Diff-Reviews und die Wiederverwendung einzelner UI-Bausteine.

3. Das Routing ist linear und stark manuell.
   Viele `if (req.method === ... && requestUrl.pathname === ...)`-Blöcke machen Seiteneffekte und Regressionen wahrscheinlicher.

4. Frontend-Verhalten liegt noch in einem grossen Admin-UI-Script.
   Es ist aus der Serverdatei geloest, sollte aber in weiteren Schritten nach UI-Domaenen geteilt werden.

## Empfohlene nächste Schritte

### Priorität 1

- weitere Shell-Teilbereiche aus `htmlShell()` in dedizierte Templates auslagern
- Konfigurationsseiten als dedizierte Templates buendeln
- Wiederkehrende HTML-Segmente als kleine Template-Funktionen kapseln:
  - Header
  - Modal-Shells
  - Tab-Sektionen
  - Asset-Includes

Nutzen:

- deutlich kleinere `app.ts`
- besser lesbare Diffs
- gezieltere UI-Änderungen ohne Risiko in Routing/Serverlogik

### Priorität 2

- API-Routen nach Domänen aufteilen:
  - `system-routes`
  - `installer-routes`
  - `connector-routes`
  - `schedule-routes`
  - `migration-routes`
  - `salesforce-routes`
- In `app.ts` nur noch Request-Kontext aufbauen und Route-Handler registrieren

Nutzen:

- klarere Verantwortlichkeiten
- geringere Merge-Konflikte
- leichteres Testen einzelner Handler

### Priorität 3

- Inline-JavaScript aus `htmlShell()` weiter in spezialisierte UI-Module zerlegen
- bestehendes Muster beibehalten und ausbauen:
  - `connector-ui-module.ts`
  - `scheduler-ui-module.ts`
  - `migration-ui-module.ts`
- zusätzlich sinnvoll:
  - `overview-ui-module.ts`
  - `template-picker-ui-module.ts`
  - `installer-ui-module.ts`

Nutzen:

- bessere Trennung von UI-Domänen
- geringere mentale Last beim Arbeiten an einzelnen Features

## Template-Empfehlung

Für die nächste Stufe sollte mit Templates gearbeitet werden, aber pragmatisch:

- kein sofortiger Wechsel auf ein externes Templating-Framework nötig
- zunächst TypeScript-basierte Render-Templates mit kleinen Funktionen verwenden
- erst bei weiter wachsender UI prüfen, ob ein separates Frontend-Bundle sinnvoll ist

Empfohlene Struktur:

- `src/server/templates/page-template.ts`
- `src/server/templates/login-template.ts`
- `src/server/templates/app-shell-template.ts`
- `src/server/templates/partials/*.ts`

## Zielbild

`app.ts` sollte mittelfristig nur noch folgende Aufgaben haben:

- Server erzeugen
- gemeinsame Request-Helfer bereitstellen
- Auth-Gates anwenden
- Route-Module delegieren
- finale HTML-Shell aus einem Template-Modul rendern

Alles andere sollte außerhalb der Datei liegen.

In der Zielarchitektur gehoert `app.ts` bzw. ihr Nachfolger fachlich in den Bereich `web-dashboard` und darf keine gemischte Agent-/Updater-Verantwortung mehr tragen.
