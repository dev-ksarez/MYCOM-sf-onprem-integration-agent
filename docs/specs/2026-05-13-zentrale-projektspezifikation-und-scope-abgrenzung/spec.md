# Zentrale Projektspezifikation und Scope-Abgrenzung

- Spec-ID: 2026-05-13-zentrale-projektspezifikation-und-scope-abgrenzung
- Status: draft
- Owner: Projektteam SF OnPrem Integration Agent
- Reviewers: Produktverantwortung, Betrieb, Delivery
- Verknuepfte Tickets: 

## Kontext

Im Repository existieren sowohl projektspezifische Spezifikationen unter `docs/specs/` als auch kundenbezogene Inhalte in `artifacts/`.
Der Inhalt in `artifacts/spec_AnnaburgerProd` beschreibt eine konkrete Kundeninstallation (Annaburger PROD1) und ist kein allgemeiner Projektscope.

Ohne zentrale Scope-Spezifikation besteht das Risiko, dass kundenindividuelle Anforderungen als Produktstandard verstanden werden.
Das fuehrt zu unklaren Prioritaeten, uneinheitlicher Umsetzung und erschwerter Release-Kommunikation.

## Problem

Es fehlt eine zentrale, aktuelle Projektspezifikation mit klarer Abgrenzung zwischen:

- Produktkern (wiederverwendbare, releasefaehige Funktionen)
- kundenspezifischen Implementierungen (z. B. Annaburger)
- Betriebs-/Migrationsartefakten

Dadurch ist unklar, welche Anforderungen verpflichtend fuer das Gesamtprodukt sind und welche nur fuer einzelne Kunden gelten.

## Zielbild

1. Das Projekt besitzt eine zentrale Spec, die den Produktscope und die Architekturleitplanken verbindlich beschreibt.
2. Kundenspezifische Inhalte werden explizit als solche markiert und nicht als Produktstandard gefuehrt.
3. Die Spec beschreibt die Kernbereiche des Produkts: Agent, Web UI, Integrationsadapter, Scheduler, Deployment/Updater.
4. Release- und Umsetzungsentscheidungen beziehen sich primaer auf die zentrale Projektspezifikation.
5. Neue Features starten weiterhin mit eigenen Feature-Specs, werden aber gegen die zentrale Scope-Definition geprueft.
6. Das Team kann klar kommunizieren, welche Funktionen produktweit und welche kundenbezogen sind.

## Nicht-Ziele

- Keine Umsetzung konkreter Kundenanforderungen aus Annaburger in diesem Schritt.
- Keine Loeschung historischer Artefakte oder Migrationsdaten.
- Keine technische Refaktorierung an Schedulern, Adaptern oder Salesforce-Metadaten nur aufgrund dieser Spec.

## Akzeptanzkriterien

- [ ] Verhalten ist fuer den Nutzer oder Operator eindeutig beobachtbar.
- [ ] Erfolgs- und Fehlerfall sind beschrieben.
- [ ] Konfiguration, Migration oder Deployment-Folgen sind dokumentiert.
- [ ] Der zentrale Projektscope ist von kundenindividuellen Inhalten klar getrennt dokumentiert.
- [ ] `artifacts/spec_AnnaburgerProd` ist als Kundeninstallation eingeordnet und nicht als Projekt-Referenzspec behandelt.
- [ ] Der Umgang mit kuenftigen Kunden-Specs ist als Regel definiert.

## Umsetzungsskizze

Betroffene Bereiche im Repo:

- `docs/specs/`
- `SPEC_DRIVEN_DEVELOPMENT.md`
- `artifacts/`

Technische Leitplanken:

- Zentrale Scope-Definition liegt in `docs/specs/` und ist releasebezogene Referenz.
- Kundeninstallationen bleiben in `artifacts/` oder dedizierten Kundenordnern und werden als kundenindividuell gekennzeichnet.
- Feature-Specs in `docs/specs/<datum>-<slug>/spec.md` duerfen den zentralen Scope erweitern, aber nicht stillschweigend durch Kundenartefakte ersetzen.
- Bei Konflikten gilt: zentrale Spec + Produktentscheidungen vor kundenlokalen Notizen.

Regel fuer neue Kunden-Spezifikationen:

1. Ablage unter kundenbezogenem Pfad (z. B. `artifacts/customer-installations/<kunde>/`).
2. Deutliche Kennzeichnung als kundenindividuell.
3. Keine implizite Uebernahme in Produkt-Backlog ohne separate Produkt-Spec.

## Aufgaben

- [x] Zentrale Scope-Spec initial erstellt.
- [x] Abgrenzung Produkt vs. Kundeninstallation dokumentiert.
- [ ] Team-Review und Freigabe der Scope-Regeln.
- [ ] Optional: Kunden-Specs strukturell in einen dedizierten Unterordner ueberfuehren.

## Verifikation

- Build oder schmaler Smoke-Test: `npm run spec:validate` erfolgreich.
- Manuelle Checks in Web UI oder Agent: nicht erforderlich (Dokumentationsaenderung).
- Betriebsrelevante Beobachtung nach Deploy: Team nutzt zentrale Spec als Referenz in neuen Planungs- und Review-Runden.

## Status

- Status: draft
- Letzte Entscheidung: Annaburger-Content wird als kundenbezogene Installation betrachtet, nicht als zentraler Produktscope.
- Naechster Schritt: Review der Scope-Regeln mit Produkt/Betrieb und anschliessende Status-Umstellung auf `done`.