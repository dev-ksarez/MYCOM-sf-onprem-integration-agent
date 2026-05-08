# Release 0.2.22

## Bugfixes

### Mappingliste auf Remote-Agenten leer (Fix)

**Symptom**: Wenn ein Schedule auf einem Remote-Agenten geöffnet wurde, war die Mapping-Liste im Mapping-Manager leer – obwohl das Mapping korrekt in Salesforce gespeichert war. Lokal (wo der Connector erreichbar ist) wurde die Liste korrekt angezeigt.

**Ursache**: `getSchedulerMappingManagerSources()` ermittelte die darzustellenden Quellfelder ausschließlich aus den live vom Connector geladenen Feldmetadaten (`state.mappingFields`). Wenn der Connector auf dem Remote-Agenten nicht erreichbar ist (z.B. lokale Datenbankverbindung fehlt), blieb dieses Array leer – die Mapping-Regeln selbst waren jedoch vollständig in Salesforce (`MSD_Schedule__c.MSD_MappingDefinition__c`) gespeichert.

**Fix**: Wenn keine Feldmetadaten vom Connector geladen werden konnten, werden die Quellfelder jetzt direkt aus den in Salesforce persistierten Mapping-Regeln (`state.mappingRules`) synthetisiert. Der „Felder laden"-Button bleibt jederzeit verfügbar, um frische Connector-Metadaten zu laden.

- **Lokal (Connector erreichbar)**: Verhalten unverändert – alle Quellfelder inkl. ungemappter werden angezeigt ✓
- **Remote-Agent (Connector nicht erreichbar)**: Alle gemappten Felder aus Salesforce werden jetzt korrekt angezeigt ✓
- **Persistenz**: Mapping-Daten werden ausschließlich in Salesforce gespeichert – keine Änderung am Datenmodell ✓

## Migration Path

Kein Deployment-Schritt erforderlich – reine UI-Änderung in `src/server/app.ts`.
