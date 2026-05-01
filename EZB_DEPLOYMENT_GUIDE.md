# EZB__c CustomObject Deployment Guide

Da das Objekt noch nicht in Salesforce vorhanden ist, können Sie es auf folgende Weise erstellen:

## 🟢 Option 1: Über SFDX CLI (Empfohlen & Schnellste Methode)

```bash
cd salesforce/metadata

# Deployment initiieren
sfdx force:source:deploy -p . -u <org-alias> -w 10

# Oder mit Metadaten API
sfdx force:mdapi:deploy -d . -u <org-alias> -w 10
```

## 🟡 Option 2: Über Salesforce UI (Manuell)

1. **Öffne Setup** → Search "CustomObject" oder "Custom Objects"
2. **Click "Create"**
3. **Fülle folgende Felder aus:**
   - Object Name: `EZB` (System wird automatisch `__c` hinzufügen → `EZB__c`)
   - Label: `EZB Exchange Rate`
   - Plural Label: `EZB Exchange Rates`
   - Record Name: `EZB Rate ID` (or similar)
   - Data Type: Auto Number
   - Display Format: `EZB-{0000}`
   - Sharing Model: `Read/Write`

4. **Speichern** → Next

5. **Felder hinzufügen** (nacheinander hinzufügen):

   **Feld 1:**
   - Field Label: `Currency Pair`
   - Field Name: `CurrencyPair`  
   - Data Type: `Text`
   - Length: `20`
   - ✓ Unique
   - ✓ External ID
   - Save

   **Feld 2:**
   - Field Label: `Rate`
   - Field Name: `Rate`
   - Data Type: `Number`
   - Precision: `18`
   - Scale: `6`
   - Save

   **Feld 3:**
   - Field Label: `Effective Date`
   - Field Name: `EffectiveDate`
   - Data Type: `Date`
   - Save

   **Feld 4:**
   - Field Label: `Last Updated`
   - Field Name: `LastUpdated`
   - Data Type: `DateTime`
   - Save

   **Feld 5:**
   - Field Label: `Source`
   - Field Name: `Source`
   - Data Type: `Text`
   - Length: `100`
   - Save

## 🔵 Option 3: Über Metadata XML (Deployment Package)

```bash
# ZIP der Metadaten erstellen und hochladen
cd salesforce
zip -r deploy.zip metadata/

# Im Setup: Deploy → Deploy from Metadata API → Upload ZIP
# Datei: deploy.zip
# Deployment Options:
# ✓ Single File Upload
# ✓ Deploy
```

---

## ✅ Validierung nach Deployment

Nach erfolgreichem Deployment:

1. **Objekt prüfen**: Setup → Custom Objects → suche "EZB Exchange Rate"
2. **Felder prüfen**: 5 Custom Fields sollten vorhanden sein
3. **Tab erstellen** (optional): Setup → Create → Tabs → New für EZB__c
4. **Profil aktualisieren**: Setup → Profiles → System Administrator → Object Settings → EZB__c → ✓ Lesen, ✓ Bearbeiten, ✓ Anlegen

---

## 🚀 Integration Agent konfigurieren (Nach Deployment)

Einmal das Objekt vorhanden ist, nutze die Integration Agent UI:

1. **Scheduler Tab** → "Neuer Scheduler"
2. **Name**: `ECB to Salesforce Daily`
3. **Source Type**: `REST_API`
4. **Source Connector**: `ECB Exchange Rates`
5. **Source Definition**:
```json
{
  "endpoint": "/service/data/EXR/D.USD.EUR.SP00.A",
  "method": "GET",
  "query": {
    "lastNObservations": 1,
    "format": "jsondata"
  },
  "resultPath": "dataSets.0.series.0:0:0:0:0.observations"
}
```
6. **Target Type**: `SALESFORCE`
7. **Target Object**: `EZB__c`
8. **Field Mappings**:
```json
[
  {
    "source": "0",
    "target": "Rate__c",
    "action": "map"
  }
]
```
9. **Speichern** & Run Now zum Testen

---

## 🆘 Troubleshooting

**Problem**: "EZB__c not found" in scheduler
- **Lösung**: Stelle sicher, dass das Objekt via UI oder SFDX deployed wurde. Im Monitoring prüfen, ob Salesforce Zugriff OK ist.

**Problem**: Scheduler läuft, aber keine Daten werden synchronisiert
- **Lösung**: 
  1. Prüfe REST-Connector ECB in Connectoren-Tab
  2. Klicke "Test" → sollte Daten zurückgeben
  3. Prüfe Field Mappings (Rate__c) im Scheduler

**Problem**: Deployment schlägt fehl
- **Lösung**: 
  1. Stelle sicher, dass SF_USERNAME, SF_PASSWORD gesetzt sind
  2. Nutze Salesforce UI als Fallback (Option 2)
  3. Prüfe Log-Output auf spezifische Fehler

---

**Status**: CustomObject-Metadaten vorhanden in `salesforce/metadata/objects/EZB__c.object`  
**Nächste Schritte**: 1. Deploy EZB__c, 2. Scheduler konfigurieren, 3. Testen
