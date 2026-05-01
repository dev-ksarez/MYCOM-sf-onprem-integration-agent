# ECB → Salesforce EZB__c Scheduler Template

## ✅ Completion Status

- **CustomObject Created**: EZB__c (salesforce/metadata/objects/EZB__c.object)
  - Fields: CurrencyPair__c (external ID, unique), Rate__c (decimal), EffectiveDate__c (date), LastUpdated__c (datetime), Source__c (text)
  - Ready for deployment

- **REST Connector Created**: ECB Exchange Rates
  - Type: REST_API
  - Base URL: https://data-api.ecb.europa.eu
  - Status: Active ✅

## 📋 Scheduler Configuration

### Quick Copy-Paste JSON (for API POST)

```json
{
  "name": "ECB to Salesforce Daily",
  "sourceConnectorName": "ECB Exchange Rates",
  "sourceType": "REST_API",
  "sourceQuery": "{\"endpoint\":\"/service/data/EXR/D.USD.EUR.SP00.A\",\"method\":\"GET\",\"query\":{\"lastNObservations\":1,\"format\":\"jsondata\"},\"resultPath\":\"dataSets.0.series.0:0:0:0:0.observations\"}",
  "sourceDefinition": "{\"endpoint\":\"/service/data/EXR/D.USD.EUR.SP00.A\",\"method\":\"GET\",\"query\":{\"lastNObservations\":1,\"format\":\"jsondata\"},\"resultPath\":\"dataSets.0.series.0:0:0:0:0.observations\"}",
  "targetType": "SALESFORCE",
  "targetSystem": "Salesforce",
  "objectName": "EZB__c",
  "operation": "upsert",
  "direction": "Inbound",
  "active": true,
  "batchSize": 10,
  "timingDefinition": "{\"intervalMinutes\":1440,\"nextRunAt\":\"2026-05-01T09:00:00Z\"}",
  "mappingDefinition": "[{\"source\":\"0\",\"target\":\"Rate__c\",\"action\":\"map\"}]"
}
```

### Manual Salesforce Setup (if needed)

1. **In Salesforce Setup**, create a Schedule record (MSD_Schedule__c):
   - **Name**: ECB to Salesforce Daily
   - **Source System**: Salesforce (generic REST)
   - **Target System**: Salesforce
   - **Source Type**: REST_API
   - **Target Type**: SALESFORCE
   - **Object Name**: EZB__c
   - **Operation**: Upsert
   - **Direction**: Inbound
   - **Active**: ✓
   - **Batch Size**: 10
   - **Connector**: ECB Exchange Rates (lookup)

2. **Source Definition** (paste into field):
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

3. **Field Mappings** (paste into field):
```json
[
  {
    "source": "0",
    "target": "Rate__c",
    "action": "map"
  }
]
```

## 🚀 Deploy CustomObject to Salesforce

### Via SFDX CLI
```bash
cd salesforce/metadata
sfdx force:source:deploy -p . -u <your-alias>
```

### Via Salesforce Migration Tool
```bash
# Export and deploy metadata
sfdx force:mdapi:deploy -d salesforce/metadata -u <your-alias>
```

### Manual Upload
1. Go to Setup → Deploy → Deploy from Metadata API
2. Upload the `salesforce/metadata` folder as ZIP
3. Select "EZB__c" to deploy

## 📊 Testing the Flow

Once scheduler is created and EZB__c is deployed:

1. **Monitor in Dashboard**: Integration Agent → Scheduler → Look for "ECB to Salesforce Daily"
2. **Manual Trigger**: Click "Run Now" to test one-time sync
3. **View Results**: Check EZB__c records in Salesforce for EUR/USD rate data
4. **Auto-Schedule**: Daily at 09:00 UTC (configurable via timingDefinition)

## 🔗 Key Endpoints

- **REST API Create Schedule**: `POST http://localhost:9010/api/schedules`
- **REST API Get Schedules**: `GET http://localhost:9010/api/schedules`
- **Connector Test**: `POST http://localhost:9010/api/connectors/test`
- **Source Preview**: `POST http://localhost:9010/api/sources/preview`

## ✅ Next Steps

1. Deploy EZB__c CustomObject to Salesforce
2. Create Scheduler record (manual or via API)
3. Run dry-run test to validate ECB API response + field mapping
4. Enable daily schedule for production sync
5. Monitor in Monitoring tab for success/error metrics

---

**Timestamp**: 2026-04-30  
**Status**: Ready for deployment  
**Components**: CustomObject (EZB__c) ✅ | Connector (ECB Exchange Rates) ✅ | Scheduler (Pending manual creation)
