# Beispiel: Bestellimport per Agent-Endpunkt

Dieses Beispiel beschreibt einen externen Auftragseingang, bei dem ein Fremdsystem Bestellungen per HTTP an den Agenten sendet. Der Agent nimmt den Request unter einem konfigurierten Root-Endpunkt entgegen, prueft OAuth2/Bearer Authentifizierung und schreibt die Daten ueber das Scheduler-Mapping nach Salesforce.

## Connector

Connector Type: `AGENT_ENDPOINT`

```json
{
  "name": "Inbound Orders API",
  "connectorType": "AGENT_ENDPOINT",
  "targetSystem": "Agent",
  "direction": "Inbound",
  "secretKey": "ORDER_ENDPOINT_BEARER_TOKEN",
  "parameters": {
    "rootPath": "/api/inbound/orders",
    "authType": "oauth2",
    "maxBodyBytes": 1048576,
    "trustedHeaders": ["x-request-id", "x-correlation-id"]
  }
}
```

Hinweis: Fuer den MVP kann `secretKey` auf eine Umgebungsvariable zeigen, deren Wert als Bearer Token akzeptiert wird. Alternativ kann in `parameters.oauth2.introspectionUrl` ein OAuth2 Introspection Endpoint hinterlegt werden.

## Scheduler

- Source Type: `ENDPOINT`
- Target Type: `SALESFORCE`
- Operation: `upsert`

Source Definition:

```json
{
  "method": "POST",
  "path": "/v1",
  "contentType": "application/json",
  "recordMode": "single",
  "queryFields": ["tenant"],
  "headerFields": ["x-request-id"],
  "response": {
    "successStatus": 202,
    "errorStatus": 422
  },
  "validation": {
    "requiredBodyFields": ["orderNumber", "customerNumber"]
  }
}
```

Target Definition:

```json
{
  "objectApiName": "Order__c",
  "operation": "upsert",
  "externalIdField": "External_Order_Number__c"
}
```

Mapping Definition:

```json
[
  {
    "sourceField": "body.orderNumber",
    "targetField": "External_Order_Number__c",
    "targetType": "string",
    "transform": { "type": "NONE" }
  },
  {
    "sourceField": "body.customerNumber",
    "targetField": "Customer_Number__c",
    "targetType": "string",
    "transform": { "type": "NONE" }
  },
  {
    "sourceField": "body.amount",
    "targetField": "Amount__c",
    "targetType": "number",
    "transform": { "type": "NONE" }
  },
  {
    "sourceField": "request.receivedAt",
    "targetField": "Received_At__c",
    "targetType": "datetime",
    "transform": { "type": "NONE" }
  }
]
```

## Beispielrequest

```bash
curl -i \
  -X POST "https://agent.example.com/api/inbound/orders/v1?tenant=prod" \
  -H "Authorization: Bearer ${ORDER_ENDPOINT_BEARER_TOKEN}" \
  -H "Content-Type: application/json" \
  -H "x-request-id: order-4711" \
  -d '{
    "orderNumber": "4711",
    "customerNumber": "100200",
    "amount": 129.95
  }'
```

Erwartete Antwort bei erfolgreicher Verarbeitung:

```json
{
  "status": "Success",
  "runId": "a01...",
  "correlationId": "order-4711",
  "scheduleId": "a02..."
}
```

## Array-Import

Fuer Batch-Requests kann derselbe Connector verwendet werden. Der Scheduler bekommt dann `recordMode=array` und optional `bodyPath`.

```json
{
  "method": "POST",
  "path": "/batch",
  "contentType": "application/json",
  "recordMode": "array",
  "bodyPath": "orders",
  "headerFields": ["x-request-id"],
  "response": {
    "successStatus": 202,
    "errorStatus": 422
  },
  "validation": {
    "requiredBodyFields": ["orders"]
  }
}
```

Request Body:

```json
{
  "orders": [
    { "orderNumber": "4711", "customerNumber": "100200", "amount": 129.95 },
    { "orderNumber": "4712", "customerNumber": "100300", "amount": 79.5 }
  ]
}
```
