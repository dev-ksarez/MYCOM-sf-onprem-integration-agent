import assert from "node:assert/strict";
import {
  createEndpointRecords,
  parseEndpointSourceDefinition,
  validateEndpointRequestBody
} from "../src/source-adapters/endpoint/endpoint-source-adapter";

function runSingleRecordScenario(): void {
  const definition = parseEndpointSourceDefinition(JSON.stringify({
    method: "POST",
    path: "/v1/orders",
    recordMode: "single",
    queryFields: ["tenant"],
    headerFields: ["x-request-id"],
    validation: {
      requiredBodyFields: ["orderNumber", "customerNumber"]
    }
  }));

  const body = {
    orderNumber: "4711",
    customerNumber: "100200",
    amount: 129.95
  };
  validateEndpointRequestBody(definition, body);
  const records = createEndpointRecords(definition, {
    body,
    query: { tenant: "test" },
    headers: { "x-request-id": "req-1" },
    request: { method: "POST", path: "/api/inbound/orders/v1", receivedAt: "2026-06-08T10:00:00.000Z" },
    auth: { subject: "client-a" }
  });

  assert.equal(records.length, 1);
  assert.equal((records[0].values.body as Record<string, unknown>).orderNumber, "4711");
  assert.equal((records[0].values.query as Record<string, unknown>).tenant, "test");
  assert.equal((records[0].values.headers as Record<string, unknown>)["x-request-id"], "req-1");
  assert.equal((records[0].values.auth as Record<string, unknown>).subject, "client-a");
}

function runArrayRecordScenario(): void {
  const definition = parseEndpointSourceDefinition(JSON.stringify({
    method: "POST",
    path: "/v1/orders/batch",
    recordMode: "array",
    bodyPath: "orders"
  }));

  const records = createEndpointRecords(definition, {
    body: {
      orders: [
        { orderNumber: "4711" },
        { orderNumber: "4712" }
      ]
    },
    query: {},
    headers: {},
    request: { method: "POST", path: "/api/inbound/orders/v1/batch" },
    auth: {}
  });

  assert.equal(records.length, 2);
  assert.equal((records[1].values.body as Record<string, unknown>).orderNumber, "4712");
}

function runValidationFailureScenario(): void {
  const definition = parseEndpointSourceDefinition(JSON.stringify({
    method: "POST",
    path: "/v1/orders",
    validation: {
      requiredBodyFields: ["orderNumber", "customerNumber"]
    }
  }));

  assert.throws(
    () => validateEndpointRequestBody(definition, { orderNumber: "4711" }),
    /customerNumber/
  );
}

function runArrayFailureScenario(): void {
  const definition = parseEndpointSourceDefinition(JSON.stringify({
    method: "POST",
    path: "/v1/orders/batch",
    recordMode: "array",
    bodyPath: "orders"
  }));

  assert.throws(
    () => createEndpointRecords(definition, {
      body: { orders: { orderNumber: "4711" } },
      query: {},
      headers: {},
      request: {},
      auth: {}
    }),
    /kein Array/
  );
}

runSingleRecordScenario();
runArrayRecordScenario();
runValidationFailureScenario();
runArrayFailureScenario();

console.log("Endpoint source adapter smoke test passed.");
