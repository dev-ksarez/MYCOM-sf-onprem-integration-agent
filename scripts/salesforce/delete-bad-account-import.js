#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');

dotenv.config();
require('ts-node/register/transpile-only');

const { SalesforceClient } = require('../../src/clients/salesforce/salesforce-client');

const START = '2026-05-01T19:59:00Z';
const END = '2026-05-01T20:18:30Z';
const ID_LIKE = /^001[A-Za-z0-9]{12,15}$/;

function timestampForFile() {
  return new Date().toISOString().replace(/[:.]/g, '-');
}

async function queryAll(connection, soql) {
  const records = [];
  let result = await connection.query(soql);
  records.push(...(result.records || []));

  while (!result.done && result.nextRecordsUrl) {
    result = await connection.queryMore(result.nextRecordsUrl);
    records.push(...(result.records || []));
  }

  return records;
}

function isBadRecord(record) {
  const name = String(record.Name || '').trim();
  const erpAddress = String(record.ERP_Address_Number__c || '').trim();
  const erpAccount = String(record.ERP_Account_Number__c || '').trim();

  return (
    !erpAccount &&
    name === erpAddress &&
    ID_LIKE.test(name) &&
    ID_LIKE.test(erpAddress)
  );
}

async function main() {
  const execute = process.argv.includes('--execute');
  const config = {
    loginUrl: process.env.SF_LOGIN_URL,
    clientId: process.env.SF_CLIENT_ID,
    clientSecret: process.env.SF_CLIENT_SECRET,
    queryLimit: Number(process.env.SF_QUERY_LIMIT || 5000)
  };

  const client = new SalesforceClient(config);
  await client.login();
  const connection = client.connection;

  const soql = [
    'SELECT Id, Name, ERP_Address_Number__c, ERP_Account_Number__c, CreatedDate, LastModifiedDate',
    'FROM Account',
    `WHERE CreatedDate >= ${START}`,
    `AND CreatedDate <= ${END}`,
    "AND Name LIKE '001%'",
    "AND ERP_Address_Number__c LIKE '001%'",
    'ORDER BY CreatedDate ASC'
  ].join(' ');

  const queried = await queryAll(connection, soql);
  const candidates = queried.filter(isBadRecord);

  const cleanupDir = path.join(process.cwd(), 'artifacts', 'migrations', 'mig-1777664202787', 'cleanup');
  fs.mkdirSync(cleanupDir, { recursive: true });
  const backupPath = path.join(cleanupDir, `${timestampForFile()}-bad-accounts.json`);
  fs.writeFileSync(backupPath, JSON.stringify({
    start: START,
    end: END,
    queriedCount: queried.length,
    candidateCount: candidates.length,
    execute,
    records: candidates
  }, null, 2));

  console.log(JSON.stringify({
    execute,
    queriedCount: queried.length,
    candidateCount: candidates.length,
    backupPath,
    sample: candidates.slice(0, 10)
  }, null, 2));

  if (!execute || !candidates.length) {
    return;
  }

  const batches = [];
  for (let index = 0; index < candidates.length; index += 200) {
    batches.push(candidates.slice(index, index + 200));
  }

  let deletedCount = 0;
  const failed = [];

  for (const batch of batches) {
    const ids = batch.map((record) => record.Id).filter(Boolean);
    if (!ids.length) {
      continue;
    }

    const results = await connection.sobject('Account').destroy(ids, { allOrNone: false });
    const normalized = Array.isArray(results) ? results : [results];
    normalized.forEach((result, idx) => {
      if (result && result.success) {
        deletedCount += 1;
        return;
      }
      failed.push({
        id: ids[idx],
        errors: result && result.errors ? result.errors : ['unknown error']
      });
    });
  }

  console.log(JSON.stringify({
    deletedCount,
    failedCount: failed.length,
    failed: failed.slice(0, 20),
    backupPath
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});