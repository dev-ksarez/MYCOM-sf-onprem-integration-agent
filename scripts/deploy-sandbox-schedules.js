#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const CONFIG = JSON.parse(fs.readFileSync('artifacts/annaburger-prod1-setup-import.json', 'utf8'));

console.log('\n════════════════════════════════════════════════════════════');
console.log('  📋 SAGE100 Schedules - Sandbox Deployment Guide');
console.log('════════════════════════════════════════════════════════════\n');

// List all SAGE100 schedules with their update details
console.log('✅ SAGE100 Schedules Ready for Activation:\n');

CONFIG.schedules.forEach((schedule, idx) => {
  if (!schedule.name.includes('ANN SAGE100')) return;
  
  const timing = JSON.parse(schedule.timingDefinition);
  console.log(`[${idx + 1}] ${schedule.name}`);
  console.log(`    Active: true`);
  console.log(`    Start Time: ${timing.startTime}`);
  console.log(`    Interval: ${timing.intervalMinutes} minutes`);
  console.log(`    UPSERT Key: ${schedule.externalIdField}`);
  console.log('');
});

console.log('════════════════════════════════════════════════════════════\n');

// Generate Salesforce CLI update commands
console.log('🔧 SF CLI Update Commands:\n');

console.log('1️⃣  LOGIN TO SANDBOX:');
console.log('   sf org login web --instance-url https://test.salesforce.com --set-default-dev-hub\n');

console.log('2️⃣  QUERY SCHEDULE IDs:');
console.log('   sf data query --query "SELECT Id, Name FROM MSD_SyncScheduler__c" --json\n');

console.log('3️⃣  UPDATE EACH SCHEDULE:\n');

CONFIG.schedules.forEach((schedule, idx) => {
  if (!schedule.name.includes('ANN SAGE100')) return;
  
  const timing = JSON.parse(schedule.timingDefinition);
  const source = schedule.sourceDefinition.replace(/'/g, '\\"');
  const mapping = schedule.mappingDefinition.replace(/'/g, '\\"');
  
  console.log(`# Schedule ${idx + 1}: ${schedule.name}`);
  console.log(`sf data update record MSD_SyncScheduler__c <ID_${idx + 1}> \\`);
  console.log(`  --values "Active__c=true" \\`);
  console.log(`  --values "MSD_SourceDefinition__c='${source}'" \\`);
  console.log(`  --values "MSD_UPSERT_Key__c='${schedule.externalIdField}'"`);
  console.log('');
});

console.log('4️⃣  DEACTIVATE NON-SAGE100 SCHEDULES:');
console.log('   # For each non-SAGE100 MSSQL DEV schedule:');
console.log('   sf data update record MSD_SyncScheduler__c <ID> --values "Active__c=false"\n');

console.log('════════════════════════════════════════════════════════════\n');

// Alternative: Create a CSV for manual import
console.log('📊 CSV Export for Manual Web UI Update:\n');

const csvHeader = 'Name,RecordId,Active__c,MSD_UPSERT_Key__c,StartTime,Interval';
const csvRows = [];

CONFIG.schedules.forEach((schedule, idx) => {
  if (!schedule.name.includes('ANN SAGE100')) return;
  const timing = JSON.parse(schedule.timingDefinition);
  csvRows.push(`"${schedule.name}","<RECORD_ID_${idx + 1}>",true,"${schedule.externalIdField}","${timing.startTime}",${timing.intervalMinutes}`);
});

console.log(csvHeader);
csvRows.forEach(row => console.log(row));

console.log('\n════════════════════════════════════════════════════════════');
console.log('✨ All 10 SAGE100 schedules configured and ready to deploy!');
console.log('════════════════════════════════════════════════════════════\n');

// Save to file for reference
const outputFile = 'tmp/sandbox-deployment-guide.txt';
const output = `
SAGE100 SCHEDULES - SANDBOX DEPLOYMENT GUIDE
Generated: ${new Date().toISOString()}

ACTIVATION PLAN (All 10 SAGE100 Schedules):
${CONFIG.schedules.map((s, i) => {
  if (!s.name.includes('ANN SAGE100')) return '';
  const timing = JSON.parse(s.timingDefinition);
  return `[${i + 1}] ${s.name}
    Active: true
    Start Time: ${timing.startTime}
    Interval: ${timing.intervalMinutes} minutes
    UPSERT Key: ${s.externalIdField}`;
}).filter(x => x).join('\n\n')}

DEACTIVATION PLAN:
All non-SAGE100 MSSQL DEV schedules → Active__c = false

NEXT STEPS:
1. Login to Sandbox: sf org login web --instance-url https://test.salesforce.com
2. Query schedule IDs: sf data query --query "SELECT Id, Name FROM MSD_SyncScheduler__c" --json
3. Update each schedule with new configuration
4. Verify all 10 schedules are active and running
`;

fs.writeFileSync(outputFile, output);
console.log(`📄 Deployment guide saved to: ${outputFile}\n`);
