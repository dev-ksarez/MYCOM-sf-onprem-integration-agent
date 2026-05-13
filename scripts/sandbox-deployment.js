#!/usr/bin/env node

const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');
const util = require('util');

const execPromise = util.promisify(exec);

const CONFIG = JSON.parse(fs.readFileSync('artifacts/annaburger-prod1-setup-import.json', 'utf8'));

const SAGE100_SCHEDULES = [
  { idx: 1, name: 'Account', table: 'KHKAdressen' },
  { idx: 2, name: 'Contact', table: 'KHKAnsprechpartner' },
  { idx: 3, name: 'ProductGroup', table: 'KHKArtikelgruppen' },
  { idx: 4, name: 'Product2', table: 'KHKArtikel' },
  { idx: 5, name: 'ERP_OpenItems__c', table: 'KHKKontokorrent' },
  { idx: 6, name: 'Quote', table: 'KHKArchivVKBelege (Angebote)' },
  { idx: 7, name: 'Order', table: 'KHKArchivVKBelege (Rechnungen)' },
  { idx: 8, name: 'ERP_ProductAccessory__c', table: 'KHKArtikelZubehoer' },
  { idx: 9, name: 'ERP_ProductDescription__c', table: 'KHKArtikelBezeichnung' },
  { idx: 10, name: 'Opportunity', table: 'KHKVerkausprojekte' }
];

async function main() {
  console.log('\n════════════════════════════════════════════════════════════');
  console.log('  🚀 SAGE100 Sandbox Deployment - Automated Setup');
  console.log('════════════════════════════════════════════════════════════\n');

  // Step 1: Check Salesforce CLI
  console.log('1️⃣  Checking Salesforce CLI...');
  try {
    const { stdout } = await execPromise('sf --version');
    console.log(`   ✅ ${stdout.trim()}\n`);
  } catch (error) {
    console.log('   ❌ Salesforce CLI not found. Install with: npm install -g @salesforce/cli\n');
    process.exit(1);
  }

  // Step 2: List existing orgs
  console.log('2️⃣  Checking connected orgs...');
  try {
    const { stdout } = await execPromise('sf org list --json', { maxBuffer: 10 * 1024 * 1024 });
    const orgs = JSON.parse(stdout);
    
    if (orgs.result && orgs.result.nonScratchOrgs && orgs.result.nonScratchOrgs.length > 0) {
      console.log('   ✅ Found orgs:\n');
      orgs.result.nonScratchOrgs.forEach(org => {
        console.log(`      • ${org.alias || org.username} (${org.isDefaultDevHub ? '🔷 DEFAULT' : ''})`);
      });
      console.log('');
    } else {
      console.log('   ⚠️  No connected orgs found\n');
      console.log('   📌 To login to Sandbox:');
      console.log('      sf org login web --instance-url https://test.salesforce.com --alias annaburger-sandbox\n');
      process.exit(1);
    }
  } catch (error) {
    console.log('   ❌ Error listing orgs\n');
  }

  // Step 3: Query schedule IDs
  console.log('3️⃣  Querying Schedule IDs from Sandbox...');
  try {
    const { stdout } = await execPromise(
      'sf data query --query "SELECT Id, Name, Active__c FROM MSD_SyncScheduler__c" --json',
      { maxBuffer: 10 * 1024 * 1024 }
    );
    
    const result = JSON.parse(stdout);
    const schedules = result.result?.records || [];
    
    console.log(`   ✅ Found ${schedules.length} schedules\n`);
    
    // Map schedule names to IDs
    const scheduleMap = {};
    schedules.forEach(s => {
      SAGE100_SCHEDULES.forEach(sage => {
        if (s.Name.includes(sage.table) || s.Name.includes(sage.name)) {
          scheduleMap[sage.name] = s.Id;
        }
      });
    });

    // Save for reference
    fs.writeFileSync('tmp/sandbox-schedule-ids.json', JSON.stringify({
      timestamp: new Date().toISOString(),
      schedules: scheduleMap,
      allSchedules: schedules
    }, null, 2));

    console.log('   📄 Schedule mapping saved to: tmp/sandbox-schedule-ids.json\n');

    // Step 4: Show activation plan
    console.log('4️⃣  Activation Plan:\n');
    SAGE100_SCHEDULES.forEach((sage, idx) => {
      const config = CONFIG.schedules[idx];
      const timing = JSON.parse(config.timingDefinition);
      const hasId = scheduleMap[sage.name];
      
      console.log(`   [${sage.idx}] ${sage.name}`);
      console.log(`       Table: ${sage.table}`);
      console.log(`       Start Time: ${timing.startTime}`);
      console.log(`       Interval: ${timing.intervalMinutes} min`);
      console.log(`       UPSERT Key: ${config.externalIdField}`);
      console.log(`       Status: ${hasId ? '✅ Found' : '⚠️  NOT FOUND'}`);
      console.log('');
    });

    console.log('════════════════════════════════════════════════════════════\n');

    // Step 5: Show update commands
    console.log('5️⃣  To activate all schedules, run:\n');
    console.log('   Option A: Use the interactive update script');
    console.log('      node scripts/update-sandbox-schedules-interactive.js\n');
    console.log('   Option B: Manual CLI commands per schedule\n');

    SAGE100_SCHEDULES.slice(0, 2).forEach((sage, idx) => {
      const config = CONFIG.schedules[idx];
      const schedId = scheduleMap[sage.name] || `<ID_${sage.idx}>`;
      
      console.log(`      # ${sage.name}`);
      console.log(`      sf data update record MSD_SyncScheduler__c ${schedId} \\`);
      console.log(`        --values "Active__c=true" \\`);
      console.log(`        --values "MSD_UPSERT_Key__c='${config.externalIdField}'"`);
      console.log('');
    });

    console.log('      [... 8 more schedules ...]\n');

  } catch (error) {
    console.log(`   ❌ Query failed: ${error.message}\n`);
    console.log('   📌 Troubleshooting:');
    console.log('      1. Check if Sandbox org is connected: sf org list');
    console.log('      2. Verify MSD_SyncScheduler__c exists in Sandbox');
    console.log('      3. Run: sf org login web --instance-url https://test.salesforce.com\n');
  }

  console.log('════════════════════════════════════════════════════════════');
  console.log('✨ Deployment ready! Next: Run update script or manual CLI commands');
  console.log('════════════════════════════════════════════════════════════\n');
}

main().catch(console.error);
