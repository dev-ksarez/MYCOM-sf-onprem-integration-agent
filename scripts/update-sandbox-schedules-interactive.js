#!/usr/bin/env node

const fs = require('fs');
const readline = require('readline');
const dotenv = require('dotenv');

dotenv.config();

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

const question = (prompt) => new Promise(resolve => rl.question(prompt, resolve));

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

async function getAccessToken() {
  const loginUrl = (process.env.SF_LOGIN_URL || '').replace(/\/$/, '');
  const body = new URLSearchParams({
    grant_type: 'password',
    client_id: process.env.SF_CLIENT_ID || '',
    client_secret: process.env.SF_CLIENT_SECRET || '',
    username: process.env.SF_USERNAME || '',
    password: process.env.SF_PASSWORD || ''
  });

  try {
    const resp = await fetch(`${loginUrl}/services/oauth2/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body
    });
    
    if (!resp.ok) {
      const errText = await resp.text();
      console.log(`❌ OAuth Error: ${errText}`);
      return null;
    }
    
    const data = await resp.json();
    return {
      access_token: data.access_token,
      instance_url: data.instance_url
    };
  } catch (error) {
    console.log(`❌ Authentication failed: ${error.message}`);
    return null;
  }
}

async function querySchedules() {
  console.log('\n🔍 Querying schedules from Sandbox...\n');
  
  const auth = await getAccessToken();
  if (!auth) return [];

  try {
    const query = encodeURIComponent("SELECT Id, Name FROM MSD_SyncScheduler__c");
    const resp = await fetch(
      `${auth.instance_url}/services/data/v66.0/query?q=${query}`,
      { headers: { Authorization: `Bearer ${auth.access_token}` } }
    );
    
    if (!resp.ok) {
      console.log(`❌ Query failed: ${resp.status}`);
      return [];
    }
    
    const result = await resp.json();
    return result.records || [];
  } catch (error) {
    console.log(`❌ Failed to query schedules: ${error.message}`);
    return [];
  }
}

async function updateSchedule(auth, scheduleId, scheduleConfig) {
  const timing = JSON.parse(scheduleConfig.timingDefinition);
  
  try {
    const updateData = {
      Active__c: true,
      MSD_UPSERT_Key__c: scheduleConfig.externalIdField
    };

    const resp = await fetch(
      `${auth.instance_url}/services/data/v66.0/sobjects/MSD_SyncScheduler__c/${scheduleId}`,
      {
        method: 'PATCH',
        headers: {
          'Authorization': `Bearer ${auth.access_token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(updateData)
      }
    );

    if (resp.ok || resp.status === 204) {
      return true;
    } else {
      const errText = await resp.text();
      console.log(`   ❌ Error: ${errText}`);
      return false;
    }
  } catch (error) {
    console.log(`   ❌ Error: ${error.message}`);
    return false;
  }
}

async function main() {
  console.log('\n════════════════════════════════════════════════════════════');
  console.log('  🚀 SAGE100 Sandbox - Interactive Schedule Update');
  console.log('════════════════════════════════════════════════════════════');

  // Get access token
  console.log('\n🔐 Authenticating to Sandbox...');
  const auth = await getAccessToken();
  
  if (!auth) {
    console.log('\n❌ Authentication failed. Check .env credentials.\n');
    rl.close();
    process.exit(1);
  }

  console.log('✅ Authenticated!\n');

  // Query all schedules
  const allSchedules = await querySchedules();
  
  if (allSchedules.length === 0) {
    console.log('\n❌ No schedules found. Exiting.');
    rl.close();
    process.exit(1);
  }

  console.log(`\n✅ Found ${allSchedules.length} schedules in Sandbox:\n`);
  
  // Display and map
  const scheduleMap = {};
  allSchedules.forEach((s, i) => {
    console.log(`   [${i + 1}] ${s.Name} (${s.Id})`);
    
    SAGE100_SCHEDULES.forEach(sage => {
      if (s.Name.includes(sage.table) || s.Name.includes(sage.name)) {
        scheduleMap[sage.name] = { id: s.Id, name: s.Name };
      }
    });
  });

  console.log('\n════════════════════════════════════════════════════════════');
  console.log('\n📋 SAGE100 Schedules to Activate:\n');

  let found = 0;
  SAGE100_SCHEDULES.forEach(sage => {
    const mapped = scheduleMap[sage.name];
    if (mapped) {
      found++;
      console.log(`   ✅ [${sage.idx}] ${sage.name}`);
      console.log(`      Mapping: ${mapped.name}`);
      console.log(`      ID: ${mapped.id}\n`);
    } else {
      console.log(`   ⚠️  [${sage.idx}] ${sage.name} - NOT FOUND`);
    }
  });

  console.log(`\n${found}/10 SAGE100 schedules found\n`);

  // Ask for confirmation
  const confirm = await question('📌 Proceed with activation? (y/n) ');
  
  if (confirm.toLowerCase() !== 'y') {
    console.log('\n👋 Cancelled.\n');
    rl.close();
    process.exit(0);
  }

  // Perform updates
  console.log('\n🔄 Activating schedules...\n');
  
  let updated = 0;
  let failed = 0;

  for (let i = 0; i < SAGE100_SCHEDULES.length; i++) {
    const sage = SAGE100_SCHEDULES[i];
    const mapped = scheduleMap[sage.name];
    
    if (!mapped) {
      console.log(`   ⏭️  [${sage.idx}] ${sage.name} - SKIPPED (not found)\n`);
      continue;
    }

    const config = CONFIG.schedules[i];
    const timing = JSON.parse(config.timingDefinition);
    
    console.log(`   ⏳ [${sage.idx}] ${sage.name}...`);
    
    const success = await updateSchedule(auth, mapped.id, config);
    if (success) {
      console.log(`      ✅ Activated (${timing.startTime}, ${timing.intervalMinutes} min)\n`);
      updated++;
    } else {
      console.log(`      ❌ Failed\n`);
      failed++;
    }
  }

  // Summary
  console.log('════════════════════════════════════════════════════════════');
  console.log('\n📊 Update Summary:\n');
  console.log(`   ✅ Activated: ${updated}`);
  console.log(`   ❌ Failed: ${failed}`);
  console.log(`   ⏭️  Skipped: ${10 - updated - failed}\n`);

  // Save results
  fs.writeFileSync('tmp/sandbox-update-results.json', JSON.stringify({
    timestamp: new Date().toISOString(),
    updated,
    failed,
    scheduleMap
  }, null, 2));

  console.log('📄 Results saved to: tmp/sandbox-update-results.json\n');
  console.log('════════════════════════════════════════════════════════════\n');

  rl.close();
}

main().catch(console.error);
