#!/usr/bin/env node

const fs = require('fs');

const CONFIG = JSON.parse(fs.readFileSync('artifacts/annaburger-prod1-setup-import.json', 'utf8'));

console.log('\n════════════════════════════════════════════════════════════');
console.log('  📋 SAGE100 Sandbox Update - Command Generator');
console.log('════════════════════════════════════════════════════════════\n');

// Step 1: Get schedule IDs from user or file
console.log('Step 1: Finding Schedule IDs\n');
console.log('Option A: If you have a file with schedule IDs:');
console.log('   Create tmp/schedule-ids.json with format:');
console.log('   {"ScheduleName": "RecordId", ...}\n');
console.log('Option B: Get IDs manually from Sandbox UI:\n');

let scheduleIds = {};

// Try to load from file if it exists
if (fs.existsSync('tmp/schedule-ids.json')) {
  try {
    scheduleIds = JSON.parse(fs.readFileSync('tmp/schedule-ids.json', 'utf8'));
    console.log('✅ Loaded schedule IDs from tmp/schedule-ids.json\n');
  } catch (e) {
    console.log('⚠️  Could not parse tmp/schedule-ids.json\n');
  }
}

if (Object.keys(scheduleIds).length === 0) {
  console.log('📌 To get schedule IDs, go to Sandbox → MSD_SyncScheduler__c');
  console.log('   Copy the URL: /lightning/r/MSD_SyncScheduler__c/[ID]/view\n');
  
  // Create template
  scheduleIds = {
    'ANN SAGE100 KHKAdressen -> Account': 'a069O00000fLJJVQA4',
    'ANN SAGE100 KHKAnsprechpartner -> Contact': 'a069O00000fLJJWQA4',
    'ANN SAGE100 KHKArtikelgruppen -> ProductGroup': 'a069O00000fLJJXQA4',
    'ANN SAGE100 KHKArtikel -> Product2': 'a069O00000fLJJYQA4',
    'ANN SAGE100 KHKKontokorrent -> ERP_OpenItems__c': 'a069O00000fLJJZQA4',
    'ANN SAGE100 KHKArchivVKBelege (Angebote) -> Quote': 'a069O00000fLJJaQA4',
    'ANN SAGE100 KHKArchivVKBelege (Rechnungen) -> Order': 'a069O00000fLJJbQA4',
    'ANN SAGE100 KHKArtikelZubehoer -> ERP_ProductAccessory__c': 'a069O00000fLJJcQA4',
    'ANN SAGE100 KHKArtikelBezeichnung -> ERP_ProductDescription__c': 'a069O00000fLJJdQA4',
    'ANN SAGE100 KHKVerkausprojekte -> Opportunity': 'a069O00000fLJJeQA4'
  };
  
  fs.writeFileSync('tmp/schedule-ids-template.json', JSON.stringify(scheduleIds, null, 2));
  console.log('📄 Template created: tmp/schedule-ids-template.json');
  console.log('   Replace with actual IDs and save as: tmp/schedule-ids.json\n');
}

// Step 2: Generate CLI commands
console.log('════════════════════════════════════════════════════════════\n');
console.log('Step 2: Update Commands\n');

const cliCommands = [];

CONFIG.schedules.forEach((schedule, idx) => {
  if (!schedule.name.includes('ANN SAGE100')) return;

  const timing = JSON.parse(schedule.timingDefinition);
  const scheduleId = scheduleIds[schedule.name] || `<ID_${idx + 1}>`;
  
  const cmd = `sf data update record MSD_SyncScheduler__c ${scheduleId} \\
  --values "Active__c=true" \\
  --values "MSD_UPSERT_Key__c='${schedule.externalIdField}'"`;

  cliCommands.push({
    name: schedule.name,
    id: scheduleId,
    command: cmd
  });

  console.log(`# [${idx + 1}] ${schedule.name}`);
  console.log(cmd);
  console.log('');
});

// Save to file
fs.writeFileSync('tmp/sandbox-cli-commands.sh', `#!/bin/bash
# SAGE100 Schedules Update Commands
# Generated: ${new Date().toISOString()}
# Target: Sandbox (annaburger--dev.sandbox.my.salesforce.com)

echo "🔄 Updating SAGE100 Schedules..."
echo ""

${cliCommands.map((c, i) => `
# [${i + 1}] ${c.name}
echo "⏳ Updating [\${(${i+1})}/10] ${c.name}..."
${c.command} || echo "❌ Failed"
echo ""
`).join('\n')}

echo "✅ Done!"
`);

console.log('════════════════════════════════════════════════════════════');
console.log('\n📝 To deploy:\n');
console.log('Option 1: Run CLI commands manually (one by one)');
console.log('   Copy & paste each command above\n');
console.log('Option 2: Use generated script:');
console.log('   chmod +x tmp/sandbox-cli-commands.sh');
console.log('   ./tmp/sandbox-cli-commands.sh\n');
console.log('Option 3: Use Web UI:');
console.log('   1. Open Sandbox → MSD_SyncScheduler__c');
console.log('   2. Edit each SAGE100 schedule');
console.log('   3. Set Active__c = true');
console.log('   4. Save\n');

console.log('════════════════════════════════════════════════════════════');
console.log('✨ Script saved to: tmp/sandbox-cli-commands.sh');
console.log('════════════════════════════════════════════════════════════\n');

// Generate summary report
const report = {
  generated: new Date().toISOString(),
  target: 'Sandbox (annaburger--dev.sandbox.my.salesforce.com)',
  schedulesToUpdate: CONFIG.schedules.filter(s => s.name.includes('ANN SAGE100')).length,
  scheduleIds,
  cliCommands: cliCommands.map(c => ({
    name: c.name,
    id: c.id
  }))
};

fs.writeFileSync('tmp/sandbox-update-report.json', JSON.stringify(report, null, 2));
console.log('📊 Report saved to: tmp/sandbox-update-report.json\n');
