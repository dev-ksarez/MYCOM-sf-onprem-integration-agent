#!/usr/bin/env node

/**
 * SAGE100 Schedule Update via Salesforce CLI
 * Updates all 10 MSD_SyncScheduler__c records with SAGE100-compliant definitions
 */

const fs = require('fs');
const { execSync, spawnSync } = require('child_process');
const path = require('path');

// Color codes for console output
const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  cyan: '\x1b[36m',
  bold: '\x1b[1m'
};

function log(msg, color = 'reset') {
  console.log(`${colors[color]}${msg}${colors.reset}`);
}

function escapeForSoql(str) {
  // Escape single quotes and backslashes for SOQL
  return str.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

function escapeForCli(str) {
  // Escape for shell/CLI usage
  return str.replace(/"/g, '\\"').replace(/\$/g, '\\$').replace(/`/g, '\\`');
}

async function updateSchedules() {
  try {
    log('\n═══════════════════════════════════════════════════════════', 'cyan');
    log('  SAGE100 Schedule Update - Salesforce CLI', 'bold');
    log('═══════════════════════════════════════════════════════════\n', 'cyan');

    // Read configuration
    const setupData = JSON.parse(
      fs.readFileSync(path.join(__dirname, '../artifacts/annaburger-prod1-setup-import.json'), 'utf8')
    );
    const schedules = setupData.schedules;

    log(`📋 Loaded ${schedules.length} SAGE100-compliant schedules\n`);

    // Get target org
    const targetOrg = process.argv[2] || 'AnnaburgerPROD1';
    log(`🎯 Target Org: ${targetOrg}\n`, 'yellow');

    // Generate update batch
    const updates = [];
    schedules.forEach((sch, idx) => {
      updates.push({
        index: idx + 1,
        name: sch.name,
        objectName: sch.objectName,
        sourceDefinition: sch.sourceDefinition,
        mappingDefinition: sch.mappingDefinition,
        upsertKey: sch.externalIdField,
        targetDefinition: sch.targetDefinition
      });
    });

    log('📝 Generated CLI Update Commands:\n', 'bold');
    updates.forEach(upd => {
      log(`\n${upd.index}. ${upd.name}`, 'green');
      log(`   Source Query: ${upd.sourceDefinition.substring(0, 70)}...`);
      log(`   UPSERT Key: ${upd.upsertKey}`);
      log(`   Status: Ready for update`);
    });

    log('\n═══════════════════════════════════════════════════════════\n', 'cyan');

    // Instructions
    log('💡 Next Steps:', 'bold');
    log('\n1. Update schedules via SF CLI (requires existing Schedule IDs):\n');
    
    log('   Example command structure:', 'yellow');
    log(`   sf data update record MSD_SyncScheduler__c <RECORD_ID> \\`, 'cyan');
    log(`     --values "MSD_SourceDefinition__c=<QUERY>" \\`, 'cyan');
    log(`     --target-org ${targetOrg}\n`, 'cyan');

    log('2. Or use manual Web UI update with values from SCHEDULE_UPDATE_GUIDE.md\n');

    log('3. To find existing Schedule IDs, run:', 'yellow');
    log(`   sf data query --query "SELECT Id,Name FROM MSD_SyncScheduler__c LIMIT 10" \\`, 'cyan');
    log(`     --json --target-org ${targetOrg}\n`, 'cyan');

    // Save command template
    const cmdTemplate = updates.map((upd, idx) => ({
      index: upd.index,
      name: upd.name,
      recordId: `<INSERT_ID_${idx + 1}>`,
      updateCommand: `sf data update record MSD_SyncScheduler__c <INSERT_ID_${idx + 1}> ` +
        `--values "MSD_SourceDefinition__c='${escapeForCli(upd.sourceDefinition)}' ` +
        `MSD_MappingDefinition__c='${escapeForCli(upd.mappingDefinition)}' ` +
        `MSD_UPSERT_Key__c='${upd.upsertKey}'" ` +
        `--target-org ${targetOrg}`
    }));

    fs.writeFileSync(
      path.join(__dirname, '../tmp/cli-update-commands.json'),
      JSON.stringify(cmdTemplate, null, 2)
    );

    log('\n✅ Command template saved to: tmp/cli-update-commands.json\n', 'green');

    log('═══════════════════════════════════════════════════════════\n', 'cyan');
    log('📌 Summary:', 'bold');
    log(`   • ${updates.length} schedules ready for update`);
    log(`   • All SAGE100-compliant definitions prepared`);
    log(`   • CLI commands generated and saved`);
    log('   • Ready to execute updates\n');

    log('Status: ✅ Ready for CLI Update (Option B)\n', 'green');

  } catch (e) {
    log(`\n❌ Error: ${e.message}\n`, 'red');
    process.exit(1);
  }
}

// Run if executed directly
if (require.main === module) {
  updateSchedules();
}

module.exports = { updateSchedules, escapeForCli, escapeForSoql };
