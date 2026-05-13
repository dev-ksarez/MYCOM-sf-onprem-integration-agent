#!/bin/bash

# Update SAGE100 Schedules in Sandbox
TARGET_ORG="${1:-AnnaburgerPROD1}"

echo "════════════════════════════════════════════════════════════"
echo "  ✅ Sandbox Schedule Update with Activation & Timing"
echo "════════════════════════════════════════════════════════════"
echo ""

# Show activation plan
echo "📝 SAGE100 Schedules (ACTIVATED):"
echo ""
node -e "
const fs = require('fs');
const config = JSON.parse(fs.readFileSync('artifacts/annaburger-prod1-setup-import.json', 'utf8'));
config.schedules.forEach((s, i) => {
  if (s.name.includes('ANN SAGE100')) {
    const timing = JSON.parse(s.timingDefinition);
    console.log('  ✅ [\${i+1}] \${s.name}');
    console.log('     Start Time: \${timing.startTime}');
    console.log('     Interval: \${timing.intervalMinutes} minutes');
    console.log('');
  }
});
"

echo "════════════════════════════════════════════════════════════"
echo ""
echo "📌 Deployment Steps:"
echo ""
echo "1️⃣  Authenticate to Sandbox (if not already logged in):"
echo "   sf org login web --instance-url https://test.salesforce.com"
echo ""
echo "2️⃣  Query all schedules:"
echo "   sf data query --query 'SELECT Id, Name, Active__c FROM MSD_SyncScheduler__c' --json"
echo ""
echo "3️⃣  For each SAGE100 schedule, update with new configuration"
echo "4️⃣  Deactivate non-SAGE100 schedules"
echo ""

