#!/bin/bash

# SAGE100 Schedule Batch Update via Salesforce CLI
# This script updates all 10 MSD_SyncScheduler__c records with SAGE100-compliant definitions

TARGET_ORG="AnnaburgerPROD1"
SCHEDULES_JSON="artifacts/annaburger-prod1-setup-import.json"

echo "════════════════════════════════════════════════════════════"
echo "  ✅ SAGE100 Schedule CLI Batch Update"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "Target Org: $TARGET_ORG"
echo "Configuration: $SCHEDULES_JSON"
echo ""

# Step 1: Query existing schedules
echo "📋 Step 1: Retrieving existing schedules from Salesforce..."
echo ""

SCHEDULES=$(sf data query --query "SELECT Id, Name FROM MSD_SyncScheduler__c ORDER BY CreatedDate LIMIT 10" --json --target-org "$TARGET_ORG" 2>/dev/null)

if [ $? -ne 0 ]; then
    echo "⚠️  Could not query schedules from $TARGET_ORG"
    echo ""
    echo "Possible reasons:"
    echo "  • Custom object MSD_SyncScheduler__c may not exist in this org"
    echo "  • Insufficient permissions"
    echo "  • Org authentication issue"
    echo ""
    echo "📌 Solution: Run the following command to check:"
    echo "   sf data query --query \"SELECT Id FROM MSD_SyncScheduler__c LIMIT 1\" --target-org $TARGET_ORG"
    echo ""
    exit 1
fi

# Parse schedule IDs
SCHEDULE_IDS=$(echo "$SCHEDULES" | jq -r '.result.records[].Id' 2>/dev/null)
SCHEDULE_COUNT=$(echo "$SCHEDULES" | jq '.result.totalSize' 2>/dev/null)

if [ "$SCHEDULE_COUNT" -eq 0 ]; then
    echo "❌ No schedules found in $TARGET_ORG"
    echo ""
    echo "📌 You may need to:"
    echo "   1. Create schedules in Salesforce first"
    echo "   2. Or use a different org that has schedules"
    echo ""
    exit 1
fi

echo "✅ Found $SCHEDULE_COUNT schedules"
echo ""
echo "$SCHEDULES" | jq -r '.result.records[] | "\(.Id) - \(.Name)"'
echo ""
echo "════════════════════════════════════════════════════════════"
echo ""

# Save IDs for later use
echo "$SCHEDULE_IDS" > tmp/schedule-ids.txt

echo "📝 Step 2: Generating update commands..."
echo ""

# Create a mapping of schedule names to IDs
declare -A SCHEDULE_MAP
while IFS= read -r line; do
    ID=$(echo "$line" | jq -r '.Id')
    NAME=$(echo "$line" | jq -r '.Name')
    SCHEDULE_MAP["$NAME"]="$ID"
done < <(echo "$SCHEDULES" | jq -c '.result.records[]')

# Read SAGE100 configuration
CONFIG=$(cat "$SCHEDULES_JSON")

# Generate and execute update commands
UPDATE_COUNT=0
SUCCESS_COUNT=0
FAILED_COUNT=0

echo "🔄 Updating schedules..."
echo ""

# Extract schedule names and definitions
SCHEDULE_NAMES=$(echo "$CONFIG" | jq -r '.schedules[].name')

for i in {0..9}; do
    # Get schedule at index $i
    SCHEDULE=$(echo "$CONFIG" | jq ".schedules[$i]")
    
    SCHEDULE_NAME=$(echo "$SCHEDULE" | jq -r '.name')
    SOURCE_DEF=$(echo "$SCHEDULE" | jq -r '.sourceDefinition' | sed "s/'/\\\\'/g")
    MAPPING_DEF=$(echo "$SCHEDULE" | jq -r '.mappingDefinition' | sed "s/'/\\\\'/g")
    UPSERT_KEY=$(echo "$SCHEDULE" | jq -r '.externalIdField')
    
    # Find matching ID
    SCHEDULE_ID="${SCHEDULE_MAP[$SCHEDULE_NAME]}"
    
    if [ -z "$SCHEDULE_ID" ]; then
        echo "⚠️  [$((i+1))/10] $SCHEDULE_NAME - No matching ID found (SKIPPED)"
        ((FAILED_COUNT++))
        continue
    fi
    
    echo -n "⏳ [$((i+1))/10] $SCHEDULE_NAME ... "
    
    # Execute update
    UPDATE_RESULT=$(sf data update record MSD_SyncScheduler__c "$SCHEDULE_ID" \
        --values "MSD_SourceDefinition__c='$SOURCE_DEF' MSD_MappingDefinition__c='$MAPPING_DEF' MSD_UPSERT_Key__c='$UPSERT_KEY'" \
        --target-org "$TARGET_ORG" 2>&1)
    
    if [ $? -eq 0 ]; then
        echo "✅ Updated"
        ((SUCCESS_COUNT++))
    else
        echo "❌ Failed"
        echo "   Error: $UPDATE_RESULT"
        ((FAILED_COUNT++))
    fi
    
    ((UPDATE_COUNT++))
done

echo ""
echo "════════════════════════════════════════════════════════════"
echo "📊 Update Summary:"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "   Total Processed:  $UPDATE_COUNT"
echo "   ✅ Successful:    $SUCCESS_COUNT"
echo "   ❌ Failed:        $FAILED_COUNT"
echo ""

if [ "$FAILED_COUNT" -eq 0 ]; then
    echo "🎉 All schedules updated successfully!"
    echo ""
    echo "📌 Next steps:"
    echo "   1. Verify updates in Salesforce web UI"
    echo "   2. Test with Active__c=false first"
    echo "   3. Enable schedules gradually"
    echo ""
else
    echo "⚠️  Some updates failed. Please review errors above."
    echo ""
fi

echo "════════════════════════════════════════════════════════════"
