# 🚀 Option B: Salesforce CLI Batch Update

**Status**: ✅ Ready for Execution  
**Created**: 12. Mai 2026

---

## 📋 Overview

This option uses **Salesforce CLI** to automatically update all 10 MSD_SyncScheduler__c records with SAGE100-compliant definitions.

---

## 🎯 Quick Start

### Step 1: Prepare the Environment
```bash
# Ensure SF CLI is installed
sf --version

# Verify AnnaburgerPROD1 org is connected
sf org list --all
```

### Step 2: Run the Batch Update Script
```bash
# Execute the automated batch update
./scripts/update-schedules-batch.sh
```

**The script will:**
1. ✅ Query existing schedules from AnnaburgerPROD1
2. ✅ Retrieve all 10 MSD_SyncScheduler__c record IDs
3. ✅ Update each schedule with SAGE100-compliant definitions
4. ✅ Report success/failure status

---

## 📊 What Gets Updated

For each schedule, the following fields are updated:

| Field | Value Source |
|-------|--------------|
| `MSD_SourceDefinition__c` | SAGE100-konforme SQL Query |
| `MSD_MappingDefinition__c` | SAGE100-Spaltennamen-Mappings |
| `MSD_UPSERT_Key__c` | SAGE100 Primärschlüssel |

**Example (Schedule 1: Account)**:
```
Source Query:   SELECT Adresse, Name1, Name2, ... FROM dbo.KHKAdressen WHERE Aktiv = -1
Mappings:       Name;string=Name1;TRIM | AccountNumber;string=Adresse;TRIM | ...
UPSERT Key:     AccountNumber
```

---

## 🔧 Manual CLI Commands (if needed)

### List all schedules
```bash
sf data query \
  --query "SELECT Id, Name FROM MSD_SyncScheduler__c" \
  --json \
  --target-org AnnaburgerPROD1
```

### Update a single schedule
```bash
sf data update record MSD_SyncScheduler__c <RECORD_ID> \
  --values "MSD_SourceDefinition__c='<NEW_QUERY>' MSD_UPSERT_Key__c='<NEW_KEY>'" \
  --target-org AnnaburgerPROD1
```

### Delete and recreate (if needed)
```bash
sf data delete record MSD_SyncScheduler__c <RECORD_ID> --target-org AnnaburgerPROD1
```

---

## ✅ Validation & Next Steps

After running the batch update:

1. **Verify in Salesforce**:
   - Log in to AnnaburgerPROD1
   - Navigate to MSD_SyncScheduler__c
   - Check that Source Query and Mappings are updated

2. **Validate each schedule**:
   - Use the checklist in [SCHEDULE_UPDATE_GUIDE.md](SCHEDULE_UPDATE_GUIDE.md)
   - Verify Source Queries reference correct SAGE100 tables
   - Verify UPSERT Keys are correct

3. **Test with Active__c=false**:
   - Keep schedules inactive initially
   - Test one schedule at a time
   - Monitor logs for errors

4. **Activate Gradually**:
   - Follow dependency order from SCHEDULE_UPDATE_GUIDE.md
   - Enable one schedule at a time
   - Verify data synchronization

---

## 🚨 Troubleshooting

### Error: "sObject type 'MSD_SyncScheduler__c' is not supported"
**Cause**: Custom object doesn't exist in target org  
**Solution**: Create custom object first or use different org

### Error: "Invalid field name" 
**Cause**: Syntax error in field values  
**Solution**: Check escaping of quotes in field values

### Error: "Permission denied"
**Cause**: User doesn't have permissions to update records  
**Solution**: Use admin account or grant update permissions

### Partial Update Failure
**Cause**: Some schedules updated, others failed  
**Solution**: Review error messages and retry failed ones manually

---

## 📁 Files Referenced

- **artifacts/annaburger-prod1-setup-import.json** - Master SAGE100 configuration
- **scripts/update-schedules-batch.sh** - Automated batch update script
- **scripts/update-schedules-cli.js** - Node.js CLI command generator
- **SCHEDULE_UPDATE_GUIDE.md** - Detailed validation guide

---

## 🎓 Example Output

```
════════════════════════════════════════════════════════════
  ✅ SAGE100 Schedule CLI Batch Update
════════════════════════════════════════════════════════════

Target Org: AnnaburgerPROD1

📋 Step 1: Retrieving existing schedules from Salesforce...

✅ Found 10 schedules

a019O00001cX7XQQA0 - ANN SAGE100 KHKAdressen -> Account
a019O00001cXCVSQA4 - ANN SAGE100 KHKAnsprechpartner -> Contact
...

🔄 Updating schedules...

⏳ [1/10] ANN SAGE100 KHKAdressen -> Account ... ✅ Updated
⏳ [2/10] ANN SAGE100 KHKAnsprechpartner -> Contact ... ✅ Updated
...

📊 Update Summary:
   Total Processed:  10
   ✅ Successful:    10
   ❌ Failed:        0

🎉 All schedules updated successfully!
```

---

## 📞 Support

For issues or questions:
1. Check the troubleshooting section above
2. Review SCHEDULE_UPDATE_GUIDE.md for detailed context
3. Consult SAGE100_UPDATE_SUMMARY.txt for comprehensive overview

---

**Status**: ✅ Option B Ready for Execution  
**Next Action**: Run `./scripts/update-schedules-batch.sh`
