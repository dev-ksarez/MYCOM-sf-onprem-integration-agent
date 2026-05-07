# Release 0.2.21

## Bugfixes

### Scheduler Modal State Contamination (Critical)
- Fixed race condition in scheduler modal where async metadata loaders (`loadTargetFields`, `loadTargetObjects`) would contaminate UI state when switching between schedulers
- Added sequence guards to prevent stale async responses from overwriting modal state
- Mapping rules are now hydrated immediately after DOM update before async operations complete
- **Impact**: Users can now reliably switch between schedulers without field mappings getting mixed up

### Scheduler Configuration Corruption (Critical)
- Identified and corrected Contact (Ansprechpartner) scheduler: external ID field was misconfigured
  - Now correctly uses `ERP_CONTACT_NUMBER__c` instead of invalid field reference
  - Field mappings cleaned to exclude Account-related contamination
- Identified and corrected Product2 (Produkte) scheduler: external ID field was misconfigured  
  - Now correctly uses `ERP_PRODUCT_CODE__c` with proper Product2 field mappings
  - Removed Account/Contact field contamination
- Synchronized artifact definitions across all migration snapshots to prevent re-import of corrupted configs

### Scheduler Load Optimization
- Reduced scheduler intervals to optimize org storage pressure (at 69%):
  - Contact scheduler: 5 min → 30 min
  - Accounts scheduler: 60 min → 120 min  
  - Product2 scheduler: 5 min → 60 min
  - PricebookEntry scheduler: 5 min → 30 min

## Known Issues & Next Steps

### Schedule Cache Reloading
- Agent runtime caches schedule definitions at startup (`SalesforceScheduleSource.getActiveSchedules()`)
- Manual test runs query fresh Salesforce data and execute successfully
- Scheduled runs may use stale cached configs after configuration changes
- **Workaround**: Restart agent service to reload schedule cache
- **Fix**: Implement schedule cache invalidation mechanism on config changes (pending for 0.2.21)

### Product Scheduler Source Data
- Product scheduler currently returning 0 records in scheduled runs despite correct configuration
- Manual test shows configuration is valid
- **Investigation**: Source query or delta checkpoint may need adjustment (pending diagnostics)

## Operational Notes

### Full Scheduler Audit Completed
Systematic audit of all active schedulers conducted:
- **High Volume**: Prices (100 records/run), Contact (100), Accounts (100)
- **Functional**: Prices ✓, SCH-0000 (Picklist) ✓, Outbound Accounts ✓
- **Pending**: Product source data investigation, remaining legacy schedulers
- **Storage**: Org at 69% - monitor closely; consider disabling low-priority schedulers

### Configuration Validation
- All corrected schedulers pass server-side validation (`validateScheduleConfiguration()`)
- Manual runs with corrected configs: 100% success rate
- External ID fields now consistent across all primary schedulers

## Migration Path

If upgrading from 0.2.19:
1. Deploy new `src/server/app.ts` (UI race condition fix)
2. Update scheduler definitions via API or re-import `artifacts/dev-sandbox-schedule-examples.json`
3. **Critical**: Restart agent service to reload schedule cache from Salesforce
4. Monitor next scheduled Contact/Accounts/Product runs for success

## Files Changed
- `src/server/app.ts`: UI state management, async load sequence guards, mapping hydration
- `artifacts/dev-sandbox-schedule-examples.json`: Corrected Contact & Product2 scheduler definitions
- `artifacts/schedule-timing.json`: Reduced scheduler intervals
- Migration artifacts (3 files): Synchronized with corrected definitions
