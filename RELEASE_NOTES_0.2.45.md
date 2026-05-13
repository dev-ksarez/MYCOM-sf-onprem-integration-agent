# Release Notes 0.2.45

**Release Date**: 13. Mai 2026  
**Release Type**: Hotfix / Patch  
**Priority**: CRITICAL for Annaburger production

## Overview

Hotfix release that completes backward compatibility for MSSQL connector parameters. Resolves production incident where scheduler execution fails with "Missing required MSSQL connector parameter: table" error.

## Issues Fixed

### Critical Bug: MSSQL table Parameter Alias Missing

**Symptom**: Schedule execution fails with:
```
Missing required MSSQL connector parameter: table
```

**Root Cause**: 
- Release 0.2.44 added backward compatibility for `schema` → `schemaName` fallback, but did **not** add the corresponding fallback for `table` → `tableName`.
- Older connector configurations store parameters under keys `schemaName` and `tableName`, but the code now expects `schema` and `table`.
- Only `schema` aliasing was implemented; `table` aliasing was omitted.

**Fix**:
- Updated `MssqlConnector` constructor to use `getStringWithAliases()` for both parameters:
  - `schema` parameter tries both `schema` and `schemaName` keys, defaults to `dbo` if missing
  - `table` parameter tries both `table` and `tableName` keys, required if both missing
- Both parameters now support legacy connector configurations seamlessly

**Affected Schedules (Annaburger Production)**:
- SCH-0020
- SCH-0021  
- SCH-0022

## Technical Details

### File Changes
- **src/connectors/mssql/mssql-connector.ts**
  - Constructor at line ~162 now uses `getStringWithAliases()` for `table` parameter
  - Supports both new (`table`) and legacy (`tableName`) parameter keys

### Backward Compatibility
- ✅ Fully backward compatible with older connector configurations
- ✅ No configuration changes required on customer side
- ✅ Legacy `tableName` and `schemaName` keys continue to work
- ✅ Default fallback to `dbo` for missing schema parameter

## Deployment Instructions

### For Windows Customers

**Recommended**: Deploy via Windows Update Mechanism

```powershell
$AppRoot = "C:\apps\sf-onprem-integration-agent"
$ReleaseVersion = "0.2.45"

cd $AppRoot
npm run win:update-existing -- -AppRoot "$AppRoot" -ReleaseVersion "$ReleaseVersion"
```

**Verification**:
```powershell
Get-Service "SfOnpremIntegrationAgent"
# Expected: Running status
```

### Rollback (if needed)

If issues occur, rollback to 0.2.44:

```powershell
$AppRoot = "C:\apps\sf-onprem-integration-agent"
npm run win:update-existing -- -AppRoot "$AppRoot" -ReleaseVersion "0.2.44"
```

## Testing & Validation

### Smoke Test
- ✅ Build passes: `npm run build`
- ✅ Spec validation passes: `npm run spec:validate`

### Production Verification
After deployment, confirm:
1. Service starts without errors: `Get-Service SfOnpremIntegrationAgent`
2. Schedule execution succeeds (check logs for no "Missing required MSSQL connector parameter" errors)
3. All affected schedules (SCH-0020, SCH-0021, SCH-0022) execute successfully

## Known Issues

None.

## Next Steps

- Monitor Annaburger production logs for any `MSSQL_CONNECTION_FAILED` or parameter-related errors
- If all schedules execute successfully, consider version 0.2.45 as stable

## Commits Included

- feat(projects): add project management UI and production safeguards
- chore(release): bump version to 0.2.45

---

**Questions?** See [WINDOWS_DEPLOYMENT.md](WINDOWS_DEPLOYMENT.md) Incident-Runbook section or contact the development team.
