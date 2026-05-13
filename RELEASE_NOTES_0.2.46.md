# Release Notes 0.2.46

**Release Date**: 13. Mai 2026  
**Release Type**: Hotfix / Patch  
**Priority**: CRITICAL for Annaburger production

## Overview

Hotfix release for manual scheduler runs in production where clicking `RUN` appeared to do nothing.

## Issues Fixed

### Critical Bug: MSSQL source schedules required target parameters at connector construction

**Symptom**:
- Clicking `RUN` in Scheduler UI did not start a visible run
- No run logs were created for affected schedules
- Schedules were effectively blocked before execution

**Root Cause**:
- `MssqlConnector` validated `table`/`upsertKey` too early in constructor
- This affected source-only `MSSQL_SQL` schedules, where those parameters are not needed for source querying
- As a result, execution could fail before run/log creation path

**Fix**:
- `MssqlConnector` now supports source-only usage without requiring `table`/`upsertKey` at construction time
- Validation for `table`/`upsertKey` is deferred to upsert/target paths where it is actually required
- Repository initialization is now conditional and safe for source-only MSSQL flows

### UX Improvement: RUN errors are now visible in Scheduler UI

**Fix**:
- Added explicit error handling around manual `RUN` action in scheduler UI scripts
- Users now get a clear error toast instead of silent failure behavior

## Technical Details

### File Changes

- `src/connectors/mssql/mssql-connector.ts`
  - Made repository/upsert configuration lazy and mode-specific
  - Added guard methods to enforce required settings only in upsert paths
- `src/server/admin-ui-script.ts`
  - Added error handling and user feedback for `RUN` action
- `src/server/scheduler-ui-module.ts`
  - Added error handling and user feedback for `RUN` action

## Deployment Instructions

### For Windows Customers

```powershell
$AppRoot = "C:\apps\sf-onprem-integration-agent"
$ReleaseVersion = "0.2.46"

cd $AppRoot
npm run win:update-existing -- -AppRoot "$AppRoot" -ReleaseVersion "$ReleaseVersion"
```

## Testing & Validation

- Build passes: `npm run build`
- Specs validate: `npm run spec:validate`

## Notes

- This release preserves prior MSSQL backward compatibility for `schemaName`/`tableName` aliases
- Recommended for environments using `MSSQL_SQL` source schedules
