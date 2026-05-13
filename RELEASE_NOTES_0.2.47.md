# Release Notes 0.2.47

**Release Date**: 13. Mai 2026  
**Release Type**: Hotfix / Patch  
**Priority**: CRITICAL for Production Scheduler RUN

## Overview

This hotfix fixes an unintended production-protection regression where manual scheduler runs (`RUN`) were blocked.

## Issues Fixed

### Critical Regression: Manual RUN blocked by production write protection

**Symptom**:
- Clicking `RUN` in Scheduler UI returned a write-protection error
- Error example:
  - `Schreibzugriff blockiert ... (POST /api/schedules/<id>/run)`
- Schedulers could not be started manually in protected production instances

**Root Cause**:
- The instance write-protection filter in `app.ts` treated `POST /api/schedules/:id/run` as a configuration write endpoint.
- Manual run trigger is an execution action, not a configuration mutation.

**Fix**:
- Removed `/api/schedules/:id/run` from `requiresInstanceWriteCheck` route set.
- Configuration write protection remains active for schedule/connector mutations.
- Manual run execution is now allowed again in protected production instances.

## Technical Details

### File Changes

- `src/server/app.ts`
  - Adjusted route classification in instance write-check logic.
  - Excluded manual scheduler run endpoint from blocked write routes.

## Testing & Validation

- Build passes: `npm run build`
- Specs validate: `npm run spec:validate`

## Deployment Instructions (Windows)

```powershell
$AppRoot = "C:\apps\sf-onprem-integration-agent"
$ReleaseVersion = "0.2.47"

cd $AppRoot
npm run win:update-existing -- -AppRoot "$AppRoot" -ReleaseVersion "$ReleaseVersion"
```
