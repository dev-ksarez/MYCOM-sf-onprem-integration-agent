# Release 0.2.71

Dashboard and Salesforce instance release focused on multi-instance visibility, compact operational gauges, and connector reliability.

## Highlights

- Reworked the overview Service and Salesforce Org cards with compact Chart.js gauges for CPU, RAM, disk, API usage, storage, API average, and daily data growth.
- Added clickable Salesforce Org URL, Sandbox/Production environment display, and inline calculation details for gauge values.
- Added agent version reporting to the health snapshot and dashboard Service card.
- Moved record and error charts directly below the linkage overview for faster dashboard scanning.
- Added daily record totals to Scheduler statistics and changed data growth to use today's successful records compared with yesterday.
- Fixed connector cache invalidation so newly created MSSQL connectors appear immediately in fresh Salesforce instances.
- Improved Salesforce REST/API error handling with clearer Connected App and session guidance.

## Details

- Data growth now uses `MSD_RecordsSucceeded__c` for today's successful records. The current run model does not distinguish inserts from updates, so successful records are the closest reliable technical measure for "created/generated" data.
- Scheduler statistics now show today's total records as successful plus failed records, with a tooltip split.
- The release keeps dashboard gauge information inline instead of adding separate panels.

## Validation

- `npm run build`
- Rendered `admin-ui.js` parse check with `new Function(renderAdminUiScript())`
