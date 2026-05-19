# Release 0.2.66

## Highlights

- Improves the Scheduler assistant for Salesforce custom field creation with inline configuration for API name, data type, length, precision, scale and picklist values.
- Adds scheduler run progress and source record totals to monitoring where they can be determined without extra Salesforce reads.
- Reworks the dashboard connection overview cards with clearer direction, status, runtime, record and next-run indicators.
- Adds reverse-direction Scheduler duplication support and a Sage 100 MSSQL sample database script.
- Improves mapping usability with Salesforce metadata refresh, filtered target/lookup selection and picklist value prefill.

## Validation

- `npm run build`
- `git diff --check`
- rendered `admin-ui-script` syntax check
