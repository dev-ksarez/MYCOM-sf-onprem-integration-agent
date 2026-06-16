# Release 0.2.76

- Target object selection in Scheduler is now restricted to only list Global Picklists (Global Value Sets) when the target type of the scheduler is configured as `SALESFORCE_GLOBAL_PICKLIST`.
- Standard and Custom Salesforce objects continue to be listed for the default `SALESFORCE` target type.
- Scheduler duplication now works with the Salesforce auto-number schedule name field by no longer writing the read-only `Name` field during clone creation.
- Salesforce Insert schedulers can now optionally clear the selected target object immediately before inserting new records.
