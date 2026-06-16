# Release 0.2.77

Annaburger SAGE100 export release for Salesforce-to-MSSQL staging, Sage writeback, and operational visibility.

## Added

- Added Annaburger MSSQL staging table script for Account, Contact, Opportunity, Quote, and QuoteLineItem export into the `Salesforce` database under schema `sf`.
- Added Salesforce metadata for SAGE100 writeback fields on Account, Contact, Opportunity, Quote, and QuoteLineItem.
- Added `SAGE100_Integration_Agent` permission set for the new SAGE100 and POST status fields.
- Added a reproducible Salesforce PROD configuration script for the SAGE100 export connector and related schedulers.
- Added Annaburger process documentation with Mermaid database and process diagrams.

## Changed

- MSSQL target schedulers can now override target schema and table via target definition while keeping the connector credentials centralized.
- MSSQL staging upserts now automatically set `PostStatus` to `NEW` or `UPDATED` and `PostFlag` to `1` for `*_Staging` tables when mappings do not provide those values.
- MSSQL source schedulers now support `afterExport` updates, enabling successful Salesforce writebacks to mark MSSQL rows as `SF_SYNCED`.
- The dashboard connection graph now renders dense connector/scheduler layouts in a scrollable graph area instead of shrinking large diagrams into unreadable thumbnails.

## Fixed

- Improved Global Picklist scheduler target definition handling and validation.
- Kept the Annaburger export connector visible in the Admin UI by using the existing `MS SQL` target system value.
