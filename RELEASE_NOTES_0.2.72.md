# Release 0.2.72

Geräteakte and admin UI asset release focused on local file access, Salesforce index publishing, and deployable static assets.

## Highlights

- Added Agent API endpoints for serial-number based Geräteakte file listing, protected downloads, inline previews, and generated thumbnails.
- Added optional HTTPS listener support for Agent API file and thumbnail access from Salesforce pages.
- Added periodic Geräteakte file-index publishing to Salesforce so metadata can be pushed from the agent without inbound Salesforce callouts.
- Added generic Salesforce record deletion support for index pruning.
- Split the large admin UI script into source modules and build it into `dist/public/admin-ui.js`.
- Added `sharp` for thumbnail generation.

## Details

- Salesforce Org customization metadata has been removed from this project scope because it now lives in a separate project.
- Geräteakte runtime configuration is controlled through `FILE_BROWSE_BASE_PATH`, `FILE_INDEX_PUSH_ENABLED`, `FILE_INDEX_PUSH_INTERVAL_MS`, `FILE_DOWNLOAD_SIGNING_SECRET`, and optional Agent API TLS settings.
- The release build now runs TypeScript compilation and then generates the admin UI asset bundle.

## Validation

- `npm run build`
