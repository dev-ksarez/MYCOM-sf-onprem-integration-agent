# Release 0.2.74

Geräteakte connector release that moves Annaburger file-browse configuration from environment-only settings into a dedicated `FileBrowse` connector profile.

## Highlights

- Added `FileBrowse` / Geräteakte connector support for the Agent file index publisher.
- The agent now prefers an active `FileBrowse` connector from Salesforce for Geräteakte base path and directory layout.
- Kept `FILE_BROWSE_BASE_PATH` as backward-compatible fallback and `FILE_INDEX_PUSH_ENABLED=0` as explicit kill switch.
- Browse, download, thumbnail, and index paths share the same active FileBrowse configuration.
- Added FileBrowse-specific Connector Assistant UX with only relevant fields:
  - Base Path
  - Directory Layout
  - Test Serial Number
- Added FileBrowse-specific connector test checks for base path access, layout handling, serial folder resolution, and directory discovery.

## Configuration

For Annaburger production create an active connector:

```text
Name: Geräteakte FileBrowse
Connector Type: FileBrowse
Target System: Agent
Direction: Inbound
```

Parameters:

```json
{
  "basePath": "H:\\130_Produktion\\Fertigung\\Geräteakte",
  "directoryLayout": "annaburg-fg-bucket",
  "sampleSerial": "Fg00073",
  "purpose": "geraeteakte"
}
```

## Validation

- `npm run build`
- `npm run win:build-package`
- Local FileBrowse path-resolution smoke test for `Fg0000xx/Fg000073`
