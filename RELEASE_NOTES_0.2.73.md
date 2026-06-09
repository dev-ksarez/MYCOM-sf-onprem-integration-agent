# Release 0.2.73

Geräteakte hotfix release for Annaburger production file layout support.

## Highlights

- Added configurable Geräteakte directory layout support for Annaburger production.
- Supports base paths such as `H:\130_Produktion\Fertigung\Geräteakte` with serial folders below `Fg0000xx/Fg000073`.
- Normalizes Salesforce serial numbers such as `Fg00073` to the six-digit folder name `Fg000073`.
- Uses the same path resolver for index publishing, file listing, downloads, and thumbnails.

## Configuration

For Annaburger production set:

```env
FILE_BROWSE_BASE_PATH=H:\130_Produktion\Fertigung\Geräteakte
GERAETEAKTE_DIRECTORY_LAYOUT=annaburg-fg-bucket
FILE_INDEX_PUSH_ENABLED=1
```

## Validation

- `npm run build`
- Local path-resolution smoke test for `Fg0000xx/Fg000073`
