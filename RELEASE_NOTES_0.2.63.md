# Release 0.2.63

## Highlights

- Supports qualified MSSQL delta fields such as `p.Timestamp` while still reading the checkpoint value from the selected `Timestamp` result column.
- Helps avoid `Ambiguous column name 'Timestamp'` for joined SAGE/MSSQL price queries.

## Validation

- `npm run build`
