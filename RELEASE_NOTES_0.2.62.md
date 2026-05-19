# Release 0.2.62

## Highlights

- Removes bundled `node_modules` from the Windows release package again to avoid Windows ZIP extraction failures caused by long dependency paths.
- Keeps `package-lock.json` in the customer package so production dependencies can be restored with `npm ci --omit=dev`.
- Keeps the PricebookEntry `ProductCode` activation fix from `0.2.60`.

## Validation

- `npm run build`
- `node scripts/windows/build-customer-package.js --output-dir artifacts/test-release`
- Verified the package contains `package-lock.json` and does not contain `node_modules/dotenv/config.js`
