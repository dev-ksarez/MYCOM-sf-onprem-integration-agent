# Release 0.2.61

## Highlights

- Bundles production `node_modules` in the customer installer package so runtime modules such as `dotenv/config` are available immediately on customer systems.
- Adds `package-lock.json` to the Node-based customer package builder.
- Updates the Windows updater copy plan to preserve and deploy `package-lock.json`.
- Corrects release installation instructions for the bundled customer installer flow.

## Validation

- `npm run build`
- `node scripts/windows/build-customer-package.js --output-dir artifacts/test-release --include-node-modules`
- Verified the package contains `package-lock.json` and `node_modules/dotenv/config.js`
