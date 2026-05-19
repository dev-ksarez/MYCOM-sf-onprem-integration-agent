# Release 0.2.59

## Highlights

- Hardens local admin authentication with hashed passwords and safer production bootstrap behavior.
- Reduces Docker secret exposure for separated host deployments.
- Adds JSON body limits, safer Agent API token handling and invalid-token rate limiting.
- Constrains file connector paths to configured roots and limits parsed file sizes.
- Replaces the vulnerable `xlsx` runtime parser with `exceljs`.
- Streams Salesforce source records through the transfer job to reduce memory pressure.
- Batches schedule checkpoint and stale-run log lookups to reduce Salesforce API load.
- Adds server-side fetch timeouts for Salesforce, admin OIDC, remote agent and update manifest calls.
- Clears production dependency audit findings.
- Adds `docs/source-code-guide.md` as a developer onboarding and module guide.

## Validation

- `npm run build`
- `npm audit --omit=dev`
