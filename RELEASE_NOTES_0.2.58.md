# Release 0.2.58

## Highlights

- Adds the SaaS implementation specification package under `docs/specs/2026-05-15-saas-implementierung`.
- Completes project-layer setup/version UI flows and project deployment summary support.
- Prevents stale local agent health snapshots from keeping the standalone web dashboard in a degraded/error state.
- Improves scheduler health diagnostics by preserving low-level runtime error causes and surfacing the latest scheduler error in the dashboard.

## Validation

- `npm run build`
