# AGENTS.md

## Commands
- Test: `npm test` (gate-pack verify.sh; every gate must be green)
- Lint: `npm run lint`

## What this is
LH is a deterministic goal-loop engine: durable SQLite goal/run store, serial
single-holder worker, disposable-clone executors, committed canaries as the
acceptance authority, and goal-scoped execution authority.

## Conventions
- Docs follow three layers: `docs/contracts/` (long-lived, describes the code as
  it is; changing one needs approval and a visible reseal with
  `gate-pack/contract_seal/seal.py reseal`), `docs/active/`, `docs/archive/`.
- Boundary-seal vocabulary: non-test files must not introduce the sealed gate
  vocabulary (the pattern list lives in `gate-pack/boundary_seal/canary.py`
  PATTERNS); any new site outside the sealed baseline turns that gate red.
- Commit new files before running `npm test` (boundary-seal scans tracked files
  only).
- The human/project owns the Goal, authority envelope, stop conditions, terminal
  acceptance, public release, and publication.
- Inside an approved Project Runtime Contract the loop runs, verifies, and
  continues approved work without per-node reminders. Effects outside the
  workspace go only through an injected port; post-run effects go through
  `lh_runtime/effect_guard.py`.
