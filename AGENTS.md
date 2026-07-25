# AGENTS.md

## Commands
- Test: `npm test` (gate-pack verify.sh; every gate must be green)
- Lint: `npm run lint`

## What this is
LH is a deterministic goal-loop engine: durable SQLite goal/run store, serial
single-holder worker, disposable-clone executors, committed canaries as the
acceptance authority, and goal-scoped execution authority.

## Conventions
- This repo has no docs layer; the three-layer docs convention does not apply.
- Boundary-seal vocabulary: non-test files must not introduce the sealed gate
  vocabulary (the pattern list lives in `gate-pack/boundary_seal/canary.py`
  PATTERNS); any new site outside the sealed baseline turns that gate red.
- Commit new files before running `npm test` (boundary-seal scans tracked files
  only).
- The human/project owns the Goal, authority envelope, stop conditions, terminal
  acceptance, public release, and publication.
- Inside an approved Project Runtime Contract, the loop may commit, push an
  `lh/*` branch, or conditionally merge only when that exact action is granted
  and its committed gate passes. Per-node reminders are not required.
