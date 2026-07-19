# AGENTS.md

## Commands
- Test: `npm test` (gate-pack verify.sh; every gate must be green)
- Lint: `npm run lint`

## What this is
LH is a deterministic goal-loop engine: durable SQLite goal/run store, serial
single-holder worker, disposable-clone executors, committed canaries as the
acceptance authority, promotion always human-owned.

## Conventions
- This repo has no docs layer; the three-layer docs convention does not apply.
- Boundary-seal vocabulary: non-test files must not introduce the sealed gate
  vocabulary (the pattern list lives in `gate-pack/boundary_seal/canary.py`
  PATTERNS); any new site outside the sealed baseline turns that gate red.
- Commit new files before running `npm test` (boundary-seal scans tracked files
  only).
- git commit/push only on explicit user request; merge and promotion are always
  human-owned.
