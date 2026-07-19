#!/usr/bin/env python3
"""Boundary seal drift canary (S2 + S3).

Maintainer-side, deterministic, no provider. It answers one question a non-coder
can read: has any human-summoning / fail-closed-to-human / non-promotable route
appeared that is NOT in the sealed baseline?

- S1: enum.json is the maintainer-owned closed boundary (B1/B2/B3).
- S3: the canary reseals a digest of enum.json's boundaries; if enum.json changes
      without a reseal, the seal is BROKEN.
- S2: baseline.json freezes the set of known gate sites. Any NEW site outside the
      baseline turns the canary RED and is listed by file + line text.

Drift = a site added by an agent that the maintainer never sealed. Removing a gate
is not drift (R1/R3/R4 work removes gates); it is reported as info only.

Usage:
  python3 tools/boundary_seal/canary.py            # check; exit 1 if RED
  python3 tools/boundary_seal/canary.py --json      # machine output
  python3 tools/boundary_seal/canary.py --reseal    # maintainer: refreeze baseline + enum digest
"""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

SEAL_DIR = Path(__file__).resolve().parent
REPO_ROOT = SEAL_DIR.parent.parent
ENUM_PATH = SEAL_DIR / "enum.json"
BASELINE_PATH = SEAL_DIR / "baseline.json"

# Fixed pattern set (maintainer-owned). Same vocabulary as the read-only audit.
PATTERNS = [
    "require_maintainer", "interrupt_human", "human_interrupt", "max_human_interrupts",
    "pause_scope_drift", "waiting_human", "waiting_falsifier",
    "human_completion_plan_approval_required", "human_design_decision_required",
    "product_acceptance_required", "unregistered_fail_closed", r"fail[-_ ]?closed",
    r"hard[-_ ]?stop", "unknown_stopped", "mutation_performed", "promotion_eligible",
    r"non[-_ ]?promotable", "never shrink", "permanent boundary",
]
_RX = re.compile("|".join(f"(?:{p})" for p in PATTERNS), re.IGNORECASE)

# The seal tracks authoring routes in product/runtime code, not test fixtures.
# Excluded: the seal tool itself (it quotes every pattern by design),
# and test/canary files (they carry the vocabulary as fixtures, not as live routes).
# Documented known gap: a gate hidden ONLY in these files is invisible to this canary.
EXCLUDE_PREFIXES = ("gate-pack/boundary_seal/",)


def _excluded(rel: str) -> bool:
    if rel.startswith(EXCLUDE_PREFIXES):
        return True
    name = rel.rsplit("/", 1)[-1]
    if name == "canary.py" or name.startswith("test_") or name.endswith("_test.py"):
        return True
    return rel.startswith("tests/") or "/tests/" in rel


def _tracked_files() -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "ls-files"],
        capture_output=True, text=True, check=True,
    ).stdout.splitlines()
    return [f for f in out if not _excluded(f)]


def _scan() -> set[str]:
    """Return the set of gate sites as 'relpath\\tstripped_line' (line-number stable)."""
    sites: set[str] = set()
    for rel in _tracked_files():
        path = REPO_ROOT / rel
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue  # binary or unreadable; not a route surface
        for line in text.splitlines():
            if _RX.search(line):
                sites.add(f"{rel}\t{line.strip()}")
    return sites


def _enum_digest() -> str:
    boundaries = json.loads(ENUM_PATH.read_text(encoding="utf-8"))["boundaries"]
    canonical = json.dumps(boundaries, sort_keys=True, ensure_ascii=False)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def reseal() -> None:
    sites = sorted(_scan())
    BASELINE_PATH.write_text(
        json.dumps(
            {"sealed_enum_digest": _enum_digest(), "sites": sites},
            indent=2, ensure_ascii=False,
        ) + "\n",
        encoding="utf-8",
    )
    print(f"boundary-seal: RESEALED {len(sites)} sites; enum digest {_enum_digest()}")


KNOWN_GAPS = [
    {"id": "halt-excluded-instrument-and-test-files",
     "detail": "gates hidden ONLY in boundary_seal/ or test/canary files are not scanned"},
]


def check() -> dict:
    """Return the standard reliability-canary payload plus drift detail for humans."""
    has_baseline = BASELINE_PATH.exists()
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8")) if has_baseline else {}
    base = set(baseline.get("sites", []))
    current = _scan()
    new = sorted(current - base) if has_baseline else []
    removed = sorted(base - current)
    enum_ok = has_baseline and _enum_digest() == baseline.get("sealed_enum_digest")

    canaries = [
        {"id": "baseline-present", "ok": has_baseline,
         "detail": "" if has_baseline else "no baseline.json; run --reseal first"},
        {"id": "enum-seal-intact", "ok": bool(enum_ok),
         "detail": "" if enum_ok else "enum.json digest does not match the sealed baseline"},
        {"id": "no-boundary-drift", "ok": not new,
         "detail": "" if not new else f"{len(new)} human-summoning site(s) not in the seal"},
    ]
    blocking = [{"id": c["id"], "detail": c["detail"]} for c in canaries if not c["ok"]]
    return {
        "check_id": "boundary-seal", "status": "pass" if not blocking else "fail",
        "total": len(canaries), "blocking_failures": blocking,
        "known_gaps_open": KNOWN_GAPS,
        "enum_seal": "intact" if enum_ok else ("unsealed" if not has_baseline else "BROKEN"),
        "new": new, "removed": removed,
        "baseline_sites": len(base), "current_sites": len(current),
    }


def _print_human(r: dict) -> None:
    print(f"boundary-seal: {r['status'].upper()}")
    print(f"  enum_seal: {r['enum_seal']}")
    print(f"  sites: baseline={r['baseline_sites']} current={r['current_sites']}"
          f" new={len(r['new'])} removed={len(r['removed'])}")
    if r["new"]:
        print("  DRIFT — new human-summoning sites not in the seal:")
        for s in r["new"]:
            f, _, line = s.partition("\t")
            print(f"    + {f}: {line}")
    if r["removed"]:
        print(f"  info — {len(r['removed'])} sealed sites removed (gate removal is not drift)")


def main(argv: list[str]) -> int:
    if "--reseal" in argv:
        reseal()
        return 0
    r = check()
    if "--json" in argv:
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        _print_human(r)
    return 0 if r["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
