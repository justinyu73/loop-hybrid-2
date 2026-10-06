#!/usr/bin/env python3
"""Canary inventory: no exam may sit outside the gate list where nobody runs it.

Every ``lh_runtime/*_canary.py`` must be run by ``gate-pack/verify.sh``, or be
listed in ``EXEMPT`` with the reason it is not a gate of its own.  An exam that
nobody runs rots quietly: when this check was written, six canaries were outside
the gate list and two of them had been broken for a long time without anyone
noticing.  Exemptions must name files that exist, so the list cannot go stale.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
CHECK_ID = "lh-canary-inventory"
# file name -> why it is not a gate of its own.  Keep this list short and justified.
EXEMPT: dict[str, str] = {}


def case(case_id: str, ok: bool, detail: Any) -> dict[str, Any]:
    return {"id": case_id, "ok": bool(ok), "detail": detail}


def gated() -> set[str]:
    text = (ROOT / "gate-pack" / "verify.sh").read_text(encoding="utf-8")
    return set(re.findall(r"lh_runtime/([A-Za-z0-9_]+\.py)", text))


def main() -> int:
    canaries = sorted(path.name for path in HERE.glob("*_canary.py"))
    run = gated()
    ungated = [name for name in canaries if name not in run and name not in EXEMPT]
    stale = [name for name in EXEMPT if name not in canaries]
    gated_twice = [name for name in EXEMPT if name in run]
    unexplained = [name for name, reason in EXEMPT.items() if not isinstance(reason, str) or len(reason.strip()) < 20]
    results = [
        case("every-canary-is-a-gate-or-a-justified-exemption", not ungated, {"ungated": ungated}),
        case("exemptions-name-existing-canaries", not stale, {"stale": stale}),
        case("an-exempt-canary-is-not-also-a-gate", not gated_twice, {"both": gated_twice}),
        case("every-exemption-gives-a-reason", not unexplained, {"unexplained": unexplained}),
    ]
    failures = [row for row in results if not row["ok"]]
    print(json.dumps({"check_id": CHECK_ID, "status": "pass" if not failures else "fail", "total": len(results),
                      "canaries": len(canaries), "gated": len([name for name in canaries if name in run]),
                      "results": results, "blocking_failures": [row["id"] for row in failures]},
                     indent=2, ensure_ascii=False))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
