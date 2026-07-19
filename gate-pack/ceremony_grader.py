#!/usr/bin/env python3
"""ceremony_grader — grade `done` claims against git bedrock: value-backed vs ceremony-only.

The core idea (ceremony-vs-value): a unit of work is *done* only if a commit that touched the
REAL value-surface backs it. A unit marked done with nothing behind it but ceremony commits
(gate-passes, notes, screenshots, status flips) is ceremony-only -- it LOOKS like progress and
quietly manufactures the appearance of it. This tool does NOT flip done/not-done (no false-veto
blast radius); it only SURFACES ceremony-only done so it cannot hide. `done` and the value-surface
share one bedrock definition.

Two layers:
  - grade:  for every unit whose status is `done`, is it backed by a value-surface commit whose
            subject token-matches the unit name? -> {id: value-backed | ceremony-only}.
  - gate:   a ratchet -- a NEWLY done unit that is ceremony-only is a violation; existing
            ceremony-done is grandfathered (stop the disease spreading, don't blast the legacy).
            Warn-only by default.

Deterministic (reads git + a units JSON, no LLM) so it replays to a repeatable result.

Product-agnostic. The value-surface and the units file are configured, not hardcoded -- see
config.example.json and the README. A "unit" is any `{"id","name","status"}` object (your plan /
DAG / task list); only those three fields are read for grading.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

DEFAULT_VALUE_SURFACE: tuple[str, ...] = ("src/", "tests/", "frontend/src/", "backend/", "lib/")
DEFAULT_UNITS = "units.json"
DEFAULT_THRESHOLD = 0.6
DEFAULT_SCAN_DEPTH = 500
CONFIG_BASENAME = ".ceremony_grader.json"


# ---- config (decoupling: product paths -> config) ---------------------------------------

def load_config(root: Path, explicit: str | None = None) -> dict[str, Any]:
    """Resolve config: --config > $CEREMONY_GRADER_CONFIG > <root>/.ceremony_grader.json >
    built-in defaults. Unknown keys are ignored, so a single shared JSON carrying `value_surface`
    can be pointed at both this tool and divergence_probe."""
    path = explicit or os.environ.get("CEREMONY_GRADER_CONFIG")
    cfg: dict[str, Any] = {}
    candidate = Path(path) if path else (root / CONFIG_BASENAME)
    if candidate.exists():
        cfg = json.loads(candidate.read_text(encoding="utf-8"))
    return {
        "value_surface": tuple(cfg.get("value_surface") or DEFAULT_VALUE_SURFACE),
        "units": cfg.get("units") or DEFAULT_UNITS,
        "match_threshold": float(cfg.get("match_threshold") or DEFAULT_THRESHOLD),
        "scan_depth": int(cfg.get("scan_depth") or DEFAULT_SCAN_DEPTH),
    }


def is_value(path: str, value_surface: tuple[str, ...]) -> bool:
    """Value-surface detection (decoupled). NOTE (W5): path-based and therefore gameable -- a
    trivial edit under a value path reads as value. By design this backs attribution + human
    spot-check, not prevention; the signal is soft."""
    return any(path.startswith(g) or g in path for g in value_surface)


# ---- units (generic plan / DAG / task list) ----------------------------------------------

def load_units(text: str) -> dict[str, dict[str, Any]]:
    """A units file is JSON {"units": [...]} (or {"blocks": [...]} for DAG-shaped plans). Each
    unit needs `id`, `name`, `status`; extra fields are ignored. Returns {id: unit}."""
    data = json.loads(text) if text.strip() else {}
    items = data.get("units") or data.get("blocks") or []
    return {u["id"]: u for u in items}


# ---- bedrock git -------------------------------------------------------------------------

def _tokens(s: str) -> set[str]:
    return set(t for t in re.split(r"[^a-z0-9]+", s.lower()) if t)


def _value_surface_subjects(root: Path, value_surface: tuple[str, ...], n: int) -> list[set[str]]:
    """Tokenised subjects of commits that touched the REAL value-surface -- the same bedrock the
    grade is measured against, so `done` and value share one definition."""
    r = subprocess.run(["git", "-C", str(root), "log", f"-{n}", "--name-only", "--pretty=format:%x01%s"],
                       text=True, capture_output=True, check=False)
    out = []
    for chunk in r.stdout.split("\x01"):
        if not chunk.strip():
            continue
        lines = chunk.splitlines()
        subj, files = lines[0], [f for f in lines[1:] if f.strip()]
        if any(is_value(f, value_surface) for f in files):
            out.append(_tokens(subj))
    return out


# ---- grade / ratchet / gate (the extracted block) ----------------------------------------

def done_grades(units: dict[str, dict[str, Any]], root: Path, value_surface: tuple[str, ...],
                threshold: float = DEFAULT_THRESHOLD, scan_depth: int = DEFAULT_SCAN_DEPTH) -> dict[str, str]:
    """Soft grade of done units against value-surface BEDROCK: is each backed by a commit that
    actually touched product code, or only by ceremony? Does NOT change done/not-done -- only
    surfaces ceremony-only done. Returns {id: 'value-backed'|'ceremony-only'} for status==done.
    A unit is value-backed iff some value-surface commit subject overlaps the unit name by
    >= threshold of the name's tokens."""
    vsubs = _value_surface_subjects(Path(root), value_surface, scan_depth)
    grades = {}
    for uid, u in units.items():
        if u.get("status") != "done":
            continue
        ntok = _tokens(u.get("name", ""))
        backed = bool(ntok) and any(len(ntok & st) / len(ntok) >= threshold for st in vsubs)
        grades[uid] = "value-backed" if backed else "ceremony-only"
    return grades


def ceremony_done_violations(base_done: set[str], head_done: set[str], ceremony_only: set[str]) -> list[str]:
    """Pure ratchet logic (ALT/HALT-testable): a violation is a unit NEWLY transitioned to done
    that is ceremony-only. Grandfathers existing ceremony-done (in base_done) -- the gate stops
    the disease SPREADING, not the legacy. Returns sorted violation ids."""
    newly_done = set(head_done) - set(base_done)
    return sorted(newly_done & set(ceremony_only))


def ceremony_done_gate(root: Path, cfg: dict[str, Any], base_ref: str = "HEAD~1") -> dict[str, Any]:
    """Ratchet gate: a newly-washed ceremony-done is a unit that became done in this change with
    no value-surface backing. WARN-ONLY by default (reports, never blocks) -- flip enforcement in
    your CI once you've watched the grade trend for false positives. Reads base vs head units
    from git (bedrock)."""
    root = Path(root)
    units_rel = cfg["units"]
    try:
        head_units = load_units((root / units_rel).read_text(encoding="utf-8"))
        base_raw = subprocess.run(["git", "-C", str(root), "show", f"{base_ref}:{units_rel}"],
                                  text=True, capture_output=True, check=False).stdout
        base_units = load_units(base_raw)
    except OSError:
        return {"violations": [], "newly_done": [], "enforced": False, "note": "no base/head units to compare"}
    head_done = {uid for uid, u in head_units.items() if u.get("status") == "done"}
    base_done = {uid for uid, u in base_units.items() if u.get("status") == "done"}
    ceremony = {uid for uid, v in done_grades(head_units, root, cfg["value_surface"],
                                              cfg["match_threshold"], cfg["scan_depth"]).items()
                if v == "ceremony-only"}
    viol = ceremony_done_violations(base_done, head_done, ceremony)
    return {"violations": viol, "newly_done": sorted(head_done - base_done), "enforced": False,
            "note": "ratchet (warn-only): newly-washed ceremony-done; grandfathers legacy"}


def main() -> int:
    ap = argparse.ArgumentParser(description="Grade done claims: value-backed vs ceremony-only")
    ap.add_argument("--root", default=".")
    ap.add_argument("--config", default=None, help="path to config JSON (else auto-discover)")
    ap.add_argument("--base-ref", default="HEAD~1", help="ratchet base for `gate`")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("grade")
    sub.add_parser("gate")
    a = ap.parse_args()
    root = Path(a.root).resolve()
    cfg = load_config(root, a.config)
    if a.cmd == "grade":
        units = load_units((root / cfg["units"]).read_text(encoding="utf-8"))
        grades = done_grades(units, root, cfg["value_surface"], cfg["match_threshold"], cfg["scan_depth"])
        ceremony = sorted(uid for uid, v in grades.items() if v == "ceremony-only")
        out = {"check_id": "ceremony-grader", "grades": grades,
               "value_backed": sorted(uid for uid, v in grades.items() if v == "value-backed"),
               "ceremony_only": ceremony,
               "note": "ceremony-only = done with no value-surface commit behind it (surfaced, not vetoed)"}
    else:
        out = ceremony_done_gate(root, cfg, a.base_ref)
        out["check_id"] = "ceremony-grader-gate"
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
