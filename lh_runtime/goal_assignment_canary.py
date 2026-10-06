#!/usr/bin/env python3
"""Canary for LH-owned project and criterion assignment projections."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import goal_assignment
from goal_store import GoalStore


PROJECT = "example-target"


def assignment(*, check_digest: str = "sha256:" + "b" * 64) -> dict:
    return {
        "schema": goal_assignment.ASSIGNMENT_SCHEMA,
        "project_id": PROJECT,
        "assigner_ref": "operator-goal:continuity-repair",
        "base_revision": "1" * 40,
        "verification_budget": {
            "schema": goal_assignment.VERIFICATION_BUDGET_SCHEMA,
            "max_seconds": 30.0,
        },
        "criteria": [{
            "criterion_id": "AC-C7",
            "criterion_authority_ref": (
                "docs/bootstrap-authority.md"
                "#host-check-ownership-014"
            ),
            "criterion_authority_digest": "sha256:" + "a" * 64,
            "check_id": "host-decision-seal-verify",
            "check_definition_digest": check_digest,
        }],
    }


def add_goal(store: GoalStore, suffix: str, payload: dict, *, active: bool = True) -> dict:
    event_key = f"event-{suffix}"
    store.record_event(
        event_id=event_key,
        idempotency_key=event_key,
        source="manual_intent",
        event_type="goal_candidate",
        payload={"suffix": suffix},
    )
    created = store.create_candidate(
        event_key,
        goal_id=f"goal-{suffix}",
        campaign_id="campaign-assignment",
        stage_id=f"stage-{suffix}",
        goal=payload,
    )
    if active:
        store.transition_goal(
            f"goal-{suffix}",
            "active",
            expected_state="candidate",
            event_key=event_key,
        )
    return created


def main() -> int:
    cases: list[dict[str, object]] = []

    def record(case_id: str, ok: bool, detail: object) -> None:
        cases.append({"id": case_id, "ok": bool(ok), "detail": str(detail)})

    with tempfile.TemporaryDirectory(prefix="lh-goal-assignment-") as raw:
        store = GoalStore(Path(raw) / "goals")
        created = add_goal(
            store,
            "one",
            {
                "feature_contract": "repair continuity advancement",
                "admission_envelope": {"goal_assignment": assignment()},
            },
        )
        resolved = store.resolve_project_assignment(PROJECT)
        record(
            "active-project-resolves-to-exact-goal-revision",
            resolved.get("status") == "admitted"
            and resolved.get("goal_id") == "goal-one"
            and resolved.get("revision_id") == created.get("revision_id")
            and resolved.get("goal_digest") == created.get("goal_digest"),
            resolved,
        )
        record(
            "criterion-to-check-binding-is-digest-bearing",
            resolved.get("assignment", {}).get("criteria", [{}])[0]
            == assignment()["criteria"][0]
            and str(resolved.get("assignment_digest", "")).startswith("sha256:"),
            resolved.get("assignment"),
        )
        record(
            "verification-budget-is-frozen-into-the-assignment-digest",
            resolved.get("assignment", {}).get("verification_budget")
            == assignment()["verification_budget"]
            and resolved.get("assignment_digest")
            == goal_assignment._digest(resolved.get("assignment")),
            resolved.get("assignment"),
        )

        absent = store.resolve_project_assignment("another-project")
        record(
            "missing-project-assignment-is-unavailable",
            absent.get("status") == "unavailable",
            absent,
        )

        malformed_rejected = False
        try:
            add_goal(
                store,
                "malformed",
                {
                    "goal_assignment": {
                        **assignment(),
                        "criteria": [
                            assignment()["criteria"][0],
                            assignment()["criteria"][0],
                        ],
                    },
                },
                active=False,
            )
        except ValueError:
            malformed_rejected = True
        record(
            "malformed-binding-is-rejected-before-revision-write",
            malformed_rejected and store.summary()["goal_count"] == 1,
            store.summary(),
        )

        invalid_base_rejected = False
        try:
            add_goal(
                store,
                "invalid-base",
                {
                    "goal_assignment": {
                        **assignment(),
                        "base_revision": "HEAD",
                    },
                },
                active=False,
            )
        except ValueError:
            invalid_base_rejected = True
        record(
            "symbolic-base-is-rejected-before-revision-write",
            invalid_base_rejected and store.summary()["goal_count"] == 1,
            store.summary(),
        )

        invalid_digest_rejected = False
        try:
            add_goal(
                store,
                "invalid-digest",
                {
                    "goal_assignment": {
                        **assignment(),
                        "criteria": [{
                            **assignment()["criteria"][0],
                            "check_definition_digest": "sha256:not-a-digest",
                        }],
                    },
                },
                active=False,
            )
        except ValueError:
            invalid_digest_rejected = True
        record(
            "malformed-digest-is-rejected-before-revision-write",
            invalid_digest_rejected and store.summary()["goal_count"] == 1,
            store.summary(),
        )

        missing_budget_rejected = False
        try:
            without_budget = assignment()
            without_budget.pop("verification_budget")
            add_goal(
                store,
                "missing-budget",
                {"goal_assignment": without_budget},
                active=False,
            )
        except ValueError:
            missing_budget_rejected = True
        record(
            "missing-verification-budget-is-rejected-before-revision-write",
            missing_budget_rejected and store.summary()["goal_count"] == 1,
            store.summary(),
        )

        invalid_budget_rejected = []
        for index, invalid in enumerate((True, 10 ** 400)):
            try:
                add_goal(
                    store,
                    f"invalid-budget-{index}",
                    {
                        "goal_assignment": {
                            **assignment(),
                            "verification_budget": {
                                "schema": goal_assignment.VERIFICATION_BUDGET_SCHEMA,
                                "max_seconds": invalid,
                            },
                        },
                    },
                    active=False,
                )
            except ValueError:
                invalid_budget_rejected.append(repr(invalid))
        record(
            "invalid-or-overflowing-verification-budget-is-rejected-before-revision-write",
            len(invalid_budget_rejected) == 2
            and store.summary()["goal_count"] == 1,
            {"rejected": invalid_budget_rejected, "summary": store.summary()},
        )

        add_goal(store, "two", {"goal_assignment": assignment()})
        conflict = store.resolve_project_assignment(PROJECT)
        record(
            "multiple-active-project-goals-are-a-conflict",
            conflict.get("status") == "conflict"
            and conflict.get("goal_ids") == ["goal-one", "goal-two"],
            conflict,
        )

    failures = [
        {"id": item["id"], "detail": item["detail"]}
        for item in cases
        if not item["ok"]
    ]
    result = {
        "check_id": "lh-goal-assignment",
        "status": "pass" if not failures else "fail",
        "total": len(cases),
        "blocking_failures": failures,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
