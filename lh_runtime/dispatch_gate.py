#!/usr/bin/env python3
"""W5 owner-durability dispatch gate: one seam before every driver dispatch.

The gate reads durable state only and answers allow / note / idle / stop plus
a reason for the status snapshot:

- daily cost ceiling: receipt usage summed over the current UTC calendar day.
  At or above the soft cap the tick idles (nothing new is dispatched); at or
  above the hard cap the driver session stops. An attempt already running is
  never touched — the gate only ever decides before a dispatch.
- quota pressure: an injected reader reports ``used_percent``; the thresholds
  mirror the notify/soft/hard shape of the platform quota policy (gate-pack
  code is NOT imported into the runtime). A configured reader that cannot
  produce a reading stops dispatch — unknown quota is not dispatchable.
- executor credential failure: detected after a tick from the attempt's
  durable provider record, matched conservatively (clear auth markers only).
  The driver then ends the session instead of retrying until attempts run
  out; the next session re-probes, so fixed credentials resume on their own.

Every input is re-read on each evaluation, so a cleared condition (a new UTC
day, recovered quota, fixed credentials) resumes dispatch on the next tick
with no manual reset.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable

import token_cost

ALLOW = "allow"
NOTE = "note"
IDLE = "idle"
STOP = "stop"

QUOTA_NOTIFY_USED_PERCENT = 60.0
QUOTA_SOFT_USED_PERCENT = 80.0
QUOTA_HARD_USED_PERCENT = 100.0

# Documented credential-failure signal: cli_agent_executor wraps a non-zero
# CLI exit as "RuntimeError: <name> exited <code>: <stderr>". A provider
# failure counts as a credential failure only when its message carries one of
# these markers; anything else stays an ordinary retryable failure.
AUTH_FAILURE_MARKERS = ("unauthorized", "authentication", "not logged in", "invalid api key", "401")

# Injected quota source: returns {"used_percent": float} or None when no
# reading is available. Production wires the host's quota probe; canaries
# inject fixtures.
QuotaReader = Callable[[], dict[str, Any] | None]


def is_auth_failure(failure: Any) -> bool:
    """Conservative match: only clear credential/auth messages qualify."""
    if not isinstance(failure, str):
        return False
    text = failure.lower()
    return any(marker in text for marker in AUTH_FAILURE_MARKERS)


def quota_action(used_percent: float) -> tuple[str, str]:
    """Mirror the platform quota policy thresholds (60 notify / 80 soft / 100 hard)."""
    if used_percent >= QUOTA_HARD_USED_PERCENT:
        return STOP, "quota_hard"
    if used_percent >= QUOTA_SOFT_USED_PERCENT:
        return IDLE, "quota_soft"
    if used_percent >= QUOTA_NOTIFY_USED_PERCENT:
        return NOTE, "quota_notify"
    return ALLOW, "quota_normal"


class DispatchGate:
    """Pre-dispatch gate over durable cost/quota state plus post-tick auth detection."""

    def __init__(
        self,
        run_store: Any,
        *,
        quota_reader: QuotaReader | None = None,
        soft_daily_usd: float | None = 2.0,
        hard_daily_usd: float | None = 5.0,
        pricing: dict[str, Any] | None = None,
        now_fn: Callable[[], datetime] | None = None,
    ):
        for name, value in (("soft_daily_usd", soft_daily_usd), ("hard_daily_usd", hard_daily_usd)):
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be positive")
        if soft_daily_usd is not None and hard_daily_usd is not None and soft_daily_usd >= hard_daily_usd:
            raise ValueError("soft_daily_usd must be below hard_daily_usd")
        self.run_store = run_store
        self.quota_reader = quota_reader
        self.soft_daily_usd = soft_daily_usd
        self.hard_daily_usd = hard_daily_usd
        self.pricing = pricing
        self._now_fn = now_fn if now_fn is not None else lambda: datetime.now(timezone.utc)

    def daily_cost(self) -> dict[str, Any]:
        """Sum receipt usage whose attempt started inside the current UTC day."""
        today = self._now_fn().astimezone(timezone.utc).date()
        records = [
            record
            for record in self.run_store.usage_records()
            if record.get("created_at") is not None
            and datetime.fromtimestamp(float(record["created_at"]), tz=timezone.utc).date() == today
        ]
        rolled = token_cost.aggregate(records, pricing=self.pricing)
        return {
            "day": today.isoformat(),
            "estimated_cost_usd": rolled["estimated_cost_usd"],
            "measured_records": rolled["measured_records"],
            "unknown_records": rolled["unknown_records"],
        }

    def _quota_state(self) -> dict[str, Any] | None:
        if self.quota_reader is None:
            return None
        try:
            reading = self.quota_reader()
        except Exception:
            reading = None
        used = reading.get("used_percent") if isinstance(reading, dict) else None
        if not isinstance(used, (int, float)) or isinstance(used, bool) or not 0 <= used <= 100:
            return {"used_percent": None, "action": STOP, "reason_code": "quota_unknown"}
        action, reason_code = quota_action(float(used))
        return {"used_percent": float(used), "action": action, "reason_code": reason_code}

    def evaluate(self) -> dict[str, Any]:
        """allow/note dispatch; idle skips this tick's dispatch; stop ends the session."""
        cost = self.daily_cost()
        quota = self._quota_state()
        action, reason_code = ALLOW, None
        if self.hard_daily_usd is not None and cost["estimated_cost_usd"] >= self.hard_daily_usd:
            action, reason_code = STOP, "daily_cost_hard"
        elif quota is not None and quota["action"] == STOP:
            action, reason_code = STOP, str(quota["reason_code"])
        elif self.soft_daily_usd is not None and cost["estimated_cost_usd"] >= self.soft_daily_usd:
            action, reason_code = IDLE, "daily_cost_soft"
        elif quota is not None and quota["action"] == IDLE:
            action, reason_code = IDLE, str(quota["reason_code"])
        elif quota is not None and quota["action"] == NOTE:
            action, reason_code = NOTE, str(quota["reason_code"])
        return {
            "action": action,
            "reason_code": reason_code,
            "cost": {**cost, "soft_daily_usd": self.soft_daily_usd, "hard_daily_usd": self.hard_daily_usd},
            "quota": quota,
        }

    def auth_observation(self, tick_result: dict[str, Any]) -> dict[str, Any] | None:
        """Detect a credential failure in the attempt the tick just finished.

        Reads the durable provider record of the dispatched run. A match ends
        the session (the operator re-checks credentials on the daily pass)
        instead of retrying until the attempt budget runs out.
        """
        run = tick_result.get("run") if isinstance(tick_result, dict) else None
        if not isinstance(run, dict) or run.get("status") not in {"retry_pending", "stopped"}:
            return None
        run_id = run.get("run_id")
        if not isinstance(run_id, str):
            return None
        meta = self.run_store.latest_receipt(run_id)
        if meta is None:
            return None
        try:
            receipt = json.loads((self.run_store.root / meta["receipt_ref"]).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        provider = receipt.get("provider") if isinstance(receipt, dict) else None
        artifact = provider.get("artifact") if isinstance(provider, dict) else None
        # controller stamps artifact as a {"ref", "digest"} pair; tolerate a plain ref too.
        if isinstance(artifact, dict):
            artifact = artifact.get("ref")
        if not isinstance(artifact, str):
            return None
        try:
            record = json.loads((self.run_store.root / artifact).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        failure = record.get("failure") if isinstance(record, dict) else None
        if not is_auth_failure(failure):
            return None
        return {
            "action": STOP,
            "reason_code": "executor_auth",
            "run_id": run_id,
            "detail": "executor reported a credential failure; dispatch is parked until a later session re-probes",
        }
