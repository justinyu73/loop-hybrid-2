"""Shared proof vocabulary: how strong a piece of evidence is, and the states a claim can be in.

``PROOF_RANK`` orders evidence from a fixture up to a release; ``STATUSES`` is
the closed set of claim states.  Other checks import both.

The registry checker that used to live here read a private optimization-claims
file that was never part of this repository, so it could never pass here.  It
was retired together with its canary; only the vocabulary remains.
"""
from __future__ import annotations

PROOF_RANK = {
    "fixture": 0,
    "offline_canary": 1,
    "bounded_live": 2,
    "resident_live": 3,
    "human_acceptance": 4,
    "release": 5,
}
STATUSES = {"open", "complete", "blocked", "superseded"}
