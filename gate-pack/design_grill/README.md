# Manual Design Grill

Use this foreground-only, model-neutral challenge framework while discussing a
goal with a human. It freezes bounded repository documents into a caller-owned
external session directory, defines one `subject` and configurable
`review_slots`, issues one host-egress contract for each slot, and records
supplied advisory results there. It never launches a provider.

Loop-hybrid provides the protocol and validator only. It never stores a
caller’s capsule, contracts, receipts, results, or project discussion under the
LH repository. Remove the external session directory when the discussion ends,
unless the caller’s own system has a separate retention policy.

Any human or model may invoke the framework. The invoker, the subject producer,
and a reviewer are separate fields, not fixed roles or provider identities. A
slot may require its binding to differ from the subject or an earlier review
slot; this requests a real cross-model challenge without hard-coding PEVO/O,
Claude, Codex, Gemini, or any other pairing.

1. Prepare a capsule from LH-local documents:

   `python3 gate-pack/design_grill/design_grill.py prepare --spec gate-pack/design_grill/spec.example.json --session-dir /tmp/design-grill-goal-feasibility-example`

   A caller may instead use the same relative-document spec with an explicit,
   one-call context root:

   `python3 gate-pack/design_grill/design_grill.py prepare --spec /tmp/grill/spec.json --context-root /path/to/caller-context --session-dir /tmp/grill/session`

   The context root is read only and is never written into the capsule or
   manifest.  The session must be outside both LH and that root.  Document
   references remain relative, cannot traverse directories, and cannot select
   `.git`, `.env*`, or protected credential paths.

2. For each configured slot, use `request --session-dir <external-dir> --review-id <id>`. Give its returned
   `egress_envelope` to the host egress gateway. The host chooses an
   already-authorized runner/model matching the contract; the provider profile
   records transport/account constraints and does not define a challenge role.

3. After the host records a matching admission plus one process-bound execution
   receipt (exit code and output digest), and the human obtains a bounded result
   artifact, call `record` with the contract, host receipt, and result JSON. All configured slots end at
   `human_design_decision_required`.

The host owns provider execution and output capture. This gate can only
prepare, bind, and verify evidence; it cannot ratify a goal, authorize
implementation, decide which model must challenge which, or authorize a
provider invocation.
