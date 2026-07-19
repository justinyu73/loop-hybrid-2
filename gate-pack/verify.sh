#!/usr/bin/env sh
# Derived verdict: exit at the first deterministic gate that fails.
set -u

run_gate() {
  name="$1"
  shift
  printf '[RUN ] %s\n' "$name"
  if "$@"; then
    printf '[PASS] %s\n' "$name"
    return 0
  else
    status=$?
    printf '[FAIL] %s (exit %s)\n' "$name" "$status" >&2
    return "$status"
  fi
}

run_gate ceremony sh gate-pack/run.sh || exit $?
run_gate boundary-seal python3 gate-pack/boundary_seal/canary.py || exit $?
run_gate docs-structure python3 gate-pack/docs_structure/canary.py || exit $?
run_gate quota python3 gate-pack/quota/canary.py || exit $?
run_gate improvement python3 gate-pack/improvement/canary.py || exit $?
run_gate cross-repo-runner python3 gate-pack/runner/canary.py || exit $?
run_gate goal-bind python3 gate-pack/goal_bind/canary.py || exit $?
run_gate goal-store-g1 python3 -B lh_runtime/goal_canary.py || exit $?
run_gate goal-hierarchy-h1 python3 -B lh_runtime/hierarchy_canary.py || exit $?
run_gate goal-hierarchy-h2 python3 -B lh_runtime/selector_canary.py || exit $?
run_gate goal-hierarchy-h3 python3 -B lh_runtime/turning_point_canary.py || exit $?
run_gate lh-judge-wiring python3 -B lh_runtime/judge_wiring_canary.py || exit $?
run_gate campaign-compiler-g2 python3 -B lh_runtime/campaign_canary.py || exit $?
run_gate goal-matcher-g3 python3 -B lh_runtime/matcher_canary.py || exit $?
run_gate admission-bridge-g4 python3 -B lh_runtime/admission_canary.py || exit $?
run_gate goal-loop-g5 python3 -B lh_runtime/goal_loop_canary.py || exit $?
run_gate goal-loop-g6-ci-conclusion python3 -B lh_runtime/ci_conclusion_canary.py || exit $?
run_gate execution-receipt python3 gate-pack/execution_receipt/canary.py || exit $?
run_gate verification-reducer python3 gate-pack/verification_reducer/canary.py || exit $?
run_gate value-route python3 gate-pack/value_route/canary.py || exit $?
run_gate lh-native-runtime python3 lh_runtime/canary.py || exit $?
run_gate lh-knowledge-fts5 python3 lh_runtime/knowledge_canary.py || exit $?
run_gate lh-runtime-mcp python3 lh_runtime/mcp_canary.py || exit $?
run_gate lh-command-ingress python3 -B lh_runtime/command_ingress_canary.py || exit $?
run_gate lh-intent-derivation python3 -B lh_runtime/intent_derivation_canary.py || exit $?
run_gate lh-goal-loop-driver python3 -B lh_runtime/goal_loop_driver_canary.py || exit $?
run_gate lh-supervisor python3 -B lh_runtime/supervisor_canary.py || exit $?
run_gate lh-attempt-fencing python3 -B lh_runtime/attempt_fencing_canary.py || exit $?
run_gate lh-attempt-timeout python3 -B lh_runtime/attempt_timeout_canary.py || exit $?
run_gate lh-driver-heartbeat python3 -B lh_runtime/driver_heartbeat_canary.py || exit $?
run_gate lh-goal-loop-run-verdict python3 -B lh_runtime/goal_loop_run_verdict_canary.py || exit $?
run_gate lh-run-liveness python3 -B lh_runtime/run_liveness_canary.py || exit $?
run_gate lh-durable-budget python3 -B lh_runtime/durable_budget_canary.py || exit $?
run_gate lh-b12-live-smoke python3 -B lh_runtime/b12_live_smoke_canary.py --dry-run || exit $?
run_gate lh-executor-wiring python3 -B lh_runtime/executor_wiring_canary.py || exit $?
run_gate lh-token-accounting python3 -B lh_runtime/token_accounting_canary.py || exit $?
run_gate lh-project-status python3 -B lh_runtime/project_status_canary.py || exit $?
run_gate lh-status-snapshot python3 -B lh_runtime/status_snapshot_canary.py || exit $?
run_gate lh-project-binding python3 -B lh_runtime/project_binding_canary.py || exit $?
run_gate lh-second-project-b5 python3 -B lh_runtime/second_project_canary.py || exit $?
run_gate lh-github-conclusion python3 -B lh_runtime/github_conclusion_canary.py || exit $?
run_gate lh-b7-live-smoke python3 -B lh_runtime/b7_live_smoke_canary.py --dry-run || exit $?
run_gate lh-codex-usage python3 -B lh_runtime/codex_usage_canary.py || exit $?
run_gate lh-usage-parsers python3 -B lh_runtime/usage_parser_canary.py || exit $?
run_gate lh-value-reducer python3 -B lh_runtime/value_reducer_canary.py || exit $?
run_gate lh-value-gate python3 -B lh_runtime/value_gate_canary.py || exit $?
run_gate design-grill python3 gate-pack/design_grill/canary.py || exit $?
run_gate independent-falsifier python3 gate-pack/independent_falsifier/canary.py || exit $?
run_gate provider-egress python3 gate-pack/provider_egress/canary.py || exit $?

printf '[PASS] all gate-pack checks\n'
