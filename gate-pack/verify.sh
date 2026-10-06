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
run_gate ceremony-prefix python3 -B gate-pack/ceremony_grader_canary.py || exit $?
run_gate boundary-seal python3 gate-pack/boundary_seal/canary.py || exit $?
run_gate docs-structure python3 gate-pack/docs_structure/canary.py || exit $?
run_gate docs-contracts python3 gate-pack/docs_contracts/canary.py || exit $?
run_gate decision-registry python3 -B gate-pack/decision_registry/canary.py || exit $?
run_gate progress-receipts python3 -B gate-pack/progress_receipts/canary.py || exit $?
run_gate advancement python3 -B gate-pack/advancement/canary.py || exit $?
run_gate retry-verifier python3 -B gate-pack/retry_verifier/canary.py || exit $?
run_gate improvement python3 gate-pack/improvement/canary.py || exit $?
run_gate goal-bind python3 gate-pack/goal_bind/canary.py || exit $?
run_gate goal-store-g1 python3 -B lh_runtime/goal_canary.py || exit $?
run_gate goal-assignment python3 -B lh_runtime/goal_assignment_canary.py || exit $?
run_gate goal-hierarchy-h1 python3 -B lh_runtime/hierarchy_canary.py || exit $?
run_gate goal-hierarchy-h2 python3 -B lh_runtime/selector_canary.py || exit $?
run_gate goal-hierarchy-h3 python3 -B lh_runtime/turning_point_canary.py || exit $?
run_gate lh-judge-wiring python3 -B lh_runtime/judge_wiring_canary.py || exit $?
run_gate lh-capability-routing python3 -B lh_runtime/capability_resolver_canary.py || exit $?
run_gate lh-routing-authority python3 -B lh_runtime/routing_authority_canary.py || exit $?
run_gate lh-adaptive-routing python3 -B lh_runtime/adaptive_routing_canary.py || exit $?
run_gate campaign-compiler-g2 python3 -B lh_runtime/campaign_canary.py || exit $?
run_gate goal-matcher-g3 python3 -B lh_runtime/matcher_canary.py || exit $?
run_gate admission-bridge-g4 python3 -B lh_runtime/admission_canary.py || exit $?
run_gate goal-loop-g5 python3 -B lh_runtime/goal_loop_canary.py || exit $?
run_gate goal-loop-g6-ci-conclusion python3 -B lh_runtime/ci_conclusion_canary.py || exit $?
run_gate lh-worker-async-dispatch python3 -B lh_runtime/worker_async_canary.py || exit $?
run_gate lh-verifier-normalizer-n15 python3 -B lh_runtime/verifier_normalizer_canary.py || exit $?
run_gate lh-lamp-precheck python3 -B lh_runtime/lamp_precheck_canary.py || exit $?
run_gate execution-receipt python3 gate-pack/execution_receipt/canary.py || exit $?
run_gate verification-reducer python3 gate-pack/verification_reducer/canary.py || exit $?
run_gate lh-native-runtime python3 lh_runtime/canary.py || exit $?
run_gate lh-knowledge-fts5 python3 lh_runtime/knowledge_canary.py || exit $?
run_gate lh-knowledge-indexer python3 -B lh_runtime/knowledge_indexer_canary.py || exit $?
run_gate lh-runtime-mcp python3 lh_runtime/mcp_canary.py || exit $?
run_gate lh-command-ingress python3 -B lh_runtime/command_ingress_canary.py || exit $?
run_gate lh-intent-derivation python3 -B lh_runtime/intent_derivation_canary.py || exit $?
run_gate lh-goal-loop-driver python3 -B lh_runtime/goal_loop_driver_canary.py || exit $?
run_gate lh-supervisor python3 -B lh_runtime/supervisor_canary.py || exit $?
run_gate lh-scheduler-entrypoint python3 -B lh_runtime/scheduler_entrypoint_canary.py || exit $?
run_gate lh-dispatch-envelope python3 -B lh_runtime/dispatch_envelope_canary.py || exit $?
run_gate lh-attempt-fencing python3 -B lh_runtime/attempt_fencing_canary.py || exit $?
run_gate lh-local-process-fence python3 -B lh_runtime/execution_fence_local_canary.py || exit $?
run_gate lh-fence-activation-smoke python3 -B lh_runtime/fence_activation_smoke.py || exit $?
run_gate lh-delivery-runner python3 -B lh_runtime/delivery_runner_canary.py || exit $?
run_gate lh-candidate-review python3 -B lh_runtime/candidate_review_canary.py || exit $?
run_gate lh-candidate-review-work-unit python3 -B lh_runtime/candidate_review_work_unit_canary.py || exit $?
run_gate lh-normal-successor python3 -B lh_runtime/normal_successor_canary.py || exit $?
run_gate lh-effect-guard python3 -B lh_runtime/effect_guard_canary.py || exit $?
run_gate lh-clone-push-boundary python3 -B lh_runtime/clone_push_boundary_canary.py || exit $?
run_gate lh-open-questions python3 -B lh_runtime/open_questions_canary.py || exit $?
run_gate lh-known-defects python3 -B lh_runtime/known_defects_canary.py || exit $?
run_gate lh-lifecycle-context-path-n05 python3 -B lh_runtime/lifecycle_context_path_canary.py || exit $?
run_gate lh-provider-input-binding python3 -B lh_runtime/provider_input_binding_canary.py || exit $?
run_gate lh-attempt-timeout python3 -B lh_runtime/attempt_timeout_canary.py || exit $?
run_gate lh-driver-heartbeat python3 -B lh_runtime/driver_heartbeat_canary.py || exit $?
run_gate lh-goal-loop-run-verdict python3 -B lh_runtime/goal_loop_run_verdict_canary.py || exit $?
run_gate lh-run-liveness python3 -B lh_runtime/run_liveness_canary.py || exit $?
run_gate lh-durable-budget python3 -B lh_runtime/durable_budget_canary.py || exit $?
run_gate lh-b12-live-smoke python3 -B lh_runtime/b12_live_smoke_canary.py --dry-run || exit $?
run_gate lh-executor-wiring python3 -B lh_runtime/executor_wiring_canary.py || exit $?
run_gate lh-declared-executor python3 -B lh_runtime/declared_executor_canary.py || exit $?
run_gate lh-platform-ports-p1 python3 -B lh_runtime/platform_ports_canary.py || exit $?
run_gate lh-token-accounting python3 -B lh_runtime/token_accounting_canary.py || exit $?
run_gate lh-project-status python3 -B lh_runtime/project_status_canary.py || exit $?
run_gate lh-status-snapshot python3 -B lh_runtime/status_snapshot_canary.py || exit $?
run_gate lh-instance-config python3 -B lh_runtime/instance_config_canary.py || exit $?
run_gate lh-project-binding python3 -B lh_runtime/project_binding_canary.py || exit $?
run_gate lh-lifecycle-p3 python3 -B lh_runtime/lifecycle_canary.py || exit $?
run_gate lh-second-project-b5 python3 -B lh_runtime/second_project_canary.py || exit $?
run_gate lh-owner-durability python3 -B lh_runtime/owner_durability_canary.py || exit $?
run_gate lh-grill-loop python3 -B lh_runtime/grill_loop_canary.py || exit $?
run_gate lh-failure-case-live-acceptance python3 -B lh_runtime/failure_case_live_acceptance_canary.py || exit $?
run_gate lh-stop-lines python3 -B lh_runtime/stop_lines_canary.py || exit $?
run_gate lh-usage-delta python3 -B lh_runtime/usage_delta_canary.py || exit $?
run_gate lh-cli-executor-flags python3 -B lh_runtime/cli_flags_canary.py || exit $?
run_gate lh-live-smoke python3 -B lh_runtime/live_smoke_canary.py || exit $?
run_gate lh-usage-void python3 -B lh_runtime/usage_void_canary.py || exit $?
run_gate lh-standing-intent python3 -B lh_runtime/standing_intent_canary.py || exit $?
run_gate lh-run-revival python3 -B lh_runtime/revival_canary.py || exit $?
run_gate lh-diff-grader python3 -B lh_runtime/diff_grader_canary.py || exit $?
run_gate lh-authority-surface python3 -B lh_runtime/s1_canary.py || exit $?
run_gate lh-evidence-integrity python3 -B lh_runtime/evidence_integrity_canary.py || exit $?
run_gate lh-value-reducer python3 -B lh_runtime/value_reducer_canary.py || exit $?
run_gate lh-value-gate python3 -B lh_runtime/value_gate_canary.py || exit $?
run_gate design-grill python3 gate-pack/design_grill/canary.py || exit $?
run_gate independent-falsifier python3 gate-pack/independent_falsifier/canary.py || exit $?
run_gate provider-egress python3 gate-pack/provider_egress/canary.py || exit $?
run_gate lh-pure-loop-boundary python3 -B lh_runtime/pure_loop_boundary_canary.py || exit $?

printf '[PASS] all gate-pack checks\n'
