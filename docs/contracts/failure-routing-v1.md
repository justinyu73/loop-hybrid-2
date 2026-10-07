# 失敗選路 v1

狀態：active。本文件描述失敗之後「下一步由誰做什麼」的固定選路表。依據為 `lh_runtime/failure_router.py` 與 `lh_runtime/open_questions.py`。

## 1. 原則

- 選路是**投影**：它不改變引擎的狀態轉移，也不寫入任何 store。
- 選路不呼叫模型。相同的輸入一定得到相同的收據（`lh-failure-route/v1`），收據可以重算。
- 只有封閉的「擁有者動作」清單需要人；其他代碼都是機器原因，選路結果不會是人工。
- 每個路由都保留失敗證據與先前的收據。

## 2. 收據

`route(reason_code, failure_count=0)` 回傳：

| 欄位 | 意義 |
|---|---|
| `input_reason_code` | 輸入的代碼；空值記為 `unrecorded` |
| `reason_code` | 正規化後的代碼；未知時為 `unknown_reason_code`，重複失敗時為 `repeated_failure_threshold` |
| `family` | 所屬族群 |
| `route`、`owner`、`next_action` | 路由、負責者、下一個動作 |
| `invalidates`、`preserves` | 作廢的對象；保留的項目（固定為失敗證據與先前收據） |
| `human_required`、`owner_action` | 是否需要人，以及對應的擁有者動作 |
| `failure_count`、`route_digest` | 失敗次數；收據內容的 digest |

`verify_route(receipt)` 依收據自己的輸入重算一次。內容被改過，或與目前的表不一致時，回報不通過。

## 3. 族群與路由

| 族群 | 路由 | 負責者 | 代表代碼 |
|---|---|---|---|
| `check_red` | `retry_same_unit` | executor | `check_failed`、`delivery_verifier_red`、`candidate_review_not_green` |
| `review_binding` | `repair_review_binding` | verifier | 其他 `candidate_review_` 開頭的代碼 |
| `verifier_binding` | `repair_verifier_binding` | verifier | `delivery_independent_verifier_` 開頭 |
| `evidence_integrity` | `reread_evidence` | engine | 其他 `delivery_` 開頭、`attempt_receipt_` 開頭、`unrecorded` |
| `planner_repair` | `planner_bounded_repair` | planner | `campaign_consecutive_failures`（見 `planner-recovery-v1.md`） |
| `recovery_integrity` | `reconcile_recovery_record` | engine | 其他 `campaign_recovery_` 開頭 |
| `repeated_failure` | `independent_read_only_audit` | auditor | 任何機器代碼，同一處失敗達 3 次 |
| `unknown` | `repair_router_table` | router | 表中沒有的代碼 |
| `owner_action` | `human_required` | owner | 見第 4 節 |

精確代碼先比對，接著依序比對前綴，第一個符合的生效。

## 4. 擁有者動作（封閉）

只有以下十項會得到 `human_required: true`：
- `material_scope_expansion`；
- `destructive_effect`；
- `paid_provider_invocation`；
- `runtime_activation`；
- `external_account_action`；
- `owner_merge`；
- `product_acceptance`；
- `secret_handling`；
- `publication`；
- `promotion`。

引擎代碼歸入這些類別的方式：

| 引擎代碼 | 擁有者動作 |
|---|---|
| `source_refs_mutated` 開頭 | `destructive_effect` |
| 超出允許路徑或範圍（`changed_path_outside_contract_scope` 等）、`authority_surface` 開頭、`independent_verifier_in_write_scope` | `material_scope_expansion` |
| recovery 需要更多預算或授權（`campaign_recovery_requires_authority`、`campaign_recovery_child_attempt_budget_exhausted` 等） | `material_scope_expansion` |
| `execution_fence_unavailable` 開頭、`verifier_unavailable` 開頭 | `runtime_activation` |
| `regression_detected` 開頭（已完成的 goal 驗收燈轉紅） | `product_acceptance` |

擁有者動作不受失敗次數影響，不會改走稽核。

## 5. 投影接點

`open_questions` 的每一筆待辦都帶有 `route`（完整收據），以及 `machine_route_available`。後者為 true 時，表示這筆待辦停在 `human_required`，但它的原因其實是機器可以處理的。這只是讓這種情況被看見，不改變它的狀態。

## 6. 驗收燈

`lh_runtime/failure_router_canary.py` 涵蓋以下行為：
- 代表代碼的路由；
- 未知代碼的路由；
- 擁有者動作的封閉性；
- 重複失敗的門檻；
- 收據的重算與竄改偵測；
- `open_questions` 各族群的涵蓋；
- 投影不改變 store。

## 與現行程式的差異

- 原設計讓選路結果直接驅動執行，例如同單位重試、重新具體化或稽核。公開版只提供投影，執行流程仍由引擎原有的狀態轉移決定。
- 原設計的負責者是具體的計畫節點。公開版改用角色：executor、verifier、engine、planner、auditor、router、owner。
- 原設計有不可變的修復封包（綁定路由收據、允許路徑、必跑測試）。公開版沒有，因為目前沒有執行端消費它。
