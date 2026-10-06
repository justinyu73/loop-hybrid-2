# Planner recovery v1

狀態：active。本文件描述 planner 在失敗之後提出有界修復方案，並由獨立驗證者審核的流程。依據為：
- `lh_runtime/goal_loop_worker.py`（campaign 路徑）；
- `lh_runtime/work_unit_store.py`（紀錄、預算、稽核與套用）；
- `lh_runtime/runner_adapter.py`（綁定解析）；
- `lh_runtime/project_binding.py`（contract 封存）；
- `lh_runtime/campaign_compiler.py`（連續失敗門檻）。

## 1. 範圍與原則

- recovery 只**提出並審核**修復方案；審核通過本身不構成授權。
- 產出方案的 planner 與審核的驗證者必須是不同身分，而且驗證者唯讀。
- 每一步都寫入 durable 紀錄（`lh-recovery-record/v1`）。重啟後依紀錄續行，不重複呼叫已記錄的角色。
- 任何綁定不符、身分不明或預算用盡，都會停下並記錄原因，不會猜測。

## 2. 啟用

recovery 是 opt-in，必須同時滿足三個條件：

1. **contract 帶 `planner_recovery` 區塊**（`lh-planner-recovery-binding/v1`）。欄位固定為：
   - `schema`；
   - `identity_profile`，必須是 `native-run-v1`；
   - `planner_argv` 與 `plan_verifier_argv`；
   - `planner_provider`；
   - `budget`。
   `project_binding.py` 以 contract 的 digest 封存這個區塊。
2. **contract 帶 `execution_binding`**（`host-task-area-execution-binding/v1`）。其中的 provider registry 以 digest 釘住。planner 的指令必須等於 registry 中 planning provider 的指令；驗證者的指令必須等於 verifier provider 的指令。
3. **執行經由排程器的 dispatch envelope**：envelope 的 owner 必須等於 `LH_SCHEDULER_OWNER_ID`，desired state 為 `enabled`，contract digest 與檔案內容一致。

任何一項不符，綁定解析就會拒絕（例如 `native_runtime_dispatch_mismatch`、`native_recovery_binding_invalid`、`native_recovery_provider_binding_mismatch`），recovery 不會啟用。

### 預算

| 欄位 | 限制 |
|---|---|
| `planner_calls`、`plan_verifier_calls` | 正整數；campaign 路徑兩者都必須是 1 |
| `planner_timeout_seconds`、`plan_verifier_timeout_seconds`、`incident_timeout_seconds` | 正數、有限，不超過一年 |

## 3. Campaign 路徑（已接線）

1. **觸發**：同一個 campaign 的 Goal 連續失敗，次數達到 campaign 宣告的門檻（`failure_stop_threshold`，3～5，預設 3）時，引擎寫入一筆 stop line 事件，把受影響的 Goal 轉為 `human_required`。啟用 recovery 時，事件同時記下失敗的 Goal 與 dispatch 綁定。
2. **請求**：對每一筆 stop line，引擎建立一筆 recovery 請求，原因代碼為 `campaign_consecutive_failures`。請求包括：
   - 每個失敗子 Goal 的 receipt、diff 與驗收指令；
   - 合併後的寫入範圍；
   - 剩餘的 attempt 預算；
   - 綁定 contract、dispatch 與 stop line 的 authority digest。

   子 Goal 的收據讀回不符時，記為 `campaign_recovery_rejected`，並轉為 `human_required`。
3. **角色呼叫**：依序呼叫 planner 與驗證者，兩者都以唯讀方式執行。
   - 每次呼叫前都重新驗證輸入：請求 digest、contract digest、stop line、子 Goal 收據。
   - 每次呼叫前都先寫入 claim，記錄呼叫者程序的身分。
   - coding、planning、verifier 三個 principal 必須互不相同，否則拒絕（`campaign_recovery_verifier_not_independent`）。
   - 超過角色次數或 incident 期限時，停在 `awaiting_authority`（`campaign_recovery_role_budget_exhausted`、`campaign_recovery_incident_deadline_exhausted`）。
   - 重啟時若有未結的 claim：該程序仍在執行就等待；程序已不在就記為 `outcome_unknown`，不會重呼叫。
4. **審核**：以第 5 節的規則驗證方案與判定，不通過記為 `campaign_recovery_plan_invalid`。
5. **結果**：審核通過後，一律停在 `awaiting_authority`。原因為以下兩者之一：
   - `campaign_recovery_requires_authority`；
   - `campaign_recovery_child_attempt_budget_exhausted`（有子 Goal 的 attempt 已用完）。

   引擎不會自行補充子 Goal 的 attempt 上限，也不會自行套用方案。

紀錄的狀態：`requested` → `claimed` → `result_recorded` → `verifier_claimed` → `verdict_recorded` → `awaiting_authority`。中途可能停在 `rejected` 或 `outcome_unknown`。

## 4. Work-unit 路徑（store API，尚未接線）

`work_unit_store.py` 為 work-unit 的單一 Run 提供完整的 store API：
- `record_recovery_request`；
- `claim_recovery_phase`：角色為 `planner`、`plan_verifier`、`audit`；
- `finish_recovery_phase`；
- `apply_recovery_decision`；
- 以及 incident 與 repair context 的讀回。

**目前公開版沒有任何引擎路徑呼叫這組 API。** 它是給外部協調者使用的 store 介面。

- **incident**：同一個 Run、同一個檢查階段的連續失敗，由已結算的收據推導，不採信呼叫端提供的次數。
- **稽核**：incident 的失敗次數達 3 時，必須先有唯讀稽核（`lh-planner-recovery-audit-binding/v1`、`lh-recovery-audit-result/v1`）。稽核者不得是 planner，結果必須綁定同一個請求與失敗清單。
- **請求**：必須指名真實的 Run，且 work unit、goal、revision、node 都與 store 一致；`request_id` 必須等於請求自身 key 欄位的 digest。
- **預算**：planner 呼叫與驗證者呼叫的次數，以 Run 為單位、跨該 Run 的所有請求計算；用盡時 claim 被拒（`planner_call_budget_exhausted`）。
- **套用**（`apply_recovery_decision`）：先依第 5 節重驗方案與判定，再要求紀錄為 `plan_verified`，而且動作必須與審核過的方案相同。審核之前，或動作與方案不同時，一律拒絕。

| 動作 | 結果 |
|---|---|
| `resume_phase`（方案的目標階段為 `closeout`，且 Run 已整合） | `applied` |
| `dispatch_successor`（原因與階段都是 `machine_complete`、Run 已整合、指向另一個節點） | `applied` |
| `repair_same_node`、`retry_within_budget` | 紀錄維持 `plan_verified`，apply 狀態為 `retry_pending`；必須附證據與 attempt 上限，實際的重試由後續的完成流程提交 |
| `request_authority` | `awaiting_authority` |
| `insufficient_evidence` | `awaiting_evidence` |
| `collect_evidence` | 套用時拒絕（unsupported） |

- **引擎自己的停止**：同一檢查階段連續失敗 3 次時，完成流程本身會把 work unit 停在 `audit_required`；此時沒有唯讀稽核，planner 無法 claim，也無法套用任何動作。

## 5. 方案與判定的驗證規則

兩條路徑共用 `validate_recovery_plan`：
- 方案與判定都必須綁定同一個請求 id 與請求 digest；
- 動作種類必須在封閉集合內：`resume_phase`、`repair_same_node`、`retry_within_budget`、`collect_evidence`、`request_authority`、`insufficient_evidence`、`dispatch_successor`；
- 產出者與驗證者的 principal 不同，並各自綁定 planning 與 verifier capability；
- 判定必須為 `GREEN`、附理由、唯讀，而且沒有寫入 source；
- 方案的 digest 由引擎依方案內容重算，方案與判定兩邊都必須一致；
- 判定必須綁定同一個候選 digest 與 authority digest；
- 請求帶有已完成的作用時，決策依據必須引用原因代碼與同一組證據。

## 6. 驗收燈

`lh_runtime/planner_recovery_canary.py`（gate `lh-planner-recovery`）涵蓋兩部分：

- **方案與判定的驗證規則**（第 5 節）：一筆綁定正確的紀錄必須通過，每一種單一違規都必須被拒絕；
- **campaign 路徑的狀態機**（第 3 節），以真實的 GoalStore 執行，涵蓋以下情況：
  - 審核通過後停在 `awaiting_authority`，不會被套用；
  - 子 Goal 的 attempt 用完時不補充；
  - principal 不獨立時，在任何角色呼叫之前就拒絕；
  - 角色失敗時不重試：已啟動後失敗記為 `outcome_unknown`，未能啟動則記為 `rejected`；
  - 重啟後，已記錄的 planner 不會再被呼叫；
  - 未結的 claim、但程序已不在時，記為 `outcome_unknown`；
  - incident 期限已過時，停在 `awaiting_authority`；
  - stop line 被改過時，記為 authority mismatch。

考卷中有兩處是測試替身，**不算涵蓋**：native 綁定（正式環境由封存的 contract、provider registry 與 dispatch 解析），以及失敗子 Goal 收據的讀取。

`lh_runtime/work_unit_recovery_canary.py`（gate `lh-work-unit-recovery`）在 work-unit fixture 走完真實完成流程的 Run 上，涵蓋第 4 節：
- 請求的身分與 key；
- 以 Run 為單位的呼叫預算；
- 套用時的重驗、各動作的落點、審核之前不得套用；
- 連續 3 次失敗時引擎停在 `audit_required`，以及稽核之前 planner 無法 claim。

只有 phase 綁定的 metadata（capability、fence、provider input）是 fixture 資料。

`lh_runtime/native_recovery_binding_canary.py`（gate `lh-native-recovery-binding`）以真實檔案建立第 2 節的完整封存鏈，驗證兩件事：
- 封存正確的鏈可以被解析；
- 以下十種單一變動，各以其特定原因被拒：
  - contract 在封存後被改；
  - registry 在釘住後被改；
  - planner 指令與 registry 不符；
  - 角色次數不是 1；
  - identity profile 不是 `native-run-v1`；
  - 驗證者與 coding 是同一身分；
  - dispatch owner 不符；
  - desired state 不是 `enabled`；
  - campaign 不符；
  - base 不符。

以下尚無行為考卷：從真實失敗的子 Goal 建立 campaign 請求，並經 fence 呼叫角色。它需要原生 run 的完整路徑（scheduler 入口、連續失敗的原生子 Run、native store 連結），以及能依指令邊界 metadata 產生方案的替身 planner。

這份契約另由 `gate-pack/docs_contracts/canary.py` 檢查：文件中提到的路徑、schema id 與名稱都必須存在於程式中。

## 與現行程式的差異

- 原設計先以固定選路表處理失敗，只有需要判斷的情況才交給 planner。公開版沒有選路表：campaign 路徑在連續失敗後直接交給 planner，而單次失敗不會觸發 recovery。
- 原設計的 recovery 由協調者驅動 work-unit 的完整生命週期。公開版保留了 store API 並有行為考卷，但沒有任何引擎路徑呼叫它。
- 本文件第一版（X18）的動作表把 `collect_evidence` 寫成「維持 `plan_verified`」，並把 `repair_same_node`、`retry_within_budget` 寫成「排入重試」。依程式更正為上表：前者在套用時被拒，後兩者的紀錄維持 `plan_verified`、apply 為 `retry_pending`。
- `execution_binding` 的 schema 名稱仍帶有前身時期的 `host-` 前綴。
