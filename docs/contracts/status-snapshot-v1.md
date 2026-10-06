# 狀態快照 v1

狀態：active。本文件描述 driver 寫出的狀態快照（`loop-hybrid-status-snapshot/v1`），以及讀取端可以依賴的欄位。依據為 `lh_runtime/status_snapshot.py`、`lh_runtime/project_status.py`、`lh_runtime/dispatch_gate.py` 與 `lh_runtime/status_lamp.py`。

## 1. 範圍

快照是 durable store 的**唯讀投影**，不是權威：
- 讀取端可以顯示它；
- 但 Goal、Run 與驗收的判定，一律以 store 與 receipt 為準。

快照由 `--status-snapshot-out`（或 contract 的 `runtime.status_snapshot_out`）指定的路徑寫出。

## 2. 保證存在的頂層欄位

| 欄位 | 意義 |
|---|---|
| `schema` | 固定為 `loop-hybrid-status-snapshot/v1` |
| `generated_at` | 產生時間 |
| `run_store_root`、`goal_store_root` | 兩個 store 的路徑 |
| `status` | 專案狀態物件（`loop-hybrid-project-status/v1`，見第 3 節） |
| `dispatch_gate` | 最近一次 dispatch gate 判定；未接 gate 時為 null |
| `heartbeat` | 最近一次 driver heartbeat（`loop-hybrid-driver-heartbeat/v1`） |
| `heartbeat_age_seconds` | heartbeat 的年齡；讀不到時為 null |
| `stale` | heartbeat 缺失、無法讀取或超過門檻時為 true |
| `staleness_threshold_seconds` | 門檻：attempt 牆鐘上限加上 tick 額外開銷，嚴格大於兩者之和 |
| `attempt_wall_clock_upper_bound_seconds`、`tick_overhead_seconds` | 門檻的組成 |
| `run_liveness` | 各個 running run 是否仍在門檻之內 |
| `open_questions` | 需要人處理的待辦（`lh-open-questions/v1`，見第 6 節） |
| `code_identity` | driver 載入的引擎 digest、目前磁碟上的 digest，以及兩者是否不同（`stale`）；見第 5 節 |
| `lamp` | 唯一的健康判定（`lh-status-lamp/v1`），見第 7 節 |

## 3. 狀態物件

| 欄位 | 意義 |
|---|---|
| `headline` | 給人看的一行摘要 |
| `runs_by_state`、`goals_by_state` | 各狀態的數量 |
| `active_runs`、`completed_goals` | 進行中的 run 與已完成的 Goal |
| `needs_human`、`needs_human_events`、`parked_goals` | 需要人處理的 Goal 與事件 |
| `value` | 價值判定彙總（`loop-hybrid-value-rollup/v1`），含 `value_red` |
| `cost` | `estimated_cost_usd`、`total_tokens`、`cost_complete`。費率只來自宣告的 `pricing`；有任何 unknown 用量時 `cost_complete` 為 false |
| `evidence` | 最新 receipt 的指標（`loop-hybrid-receipt-evidence/v1`） |

## 4. Dispatch gate

每次派工前，gate 會讀取 durable 狀態，回答 `allow`、`note`、`idle` 或 `stop`，並附上 `reason_code`：

| reason_code | 動作 | 條件 |
|---|---|---|
| `daily_cost_soft` | `idle` | 當天（UTC）已計價的用量達到軟上限，不派新工作 |
| `daily_cost_hard` | `stop` | 達到硬上限，driver session 結束 |
| `quota_unknown` | `stop` | 已注入的配額讀取器讀不到數值；未知配額不可派工 |
| `executor_auth` | `stop` | attempt 的 provider 紀錄出現明確的認證失敗標記 |

- 配額讀取器由使用者注入，引擎不附任何配額來源。
- 每次評估都重新讀取輸入，所以條件解除後（例如換日或修好憑證），下一個 tick 就會自動恢復。

## 5. Heartbeat

driver 在每個 tick 寫出 heartbeat，內含 holder、phase、cycles 與單調時鐘時間戳。`stale` 只是投影：它不會改變恢復語意，也不會自行結束任何 run。

heartbeat 也帶 `code_identity`：
- `loaded_digest`：driver 啟動時，引擎目錄中所有非 canary `*.py` 的 digest；
- `disk_digest`：寫出 heartbeat 當下，同一組檔案的 digest；
- `stale`：兩者不同時為 true，代表磁碟上的引擎已更新但 driver 沒有重啟。

快照的 `code_identity` 以 heartbeat 的 `loaded_digest` 與產生快照時的磁碟 digest 比較；沒有 heartbeat 時 `loaded_digest` 與 `stale` 為 null。這只是回報，引擎不會因此自動重啟。digest 涵蓋整個引擎目錄，所以即使改動的是 driver 沒載入的模組，也會回報 stale（偏保守）。

## 6. 待人處理的事項

`lh_runtime/open_questions.py` 把每個 `human_required` 的 Goal 與事件，投影成一筆有型別的待辦：
- 欄位：來源、對象、原因、開始等待時間、已等待秒數，以及超過門檻時的 `quiet` 標記；
- 類型由封閉的原因代碼前綴表決定：`awaiting_owner`、`blocked_by_evidence`、`scope_escalation`；
- 表外的代碼一律歸為 `awaiting_owner`，並保留原始原因；
- 這只是投影，不做決定，也不寫入任何 store。
- 每一筆都帶 `route`（`lh-failure-route/v1`，見 `failure-routing-v1.md`）與 `machine_route_available`：原因其實是機器可處理、卻停在 `human_required` 時為 true。

## 7. 健康燈

`lh_runtime/status_lamp.py` 的 `lamp(snapshot)` 是唯一的健康判定。它是純函式：只讀取快照、不改變任何東西，相同輸入一定得到相同結果。輸出 `ok` 或 `degraded`，附上評估過的規則與觸發的規則文字：

| 規則 | 觸發條件 |
|---|---|
| `heartbeat_stale` | 快照的 `stale` 不是 false |
| `code_identity_stale` | `code_identity.stale` 不是 false（含未知） |
| `needs_human` | `needs_human` 或 `needs_human_events` 大於 0，或數量未知 |
| `dispatch_stopped` | 最近一次 dispatch gate 判定為 `stop` |

未知不算健康：輸入缺失或讀不到時，對應規則觸發。快照的 `lamp` 欄位就是對其餘欄位呼叫 `lamp()` 的結果；讀取端需要判定時，以這盞燈為準，不要從其他欄位自行推導。

## 與現行程式的差異

- 原設計的快照包含多專案彙總，供控制平面讀取。公開版的快照只涵蓋單一專案的兩個 store。
- 原設計附有特定廠商的配額監控。公開版只保留注入式的配額讀取器介面。
