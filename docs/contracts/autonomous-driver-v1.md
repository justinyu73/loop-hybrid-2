# 自主 driver v1

狀態：active。本文件描述有界的自主 loop。依據為 `lh_runtime/goal_loop_driver.py`、`lh_runtime/goal_loop_run.py`、`lh_runtime/goal_loop_worker.py`、`lh_runtime/dispatch_gate.py`、`lh_runtime/fleet.py` 與 `lh_runtime/onboarding.py`。

## 1. 形狀

- `goal_loop_driver.run_driver` 反覆呼叫 `GoalLoopWorker.tick`，直到某個停止條件成立。它本身不接任何 provider，model runner 由外部注入。
- `goal_loop_run.run` 是正式入口：解析 contract、驗證宣告的 executor，再把它接進 driver。

```text
指令入庫 -> goal events -> worker.tick（admission / 執行 / 驗證 / 推進）-> run_driver
                                      ^ model = contract 中宣告的 executor
```

每次呼叫都是一個有界 session。任何外部排程器定期呼叫同一個指令，就能常駐；重啟後從 durable store 續跑。

## 2. 停止條件（`stop_reason`）

| 值 | 意義 |
|---|---|
| `paused` | `--pause-flag` 指定的檔案存在（每個 tick 前檢查） |
| `max_cycles` | 達到 `--max-cycles` |
| `budget` | 達到 `--max-runs` |
| `budget_exhausted` | 以 receipt 計算的 token 用量達到 `--budget-ceiling-tokens` |
| `budget_unknown` | 某筆用量為 unknown，無法確定是否超出預算 |
| `timeout` | 經過 `--max-runtime-seconds` |
| `parked` | 只剩 `human_required` 的 Goal，需要人處理 |
| `idle` | 佇列真的空了，之後可以安全重啟 |
| `not_holder` | 本程序沒有取得 driver 所有權；這不是 Goal 或 Run 的終態 |
| `shutdown_requested` | 收到停止要求；目前的 tick 收尾後結束 |
| `lifecycle_unavailable` | 無法建立程序 lifecycle |

dispatch gate 也可能以自己的 reason code 停止，例如 `daily_cost_hard` 與 `executor_auth`，見 `status-snapshot-v1.md`。連續 `--idle-limit` 個 idle tick 之後，driver 結束。

預算由呼叫者注入：`--budget-ceiling-tokens` 與可選的 `--budget-scope`。driver 在啟動與每個 tick 前，都從 `RunStore.usage_records()` 重新計算，不自行推斷日期、campaign 或費率。

## 3. Executor 選擇

- 帶 contract 時，由 `models.execute` 引用 `executors` 區塊中的宣告。
- 不帶 contract 時，以 `--executors` 傳入宣告檔、以 `--executor` 指定名稱。
- 未宣告的名稱一律拒絕；引擎不在 `PATH` 上搜尋。

dry-run（不加 `--execute`）只印出解析後的計畫，不呼叫任何模型。

## 4. 安全邊界

- executor 只在釘住 base commit 的一次性 clone 中執行，不碰原始工作樹。
- 每個指令都要經過 execution fence。沒有選 backend 時停用，run 停在 `human_required`。
- 驗收只由 committed 的驗收燈與 canary 決定，模型輸出永不構成驗收。
- 超出 `allowed_paths` 的 diff，以決定性方式轉為 `human_required`。
- 宣告中的旗標決定 agent 能有多少自主權；安全邊界仍是一次性 clone。

## 5. 多專案（fleet）

`lh_runtime/fleet.py --registry <file>` 在一次喚醒中，依登記表順序處理每個專案。登記表 `lh-fleet-registry/v1` 是封閉的：

- 每個專案有 `project_id`、`contract` 與 `desired_state`（`enabled` 或 `paused`），可選 `max_cycles`（預設 30）與 `max_runtime_seconds`；
- 兩個上限都必須是正數。未知欄位、重複的 id、未知狀態一律拒絕；
- `contract` 的相對路徑以登記表所在目錄解析。

喚醒時的處理：

- `paused` 的專案不會被喚醒；
- `enabled` 的專案以子程序執行一次有界的 `goal_loop_run.py --contract <path> --execute --max-cycles N`。每個專案保有自己的 contract、store、singleton lock 與 receipt；
- 某個專案失敗時記為 `failed`，下一個專案照常執行；
- 專案的 lock 已被其他程序持有時，該專案回報 driver 的 `not_holder`，不會重複執行。

輸出 `lh-fleet-wake/v1`，列出每個專案的狀態與 `stop_reason`。有任何專案失敗時，結束碼為 1。fleet 本身不保存狀態、也不安裝任何服務，由外部排程器（cron、工作排程器、CI 計時器）呼叫。可用 `--only` 只喚醒單一專案。

## 6. 專案上手（onboarding）

`lh_runtime/onboarding.py` 分三步把目標 repo 接上 loop：

- `init <target>`：寫入 `project_runtime_contract.json`（含 `executors` 宣告範本，executor 先以佔位路徑表示）與 `checks/acceptance.py`（驗收燈範本）。已存在的檔案不覆寫，輸出 `lh-onboarding-init/v1`。
- `validate <target>`：只讀檔案與 git 物件，不執行目標中的任何東西，輸出 `lh-onboarding-validate/v1`。回報的代碼包括：`verifier_in_allowed_paths`、`verifier_missing`、`verifier_not_committed`、`executor_not_absolute`、`executor_missing`、`executor_prompt_slot_missing`、`model_executor_undeclared`、`base_revision_unknown`。形狀都正確時，再交給引擎自己的 resolver 綁定一次；失敗則回報 `contract_unresolvable`。
- `pilot <target> --executors <file> --executor <name>`：以宣告的替身 executor 取代 contract 中的 executor，先對這份有效 contract 執行 validate，不通過就拒絕。通過後把目標 clone 到暫存目錄，base 釘在 `base_revision` 解析出的 commit，再經真實入口（`command_ingress.py`、`goal_loop_run.py --execute`）跑一次完整的 run。沒有指定 fence backend 時使用 `local-process`，receipt 會如實寫出沒有隔離。輸出 `lh-onboarding-pilot/v1`：run 與 Goal 的狀態、receipt 路徑，以及目標 repo 的 HEAD 與工作樹前後是否相同。

pilot 讀的是目標的已 commit 內容。範本的交付檢查包含 `git diff --cached --check`：在沒有設定 `core.autocrlf` 的 Windows 上，以文字模式寫出 CRLF 的 executor 會被這個檢查擋下。

## 7. 驗收燈

driver 與入口的行為，由不呼叫 provider 的 canary 驗證，例如：
- `lh_runtime/goal_loop_canary.py`：完整 loop；
- `lh_runtime/driver_heartbeat_canary.py`：heartbeat；
- `lh_runtime/live_smoke_canary.py`：離線閉環；
- `lh_runtime/fleet_canary.py`：多專案喚醒、隔離、暫停與 `not_holder`；
- `lh_runtime/onboarding_e2e_canary.py`：在乾淨環境中走完 init、validate、pilot 並讀回 receipt。

真實 executor 的實測（`live_smoke_canary.py --live`）由人執行，不在 `npm test` 之內。

## 與現行程式的差異

- 原設計的多專案排程附有控制平面、執行期登記與系統服務。公開版的 fleet 只有一個靜態登記表，觸發一律交給外部排程器。
- 原設計附有特定宿主的 executor 與兩個內建的廠商 executor。公開版只有宣告式 executor，沒有宿主 executor。
- 原設計以服務的 CI 結論推進 parked run。公開版保留 verdict store 與 conclusion source 介面，但 contract 中的 `external_verdict` 區塊會被拒絕，接線必須經由引擎 API 注入。
