# 自主 driver v1

狀態：active。本文件描述有界的自主 loop。依據為 `lh_runtime/goal_loop_driver.py`、`lh_runtime/goal_loop_run.py`、`lh_runtime/goal_loop_worker.py` 與 `lh_runtime/dispatch_gate.py`。

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

## 5. 驗收燈

driver 與入口的行為，由不呼叫 provider 的 canary 驗證，例如：
- `lh_runtime/goal_loop_canary.py`：完整 loop；
- `lh_runtime/driver_heartbeat_canary.py`：heartbeat；
- `lh_runtime/live_smoke_canary.py`：離線閉環。

真實 executor 的實測（`live_smoke_canary.py --live`）由人執行，不在 `npm test` 之內。

## 與現行程式的差異

- 原設計附有特定宿主的 executor 與兩個內建的廠商 executor。公開版只有宣告式 executor，沒有宿主 executor。
- 原設計以服務的 CI 結論推進 parked run。公開版保留 verdict store 與 conclusion source 介面，但 contract 中的 `external_verdict` 區塊會被拒絕，接線必須經由引擎 API 注入。
