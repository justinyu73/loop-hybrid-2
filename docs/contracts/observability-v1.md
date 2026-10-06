# 可觀測性 v1

狀態：active。本文件描述引擎對外呈現狀態的原則。依據為 `lh_runtime/status_snapshot.py`、`lh_runtime/project_status.py`、`lh_runtime/mcp_server.py` 與 `lh_runtime/knowledge_store.py`。

## 1. 快照是投影，不是權威

- 狀態快照、MCP 讀取介面與任何儀表板，都只是 durable store 的唯讀投影；
- 判定一律以 store 中的狀態、receipt 與 delivery evidence 為準；
- 投影不得自行推導出新的判定。例如「看起來都綠了」不等於 `verified`。

## 2. Driver 是否還活著

快照中的 `stale` 是頂層判定：heartbeat 缺失、無法讀取，或年齡超過門檻時為 true。
- 門檻由 attempt 牆鐘上限加上 tick 額外開銷推導，嚴格大於兩者之和；
- 未知不等於存活；
- `stale` 不改變恢復語意，也不會結束任何 run。

## 3. 唯讀介面

`lh_runtime/mcp_server.py` 提供唯讀的 MCP 介面，可查詢 run、goal 與知識索引：

```bash
python3 -B lh_runtime/mcp_server.py --run-store ... --knowledge-store ...
```

它沒有任何寫入工具。

## 4. 證據位置

`runs/artifacts/<run_id>/<attempt>/` 保存每次 attempt 的 receipt、diff、驗證器輸出與用量，是完整的證據鏈。receipt 以 digest 綁定，被竄改就無法通過讀回驗證。

## 5. 警報與交付

引擎不附任何通知管道。需要警報時，讀取端以快照的 `stale`、`needs_human` 與 `dispatch_gate` 自行判斷並送出。

## 6. 驗收燈

- `lh_runtime/driver_heartbeat_canary.py`：heartbeat；
- `lh_runtime/status_snapshot_canary.py`：快照欄位與 stale 判定；
- `lh_runtime/mcp_canary.py`：唯讀介面。

## 與現行程式的差異

- 原設計包含 durable 資料的保留政策，以及推送到通訊軟體的警報。公開版兩者都沒有實作：store 與 artifact 目前不會自動清理，警報由讀取端負責。保留與工作區衛生列為後續任務（X9）。
- 原設計有人工抽查實戰紀錄的流程。公開版不含任何實戰紀錄。
