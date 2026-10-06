# 可觀測性 v1

狀態：active。本文件描述引擎對外呈現狀態的原則。依據為 `lh_runtime/status_snapshot.py`、`lh_runtime/project_status.py`、`lh_runtime/mcp_server.py`、`lh_runtime/knowledge_store.py` 與 `lh_runtime/retention.py`。

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

### 保存與工作區衛生

引擎在 store 根目錄下會累積自己產生的暫存：`completion-snapshots/`（驗證快照，內含一次性 clone）、`trusted-launches/`（啟動暫存）、`completion-check-diagnostics/`（失敗的檢查輸出）、`candidate-recovery-*`（修復副本）與 `candidate-reviews/`（以內容 digest 命名的 review proof）。

`lh_runtime/retention.py --root <store>` 預設只輸出計畫（`lh-retention-plan/v1`），加上 `--apply` 才刪除計畫中的項目。一個項目必須同時滿足以下條件才會被刪：

- 未被參照：它的名稱（或 review proof 的 digest）不出現在 store 下任何 SQLite 文字欄位或 JSON 檔中；有讀不了的證據時，一律不刪；
- 超過寬限期（`--grace-hours`，預設 24 小時），以項目內最新的修改時間計算；
- 不是該類最新的一筆。暫存名稱不帶 run 身分，所以「最新」以類別計，而不是以 run 計；
- 是 store 根目錄內的真實目錄或檔案，內部沒有任何 symlink 或 junction，本身也不是 repository 根目錄（直接含 `.git`）。驗證快照中的一次性 clone 位在項目下一層，屬於引擎暫存，可以刪除。

`--apply` 刪除前會重新檢查連結與位置；沒刪成的項目寫入 `errors`，此時結束碼為 1。`artifacts/`、`loop.sqlite3` 等證據本身不在清理範圍內。

## 5. 警報與交付

引擎不附任何通知管道。需要警報時，讀取端以快照的 `lamp`（`lh_runtime/status_lamp.py` 的唯一健康判定，附觸發的規則）決定是否送出，不要從其他欄位自行推導。

## 6. 驗收燈

- `lh_runtime/driver_heartbeat_canary.py`：heartbeat；
- `lh_runtime/status_snapshot_canary.py`：快照欄位與 stale 判定；
- `lh_runtime/mcp_canary.py`：唯讀介面；
- `lh_runtime/retention_canary.py`：保存與工作區衛生；
- `lh_runtime/status_trust_canary.py`：code identity 與健康燈。

## 與現行程式的差異

- 原設計包含 durable 資料的保留政策，以及推送到通訊軟體的警報。公開版只實作了引擎暫存的清理（第 4 節，需手動執行，不會自動排程）；receipt、artifact 與 store 本身的保留期限沒有實作，警報由讀取端負責。
- 原設計有人工抽查實戰紀錄的流程。公開版不含任何實戰紀錄。
