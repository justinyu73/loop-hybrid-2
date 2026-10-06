# 獨立的重試驗證器

執行器失敗、被重試、在啟動上限內停下，這些證據不應由寫入它的同一份程式來判讀。這個驗證器是另一份實作：
- 不 import 引擎的 executor、store 或 scheduler 模組；
- 只把 executor 以 digest 綁定的收據當 JSON 讀取；
- 以唯讀方式開啟 work-unit store 的 SQLite；
- 核對兩邊的說法是否一致。

全程不寫入任何證據。

## 指令

```bash
python3 gate-pack/retry_verifier/verify_retry.py \
  --executor-root <executor root> --queue-db <store>/work-units.sqlite3 \
  --dispatch-key <key> --expected retry_success|exhausted|unknown
```

| 預期狀態 | 必須成立 |
|---|---|
| `retry_success` | 恰好一張接受收據；failure 收據數量等於啟動次數減一；較早的 attempt 都是 `interrupted`；最新的 attempt 與 run、work unit 狀態一致 |
| `exhausted` | failure 收據數量等於啟動上限（3）；沒有接受收據；run、attempt、work unit 都是 `stopped` |
| `unknown` | 恰好一張 recovery 收據；沒有 failure 或接受收據 |

此外，以下情況一律 FAIL 並附原因：
- 任何收據的 digest 不符；
- 同一個 attempt 有重複的啟動紀錄；
- 啟動次數超過上限；
- attempt 數量與啟動紀錄不符；
- dispatch 收據中的失敗歷史與 failure 收據不一致。
