# 用量與成本計算 v1

狀態：active。本文件描述用量如何從 executor 流到 receipt、快照與 dispatch gate。依據為 `lh_runtime/token_cost.py`、`lh_runtime/cli_agent_executor.py`、`lh_runtime/usage_void.py`、`lh_runtime/budget_reducer.py` 與 `lh_runtime/dispatch_gate.py`。

## 1. 資料流

```text
executor stdout 最後一行（lh-usage-line/v1）或 none
  -> usage record（measured / unknown）寫進 attempt receipt
  -> RunStore.usage_records()
  -> token_cost.aggregate(..., pricing=宣告的 pricing)
  -> 快照的 cost、dispatch gate 的每日上限、driver 的 token 預算
```

- 宣告 `usage: lh-usage-line/v1` 的 executor，在 stdout 最後一行印出 `{"usage": {"input_tokens": ..., "output_tokens": ..., "cache_read_tokens": ...}}`，即為 measured。
- 宣告 `usage: none`，或輸出不符合格式，即為 unknown，並附上原因。

## 2. 誠實規則

1. **unknown 永遠不會變成 0。** 彙總時，measured 與 unknown 分開計數；只要有 unknown，`cost_complete` 就是 false。
2. **成本只來自宣告的費率。** contract 的 `pricing` 以「每百萬 token 美元」宣告各 model 的 `input`、`output`、`cache_read`；沒有宣告的 model 成本為 unknown。引擎不附任何價目表。
3. **不猜數字。** 引擎不解析任何廠商的輸出格式。
4. **被作廢的 attempt 不計。** `lh_runtime/usage_void.py` 會把被取代的 attempt 用量標記為作廢，不重複計算。

## 3. 預算

- driver 的 token 預算以 `--budget-ceiling-tokens` 注入：
  - 用量達到上限時以 `budget_exhausted` 停止；
  - 有 unknown 用量時以 `budget_unknown` 停止，因為無法確定是否已超出。
- dispatch gate 的每日成本上限只使用宣告的費率；當天沒有可計價的用量時，不會觸發。

## 4. 驗收燈

- `lh_runtime/token_accounting_canary.py`：資料流與誠實規則；
- `lh_runtime/usage_delta_canary.py`：用量差額；
- `lh_runtime/usage_void_canary.py`：作廢的 attempt 不計。

用真實 executor 校準用量輸出，屬於人工實測。

## 與現行程式的差異

- 原設計附有兩個廠商 CLI 的用量解析器與內建價目表。公開版全部移除，只接受中立的用量行與宣告的 pricing。
