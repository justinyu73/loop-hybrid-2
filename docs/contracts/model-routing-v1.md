# 模型選路 v1

狀態：active。本文件描述引擎如何決定「由哪個宣告的 executor 執行哪一種工作」，依據為 `lh_runtime/cli_agent_executor.py`、`lh_runtime/capability_resolver.py`、`lh_runtime/provider_registry.py`、`lh_runtime/provider_input_binding.py`、`lh_runtime/adaptive_routing.py` 與 `lh_runtime/turning_point.py`。

## 1. 決策

- 專案節點不指定 provider 或 model，只描述需要的 capability。
- 引擎會執行的每一個指令，都必須先在 contract 的 `executors` 區塊宣告。宣告是封閉的資料，不是程式碼。
- 引擎從不在 `PATH` 上搜尋 provider，也不內建任何廠商名稱或價目表。

## 2. 宣告式 executor

```json
"executors": {
  "coder": {"argv": ["/absolute/path/to/your-coding-agent", "{prompt}"], "usage": "none"}
}
```

- `argv[0]` 必須是絕對路徑，`{prompt}` 恰好出現一次。
- `{model}` 與 `{base_url}` 是可選插槽：有插槽就必須有值，有值也必須有插槽，否則拒絕。
- `usage` 只有兩種：
  - `none`：用量記為 unknown；
  - `lh-usage-line/v1`：executor 在 stdout 最後一行印出 `{"usage": {...}}`。
- 非零退出就是該次 attempt 失敗。
- 不帶 contract 時，以 `--executors` 傳入宣告檔，以 `--executor` 指定名稱。

## 3. 分離的角色

| contract 欄位 | 角色 | 說明 |
|---|---|---|
| `models.execute` | 執行（produce_change） | 宣告的 coding executor |
| `models.execute_binding` | provider binding | 填入 `{model}`／`{base_url}` 插槽（runner、base_url、model） |
| `models.judge` | 判斷（evaluate_transition） | 宣告的推理 executor；不設時走純決定性選路 |
| `models.judge_model` | 判斷用的 model | 填入 judge 宣告的 `{model}` 插槽 |

不帶 contract 時，judge 由 `--judge-executor` 與 `--judge-model` 指定。執行與判斷可以是不同的宣告，各自計價。

## 4. Capability 選路

`capability_resolver` 以 `lh-capability-routing/v1` 描述 resource 與需求：
- 兩種 operation：`produce_change`、`evaluate_transition`；
- 檔案系統權限：`read_only`、`workspace_write`；
- 網路：`none`、`allowlisted`、`external`；
- 資料區域：`local`、`approved_region`、`external`；
- 獨立性：`none`、`context`、`model_family`。

resolver 只選出同時滿足所有需求的 resource，並在 attempt 綁定（`lh-attempt-binding/v1`）中封存選擇結果。沒有合格的 resource 時，依 fallback 政策處理：`stop`、`next_eligible` 或 `human_required`。

## 5. Provider 輸入綁定

送給 provider 的輸入，會在啟動前做投影與證明（`lh-provider-input-binding/v1`）：prompt、指令範本、環境投影與 launch descriptor 的 digest 必須一致。nonce 只能使用一次。

## 6. 自適應投影

`adaptive_routing` 從過往的 attempt 觀察中，產生只讀的選路投影（`lh-routing-adaptive-projection/v1`）。投影只能在已核准的候選之間調整順序，不能新增 resource、放寬權限或越過 fallback 政策。

## 7. 自動判斷的封閉選項

轉折點判斷（`lh-turning-point/v1`）只能在封閉選項中選擇：`select`（選定一條 runnable）、`parent_done`、`human_required`。輸出不合法、越出選項集合或判斷器失敗時，退回決定性選路，不會讓模型自由決定下一步。

## 與現行程式的差異

- 原設計中的執行宿主與 bootstrap 綁定、宿主介面政策屬於宿主層。公開版保留中立的介面（例如 `host-provider-registry/v1`），但不附任何宿主實作。
- 原設計列出具體的廠商 executor 預設與用量解析器。公開版全部移除，改為宣告式 executor 與中立的用量協定。
- 原設計的相容模式會沿用舊的 provider 名稱。公開版不認得任何 provider 名稱，未宣告的名稱一律拒絕。
