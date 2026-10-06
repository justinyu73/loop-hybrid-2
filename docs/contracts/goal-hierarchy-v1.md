# Goal 階層與平行 work unit v1

狀態：active。本文件描述父 Goal 下的 work unit、平行波次、依賴與 task area 的接續規則，依據為 `lh_runtime/work_unit_store.py`、`lh_runtime/parallel_scheduler.py`、`lh_runtime/plan_node_controller.py`、`lh_runtime/task_area.py` 與 `lh_runtime/work_unit_completion.py`。

## 1. 名詞

| 名詞 | 意義 |
|---|---|
| 父 Goal | 不可變的上層目標；work unit 只能在它的 revision 之下建立 |
| work unit | 一個有界的節點：一份 packet、一份 delivery contract、一份 completion contract |
| dispatch envelope | 一次派工的不可變身分（`lh-successor-dispatch-envelope/v1`），以 `dispatch_key` 去重 |
| Attempt | 同一個 work unit 的一次執行；重試是同一個 run 上的新 attempt |
| task area | 已核准的 manifest（`lh-task-area/v1`）。列出 work unit、依賴、讀寫範圍與核准狀態 |

work unit 的狀態以 `WORK_UNIT_STATES` 為準：`pending`、`ready`、`running`、`retry_pending`、`verified`、`integrated`、`stopped`。

## 2. Manifest 與核准

task area manifest 必須同時通過：
- **plan verifier**：principal 不得是 planner；`read_only` 為 true，`source_write` 為 false；verdict GREEN；綁定 manifest body digest。
- **approval**：綁定整份 manifest 的 digest。

manifest 中只有 `approved` 的 task 會被 admission；`pending` 與 `deferred` 不會派工。改動 manifest 的任何內容都會讓核准失效。

## 3. 平行波次

依賴與寫入範圍相容的 work unit 可以並行，上限是 3 個 worker。
- 寫入範圍重疊的 work unit 依序執行；
- 依賴環路一律拒絕；
- 一個 work unit 在等待、失敗或結果未知時，只阻擋它自己和依賴它的後繼，其他已核准且無衝突的工作照常進行。

## 4. Completion 與收據鏈

`WorkUnitCompletionController` 依序推進以下 phase：candidate → checks → verifier → integration → integration_checks → integration_verifier → delivery_verifier → machine_complete。

- 每一個 phase 都以 `(run, attempt, fence, phase)` 身分 claim 並 settle；
- receipt digest 由內容計算；
- `machine_complete` 引用前面每一段的 receipt digest；
- 只有 Store 中的 phase 狀態能釋放依賴，callback 回報的 GREEN 不算。

## 5. 接續規則

- **舊任務：** 前一個 work unit integrated 之後，後繼必須等 recovery（Planner）port 寫出 `dispatch_successor` 的交接，才會被放行。
- **採用候選覆核 v2 的任務：** 引擎從原 Store 重讀前一個任務的整條收據鏈並重驗，通過後依規則放行已核准的後繼，不呼叫 Planner。詳見 `candidate-review-v2.md`。

以下情況仍交給原本的 recovery port：RED 結果、未結的 recovery 請求，以及後繼為舊任務。

## 6. Completion rollup

父 Goal 的完成狀態由 work unit 的 Store 狀態推導，不另存一份冗餘狀態。任何 work unit 未 integrated，父 Goal 就沒有完成。

## 與現行程式的差異

- 原設計記錄了宿主上的 preflight 欄位與階層落地步驟。公開版只保留引擎內的資料模型與規則。
- 原設計的「轉折點受限裁量」在公開版由 `lh_runtime/turning_point.py` 實作：判斷器只能從封閉選項中選擇（`select`、`parent_done`、`human_required`），越界或異常時退回決定性選路。
- 原設計由宿主喚醒 task area。公開版不附宿主，`TaskAreaController.tick` 由呼叫者觸發。
