# 候選覆核 v2、正常接續與 effect guard

狀態：active。本文件描述「驗證器必須審查，而不只是 exit 0」這條鏈路，以及它如何延伸到接續與外部作用。依據為：
- `lh_runtime/delivery_contract.py`、`lh_runtime/run_store.py`；
- `lh_runtime/work_unit_completion.py`、`lh_runtime/runner_adapter.py`、`lh_runtime/cli_agent_executor.py`；
- `lh_runtime/task_area.py`、`lh_runtime/effect_guard.py`。

## 1. Policy、context 與 result

delivery contract 可以加上 `candidate_review`。三個 schema 都是封閉的：

| schema | 內容 |
|---|---|
| `lh-candidate-review-contract/v2` | 已核准的 spec、需求（1～128 條）與呼叫端上下文，全部以絕對路徑與 digest 釘住 |
| `lh-candidate-review-context/v2` | 綁定這一次的 candidate digest、base、checks digest、scope、身分與 spec 實際內容；context digest 由內容計算 |
| `lh-candidate-review-result/v2` | 逐條回答每個需求，加上 scope 判定與 findings；四個綁定欄位必須與 context 完全相同 |

- 判定由引擎依 findings 推導，不採信驗證器自報；RED 的需求必須有阻擋 finding。
- review 原始 bytes 以 digest 封存在 store 下的 `candidate-reviews/`，讀回 delivery 時重驗。
- 改動 spec 會讓 contract 綁定失效。

## 2. 兩條路徑

- **RunStore 路徑：** 驗證器經 stdin 收到 context，必須在 stdout 回傳 `{"verdict": "GREEN", "review": {...}}`。exit 0 但沒有合格 review 時為 RED，不會觸及外部 action port。
- **work-unit 路徑：** 候選 receipt 之後，先跑 packet 的 targeted commands（相關 checks，`lh-candidate-review-related-checks/v2`），再做 review，最後才跑完整 checks。相關 checks 為 RED 時不呼叫驗證器。

## 3. 有界返修

review 判 RED 時，完整的 review 與 raw ref 會隨下一次 attempt 的 `completion_repair` 回饋給 executor，次數受 attempt 上限約束，不會無限迴圈。

在 trusted 模式下：
- verifier 使用 `lh-verifier-result/v2`；
- 有 review context 時只接受 v2；
- 被竄改的 review 不會回饋給 executor。

## 4. 建議與 admission

非阻擋 finding 以 `review_optimization` 寫入 discovery（`lh-discovery-candidate/v1`），並依 candidate id 去重。它們不會被核准，也不會被派工。要成為任務，必須寫進新的、已核准的 manifest。

## 5. 正常接續

task area 中採用 review v2 的任務：
1. 前一個任務 integrated 之後，引擎從原 Store 重讀並驗證整條收據鏈：各段 settled、digest 自洽、身分一致、machine_complete 引用各段、verifier 不是 executor、review proof 重驗通過。
2. 通過後，依規則放行 manifest 中已核准的後繼，不呼叫 Planner。
3. RED 結果、未結的 recovery 請求與舊任務，仍交給原本的 recovery port。

## 6. Effect guard

run 完成後的外部作用（合併、發布、部署），經 `effect_guard.guarded_dispatch` 送出時：
- 目前 attempt 的 final delivery 必須為 GREEN；
- diff 不得碰到 authority surface（`lh_runtime/authority_surface.py`）；
- 帶 review v2 的 contract 不因此取得作用授權，必須另附 `lh-effect-grant/v1`，且綁定 effect、run_id 與 contract digest；
- `expected.base` 必須是 run 的 base revision；
- 作用前讀回目標身分，執行注入方的等待後再讀一次；
- 先記錄 prepared 再送出；回應遺失時只讀回確認，絕不重送；
- 完成後以確定的 key 去重。

## 與現行程式的差異

- 原設計在代管服務的 PR 與 CI 之後才合併，也有自動合併政策。公開版不附任何服務 adapter，effect guard 只驗證前置條件與綁定；grant 如何產生，屬於注入方的授權流程。
- 原設計中把 task area 喚醒交給宿主。公開版由呼叫者觸發 `TaskAreaController.tick`。
- operator contract 的 `delivery.derive` 路徑上，驗證器就是驗收燈，目前不會產生 review。要讓這條路徑也做 review，需要另一個設計。
