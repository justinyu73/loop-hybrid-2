# Goal 生命週期 v1

狀態：active。本文件描述引擎中 Goal 與 Run 的生命週期邊界，依據為 `lh_runtime/goal_store.py`、`lh_runtime/run_store.py`、`lh_runtime/admission_bridge.py`、`lh_runtime/controller.py` 與 `lh_runtime/workspace_port.py`。

## 1. 狀態與轉移

`event_received` 是持久化輸入事件的狀態；其餘都是 Goal 狀態（`GOAL_STATES`）。

| 從 | 條件 | 到 |
|---|---|---|
| `event_received` | 相同的穩定 key 已存在 | 回傳既有結果，不新建任何東西 |
| `event_received` | 有界、無歧義的新工作 | `candidate` |
| `event_received` | 多重符合、版本過舊或範圍不明 | `conflict`、`stale` 或 `human_required` |
| `candidate` | envelope 核准、預算、範圍與決定性驗收燈都通過 | `active` |
| `candidate` | 只能人做的 stage、缺燈、範圍擴大或超出核准 envelope | `human_required` |
| `active` | 驗收燈綠 | `completed`，並發出一個 stage 完成事件 |
| `active` | 更新的 revision 取代本 revision | `stale` |
| `active` | 重試預算耗盡或明確停止 | `stopped` |
| `completed` | 新的指令（例如每日 standing intent） | 以新 revision 重新成為 `candidate` |
| `stale`／`conflict` | 不自動恢復 | `human_required` |
| `human_required` | 人處理後 | `candidate`、`active` 或 `stopped` |

允許的轉移以 `ALLOWED_TRANSITIONS` 為準。同一個 `event_id`（或 idempotency key）重送時去重；若 key 相同但來源、事件類型或內容不同，一律拒絕。

## 2. Admission envelope

envelope（`lh-campaign-admission-envelope/v1`）必須指明：
- `campaign_id` 與 `stage_id`；
- 允許的路徑與副作用；
- 驗收燈；
- attempt 上限；
- 下一階段規則；
- 是否只能由人執行。

只有已核准、具 committed 且可重跑驗收燈的決定性 stage 能自動 admission。範圍擴大、品質判斷未定、缺驗證、新的憑證、對外發布，或超出 Goal 授權的權限變更，都轉為 `human_required`。授權內的一般 stage 轉移不需要人。

## 3. 目標與執行邊界

- **目標專案擁有：** repository、campaign 範圍、分支、推廣政策與產品終驗。
- **引擎擁有：** durable 的 Goal 狀態，以及釘在 `base_revision` 的一次性 clone。

一次性 clone 建立後，引擎會封閉它的對外 push：
- 所有 remote 的 push URL 都設為不可用；
- 安裝一律拒絕的 `pre-push` hook；
- executor 執行前後比對 source repo 的 refs。若 refs 被改動（例如以 git 的 no-verify 選項繞過 hook），該次 attempt 轉為 `human_required`，原因為 `source_refs_mutated_by_executor`。

注意：繞過 hook 的情況是偵測後拒絕驗收，不是事前阻止；在沒有隔離 backend 時，這是能做到的最強保證。

### 工作區的建立（WorkspacePort）

controller 經由 `WorkspacePort` 建立每次 attempt 的工作區。預設的 `GitCloneWorkspace` 就是上述的一次性 clone：
- `git clone --no-local`；
- 以 detached 狀態 checkout 到 base；
- 封閉對外 push。

使用者可以注入其他 backend，例如快照或共享物件的實作，但必須遵守以下保證。`workspace_port.check_backend`（或 `python3 lh_runtime/workspace_port.py --backend module:Class`）會逐項檢查：
- 工作區位於 workspace root 之內，並以 detached 狀態停在 base；
- 經由既有的 remote（即使繞過 hook），或經由 executor 新增的 remote 進行 push，都不會改動 source 的 refs；
- 在工作區建立的分支與 tag，不會出現在 source；
- 損壞工作區的物件庫，不會損壞 source；
- 位於較大 checkout 中的目標，會從相同的子目錄執行。

不論使用哪一種 backend，controller 都會拒絕位於 workspace root 之外的工作區（`workspace_outside_root`）。像 `git worktree` 這類與 source 共享 refs 與物件的作法，無法通過上述檢查。

引擎不寫入目標的工作樹，只會：
- 保存 artifact 與 receipt；
- 在一次性 clone 中提交；
- 經由注入的 external action port 產生外部作用。run 完成後的作用必須經過 effect guard（見 `candidate-review-v2.md`）。

## 4. Goal 與 Run

- 每個邏輯 Goal 有不可變的 revision。
- 已 admission 的 revision 至多有一個 queued 或 running 的 Run；重試是同一個 `run_id` 上的新 attempt。
- 較新的 revision 取代舊的，必須重新 admission 後才會有新的 Run。
- Run 的狀態以 `RUN_STATES` 為準：`queued`、`running`、`retry_pending`、`verified`、`stopped`、`human_required`、`awaiting_external_verdict`。

## 5. Delivery 綁定

每個 Run 都必須有 delivery 綁定（`host-delivery-unit-contract/v1`）：
- 規劃、執行、驗證共用同一個封存的契約引擎 `lh_runtime/delivery_contract.py`；
- source 與 final 兩個階段的 delivery 都必須由獨立驗證器重驗；
- operator contract 的 stage 以 `delivery.derive = acceptance_lamp` 從自己的驗收燈編出綁定；
- 驗證器若位於 `allowed_paths` 之內，載入時即拒絕。

## 6. Execution fence

每個可能產生變更的 adapter 與子程序，都必須先通過 `ExecutionFencePort`：
- `prepare(binding)` 回傳一份不可變的 launch descriptor（`lh-execution-fence-launch/v1`），或 `execution_fence_unavailable`；
- descriptor 綁定 Goal revision、Run、Attempt、base revision、clone 根目錄、允許的寫入範圍與到期時間；
- descriptor 只能啟動一次，digest 被改即拒絕。

backend 由 `LH_EXECUTION_FENCE_BACKEND` 明確選擇：
- 未設定時停用，run 停在 `human_required`，不會無 fence 執行；
- 內建的 `local-process` 管理程序群組、逾時與輸出上限，並在 receipt 標示 `kernel_containment: false`；
- 真正的隔離 backend 由使用者以 port 提供。

## 7. Goal-scoped 授權

人保有四項 Goal 層級的決定：
1. 預期的產品結果；
2. admission 與授權 envelope；
3. 超出 envelope 的方向改變；
4. 有意義的階段或 Goal 邊界上的終驗。

人不是每個決定性節點之間的必要關卡。在核准的 envelope 內，引擎可以：
- 推導下一個節點；
- 執行、驗證、重試；
- 依規則接續已核准的後繼（見 `goal-hierarchy-v1.md`）。

引擎必須在以下情況主動停下：
- 證據變成未知；
- 失敗超出修復預算；
- 出現方向衝突；
- 要求的動作會擴大範圍、憑證、權限、安全上限或發布影響。

機器完成只是給人審閱的證據，不會自行宣告終驗。發布、release 與產品驗收不在授權之內。

## 與現行程式的差異

- 先前版本的工作區建立方式寫死在 controller 中，現在改為 `WorkspacePort`，預設實作的行為不變，只附預設實作與符合性檢查，不附其他 backend。

- 原設計包含代管服務的 draft PR、自動合併與信任爬坡。公開版沒有任何服務 adapter：外部作用只經注入的 port，run 完成後的作用須經 effect guard；帶 `external_verdict` 區塊的 contract 會被拒絕。
- 原設計的 execution fence 附有 kernel 沙箱 backend，並要求兩項 kernel proof。公開版只附 `local-process`，不提供隔離，proof 一律記為 `not_contained`。
- 原設計中「單一 active Goal 符合時沿用既有 Run」的事件綁定，屬於宿主路由。公開版由 `lh_runtime/goal_matcher.py` 與 admission 依事件內容決定。
