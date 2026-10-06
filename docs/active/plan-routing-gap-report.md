# 差異報告：同 Goal 返修選路、封存的 plan admission、planner recovery

狀態：active（報告，不改程式）。日期：2026-10-06。

本報告比對原設計的三個部分與公開版的現行程式，逐項列出：已具備的、缺少的、行為不同的。每一項都附上「是否建議成為新任務包」。原設計只以概念描述，不引用其內部路徑或識別符。

比對的公開版程式：
- `lh_runtime/plan_node_controller.py`
- `lh_runtime/parallel_scheduler.py`
- `lh_runtime/work_unit_store.py`
- `lh_runtime/goal_loop_worker.py`
- `lh_runtime/project_binding.py`
- `lh_runtime/open_questions.py`

## 1. 同 Goal 返修選路（固定選路表）

**原設計**：失敗發生時，以一張封閉的「原因代碼 → 路由」表決定下一步，整個過程不呼叫模型：

- 每個機器原因代碼（檢查紅、驗證器紅、admission 綁定紅、base 漂移、語意範圍漂移、heartbeat 過期等）固定對應四件事：路由、負責修的節點、下一個動作、作廢的對象；
- 失敗證據與先前的收據一律保留；
- 只有一份封閉的「擁有者動作」白名單可以轉為人工處理，例如範圍擴大、破壞性作用、付費呼叫、發布、合併。機器原因代碼明確**禁止**轉人工；
- 同一處失敗達到門檻次數時，改走獨立的唯讀稽核；
- 不認得的代碼不猜測，一律路由到「修選路表本身」；
- 路由結果是可重算的收據；修復封包不可變，綁定以下內容：路由收據 digest、作廢與保留清單、允許路徑、必跑測試。

| 項目 | 公開版 | 判定 |
|---|---|---|
| 同一 run 的有界重試 | attempt 上限（`max_attempts`）；候選覆核 v2 的 RED finding 會回饋給下一次 attempt | 已具備 |
| 重複失敗改走唯讀稽核 | planner recovery 在失敗次數達 3 時要求唯讀稽核（`lh-planner-recovery-audit-binding/v1`、`lh-recovery-audit-result/v1`） | 已具備，但只在 recovery 路徑上 |
| 人工待辦的型別化 | `open_questions.py` 以封閉前綴表分類 | 已具備（只是投影） |
| 原因代碼 → 路由的固定表 | 沒有。失敗後要嘛是同一 run 的下一次 attempt，要嘛交給 planner（模型）判斷 | 缺少 |
| 機器原因不得轉人工 | 沒有這條約束。多處以 `human_required` 作為拒絕之後的終點，例如超出 `allowed_paths`、`source_refs_mutated_by_executor` | 行為不同 |
| 未知代碼的處理 | `open_questions.py` 把未知代碼歸為 `awaiting_owner`，交給人 | 行為不同 |
| 可重算的路由收據與不可變的修復封包 | recovery record（`lh-recovery-record/v1`）有 request、plan、verdict 的 digest 綁定，但沒有「路由收據」這一層 | 缺少 |

**建議：成為新任務包（固定選路表）。**
- 做成純函式：封閉的代碼表與擁有者白名單、門檻、可重算的收據。
- 先以投影形式接在既有失敗結果之上，不改變執行流程。
- 驗收：每個代碼的路由固定；未知代碼一律路由到修選路表；機器代碼不會轉人工；相同輸入得到相同收據。
- 是否讓路由真正驅動執行（例如自動重新具體化），等投影穩定後再另立任務。

## 2. 封存的 plan admission

**原設計**：計畫先封存（digest），之後不可修改。派工 admission 時重算 digest，並重新驗證整份計畫的形狀：
- 圖沒有環，節點與邊等於宣告的集合；
- 只有列為「可派工」的節點能被 admit；
- 平行群組之間，write/write、write/read、read/write 路徑都不重疊；
- 每個工作單位的 worker、worktree、state root、lease 都唯一；
- 沒有未展開的佔位符；
- 派工前的 verifier 必須是唯讀且 GREEN；
- 派工封包必須帶同一個 plan digest。

| 項目 | 公開版 | 判定 |
|---|---|---|
| 單一 plan 節點的冪等收據 | `plan_node_controller.py`：planner 產出封存的計畫 → 獨立的唯讀 verifier（identity 必須不同、不得寫入 source）→ queue projection（不得建立 run、attempt，也不得呼叫 provider）→ 寫入收據。相同輸入重播時讀回收據，不會再呼叫 planner；輸入 digest 漂移時拒絕 | 已具備 |
| DAG、依賴與路徑分割 | `parallel_scheduler.py` 與 `work_unit_store.py`：環偵測、依賴整合後才放行、所選子單位必須使用 parent base、write/write 與 write/read 衝突檢查、每個 wave 最多 3 個、重啟後不會重複建立 run | 已具備 |
| 計畫內容的形狀檢查 | plan 節點把這些檢查全部交給注入的 verifier callback；引擎本身不檢查計畫的形狀（環、唯一性、佔位符、可派工清單） | 缺少 → **已由 X17 補上**（宣告 `lh-sealed-plan/v1` 的計畫） |
| base 漂移 | 回報 `base_mismatch`，不派工；原設計會作廢該 wave 的收據並重新具體化 | 行為不同 |
| 宿主時期的殘留命名 | `plan_node_controller.py` 有以下殘留：預設節點名稱 `P3B`、`R0`；拒絕位於 `LH_HOST_STATE_ROOT`（預設 `~/.local/state/external-host`）之下的 state root；一段引用私有 PR 編號的註解；schema 名稱為 `host-plan-node-controller/v1`。這些不影響行為，但不符合純 LOOP 的命名 | 行為不同（殘留）→ **已由 X17 清理**（節點代號、註解、schema 改名；正式狀態目錄防護的命名仍在） |

**建議：**
- **成為新任務包（計畫形狀的引擎內驗證）。** 把無環、唯一性、路徑分割、佔位符、可派工節點這幾項做成引擎內的純函式，plan 節點在呼叫 verifier 之前先跑；verifier 仍是獨立的第二層。
- **殘留命名的清理，併入同一包。** schema 改名必須保留讀取舊名稱的能力，並在契約中標明。
- **base 漂移的自動重新具體化：不建議現在做。** 它需要 wave 層級的協調與 lease 世代，屬於控制平面；公開版目前的做法是停下並回報，這是安全的預設。

## 3. Planner recovery 契約

**原設計**：選路表先行，只有需要判斷的代碼（例如語意範圍漂移）才交給 planner 做同 Goal 的有界修復。

| 項目 | 公開版 | 判定 |
|---|---|---|
| contract 中的 recovery 綁定 | `project_binding.py` 以 contract digest 封存 `planner_recovery` 區塊；原生執行時由 runner 綁定 | 已具備 |
| 封閉的動作種類 | `resume_phase`、`repair_same_node`、`retry_within_budget`、`collect_evidence`、`request_authority`、`insufficient_evidence`、`dispatch_successor` | 已具備 |
| 產出者與驗證者分離 | 兩者的 principal 必須不同；verifier 必須唯讀、不得寫入 source；plan digest 重算後必須一致；決策依據必須引用原因代碼與證據 | 已具備 |
| 預算與稽核 | recovery 預算上限；失敗次數達 3 時要求唯讀稽核 | 已具備 |
| 觸發來源 | campaign 的連續失敗（stop line）與 work-unit 的 recovery 請求；單次失敗不會觸發 | 行為不同 |
| 誰先決定 | planner（模型）直接決定動作，沒有 deterministic 的選路先行 | 行為不同 |
| 契約文件 | `docs/contracts/` 中沒有 planner recovery 的契約，行為只寫在程式裡 | 缺少 |

**建議：**
- **成為新任務包（planner recovery 契約文件），只有文件。** 依現行程式寫出觸發、動作種類、身分分離、預算、稽核與拒絕代碼，並納入 `docs-contracts` 的檢查清單。
- **「選路先行、planner 只處理指派給它的代碼」：等固定選路表的投影穩定後，再評估是否串接。** 現在先不改 recovery 的觸發方式。

## 4. 建議的任務包

| 建議 | 內容 | 規模 | 優先 |
|---|---|---|---|
| 固定選路表 | 純函式的代碼表、擁有者白名單、門檻與可重算收據，先以投影形式提供（**已由 X16 完成**） | 中 | 高 |
| 計畫形狀的引擎內驗證 | plan 節點在呼叫 verifier 前先做形狀檢查；同時清理殘留命名（**已由 X17 完成**） | 中 | 中 |
| planner recovery 契約文件 | 依現行程式寫契約，並納入 `docs-contracts`（**已由 X18 完成**） | 小 | 中 |

不建議納入公開版的項目：
- base 漂移後由協調者重新具體化 wave；
- coordination lease 的世代復原；
- 依宿主分支數量的保護規則。

這些都依賴控制平面或宿主機制。

## 與現行程式的差異

本報告即為差異的清單；以上各表中標為「缺少」與「行為不同」的項目，就是公開版與原設計的差異。
