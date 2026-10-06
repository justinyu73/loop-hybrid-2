# 文件

三層結構（規則見 `contracts/entry-governance-v1.md`）：

- `contracts/`：長期契約，描述引擎現行的行為。修改需要核准，並由 `docs-contracts` gate 檢查。
- `active/`：進行中的 track，一個 track 一份。
- `archive/`：已完成的紀錄。

## 契約

| 文件 | 內容 |
|---|---|
| [goal-lifecycle-v1](contracts/goal-lifecycle-v1.md) | Goal 與 Run 的狀態、admission、delivery 綁定、execution fence、goal-scoped 授權 |
| [goal-hierarchy-v1](contracts/goal-hierarchy-v1.md) | work unit、平行波次、收據鏈與接續規則 |
| [model-routing-v1](contracts/model-routing-v1.md) | 宣告式 executor 與 capability 選路 |
| [candidate-review-v2](contracts/candidate-review-v2.md) | 候選覆核、有界返修、正常接續與 effect guard |
| [autonomous-driver-v1](contracts/autonomous-driver-v1.md) | 有界的自主 loop 與停止條件 |
| [command-ingress-v1](contracts/command-ingress-v1.md) | 指令入庫與狀態讀回 |
| [status-snapshot-v1](contracts/status-snapshot-v1.md) | 狀態快照欄位與 dispatch gate |
| [observability-v1](contracts/observability-v1.md) | 投影原則與證據位置 |
| [token-accounting-v1](contracts/token-accounting-v1.md) | 用量與成本 |
| [campaign-requirement-template](contracts/campaign-requirement-template.md) | 給目標專案用的 campaign 範本 |
| [entry-governance-v1](contracts/entry-governance-v1.md) | 入口與文件治理 |
| [planner-recovery-v1](contracts/planner-recovery-v1.md) | 失敗後的有界修復提案、獨立審核、預算與稽核 |
| [failure-routing-v1](contracts/failure-routing-v1.md) | 失敗後「下一步由誰做什麼」的固定選路表（投影） |
