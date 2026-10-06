# 推進判定（advancement）

給目標 repo 用的工具：**已經綠的檢查不算推進**。

只問「指定的檢查有沒有通過」的話，一個什麼都沒改的 successor 也能拿到真的收據，被算成有進展。這個工具要求在評估之前就固定好要比較的兩張收據，並只比較它們。

收據來自 `gate-pack/progress_receipts/`（見該目錄的說明）。

## 指派

```json
{"schema": "lh-advancement-assignment/v1", "task": "T1",
 "criteria": [{"id": "A", "check": "feature-a",
               "baseline_receipt": "sha256:...", "closing_receipt": "sha256:..."}]}
```

## 指令

```bash
python3 gate-pack/advancement/advancement.py evaluate --ledger <receipts ledger> --assignment assignment.json
```

| 每個判準的判定 | 意義 |
|---|---|
| `advanced` | baseline 紅、closing 綠 |
| `already_green` | baseline 就已經綠——不算推進 |
| `still_red` | 兩張都紅 |
| `regressed` | baseline 綠、closing 紅 |

只有每個判準都是 `advanced`，結果才是 `advanced`（exit 0）。以下情況一律拒絕：
- 收據不在 ledger 中（`receipt_unknown`）；
- 收據的 task 或檢查與指派不符（`receipt_check_mismatch`）；
- baseline 沒有早於 closing（`receipt_order_invalid`）；
- ledger 被改寫（`chain_broken`）。
