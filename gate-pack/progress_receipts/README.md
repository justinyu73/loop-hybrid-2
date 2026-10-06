# 進度收據與驗證佇列

給目標 repo 用的工具：**進度是一張收據，不是一句宣稱**。agent 回報的進度是它對自己的描述；收據只在檢查真的跑過之後才會存在。

本工具只用 Python 標準函式庫與 git。

## 角色

```text
請求方（寫請求）  ->  [ 佇列 ]  ->  驗證方（執行檢查、寫收據）
```

- 請求方只能指名檢查 id，不能夾帶指令。
- 驗證方從自己持有的登記表查出指令，在釘住 HEAD 的快照中執行，並把 commit 與登記表 digest 寫進收據。
- 收據寫進 append-only 的雜湊鏈 ledger。

## 檢查登記表（驗證方持有）

```json
{"schema": "lh-checks-registry/v1",
 "checks": {"unit-tests": {"argv": ["python3", "-m", "unittest"], "expect_exit": 0}}}
```

## 指令

```bash
# 請求方
python3 gate-pack/progress_receipts/receipts.py request --queue Q --task T --session S --check unit-tests --nonce N
# 驗證方（由外部排程器呼叫；一次處理完所有待處理請求）
python3 gate-pack/progress_receipts/receipts.py serve --queue Q --registry checks.json --repo REPO --ledger L
# 任何人
python3 gate-pack/progress_receipts/receipts.py verdict --ledger L --task T --check unit-tests --nonce N

# 驗證、驗收、推廣分開記錄
python3 gate-pack/progress_receipts/timeline.py record  --timeline F --task T --kind verification --ref <receipt>
python3 gate-pack/progress_receipts/timeline.py record  --timeline F --task T --kind acceptance   --ref <verification event_id>
python3 gate-pack/progress_receipts/timeline.py record  --timeline F --task T --kind promotion    --ref <acceptance event_id>
python3 gate-pack/progress_receipts/timeline.py project --timeline F --task T
```

| 判定 | 意義 |
|---|---|
| `pass` | 雜湊鏈完整，最新的符合收據 `ok: true` |
| `no_receipt` | 沒有收據，所以不算進度 |
| `check_failed` | 有收據，但檢查失敗 |
| `chain_broken` | ledger 被改寫 |

## 限制

兩個角色要真正分開，必須以不同的作業系統身分執行，讓請求方無法寫入 ledger、登記表與驗證方的工作目錄。這是部署層的事實，程式無法自行保證。所以每次 `verdict` 都會量測呼叫者能否寫入 ledger，並回報在 `principal_separated` 與 `cannot_claim` 中。
