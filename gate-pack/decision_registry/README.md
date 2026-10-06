# 決策登記（decision registry）

給目標 repo 用的工具：**考卷先於答案**。決策在工作開始前登記，附上驗收探針與允許改動的路徑；git 歷史會被拿來比對，找出沒有登記的改動。

本工具只用 Python 標準函式庫與 git。

## 設定

在目標 repo 建立 `decisions/policy.json`：

```json
{"schema": "lh-decision-policy/v1", "guarded_prefixes": ["deploy/", "governance/"]}
```

碰到這些前綴的 commit，就必須引用一個已登記的決策。

## 指令

```bash
python3 gate-pack/decision_registry/registry.py register --root <repo> --row row.json
python3 gate-pack/decision_registry/registry.py verify   --root <repo>
python3 gate-pack/decision_registry/registry.py orphans  --root <repo> --range <base>..HEAD
python3 gate-pack/decision_registry/registry.py readback --root <repo> --decision <ID> --range <base>..HEAD
```

`row.json` 範例：

```json
{
  "decision_id": "DEC-001",
  "question": "how should the deploy config change",
  "candidates": ["keep", "change"],
  "acceptance": [{"id": "config", "argv": ["python3", "checks/config.py"], "expect_exit": 0}],
  "surface": ["deploy/"],
  "artefacts": ["docs/brief.md"]
}
```

| 指令 | 作用 |
|---|---|
| `register` | 附加一列到 `decisions/registrations.jsonl`（雜湊鏈）。沒有探針、沒有 `surface`（可以是空清單）、候選少於 2 個、重複的 id、未知的 `supersedes`、artefact 不存在：一律拒絕 |
| `verify` | 檢查 ledger 未被改寫，且有效決策綁定的 artefact digest 仍與磁碟相符。要改 artefact，請以新的一列 `supersedes` 舊決策 |
| `orphans` | 逐 commit 判定：碰到受保護前綴的 commit，其 subject（或 squash 合併 body 中以 `* ` 開頭的行）必須含有該 commit 自身樹中已登記的 id。後面的 commit 洗不掉前面的孤兒 |
| `readback` | 引用該 id 的 commit 只能改動登記的 `surface`（預設拒絕）；並重跑探針，推導 `passing` 或 `failing`，結果不寫回 |

輸出都是 JSON；有任何紅時以非零碼退出，可以直接接進 CI。

## 限制

ledger 也是被管理的 agent 可以寫的檔案。這個工具讓「悄悄繞過」變成 diff 中看得見的改動，但不能讓繞過變得不可能。
