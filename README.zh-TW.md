# Loop Hybrid 2 中文說明

Loop Hybrid 2（LH2）是一個**確定性 goal loop 引擎**：把核准過的目標（goal）變成可稽核的執行（run）。
每一步可重播、每個驗收來自 committed check、一切不可逆操作留在人手上。

> English: see [README.md](README.md)

## 核心概念

- **Durable SQLite goal/run store**：goal、run、attempt、用量記錄全部落庫，重啟可恢復，不存在只活在記憶體的狀態。
- **Serial 單 holder worker**：同一時間只有一個 worker 推進 loop，狀態轉移確定性、可稽核。
- **Disposable-clone executor**：每次嘗試都在一次性 workspace clone 裡執行，不污染原始碼樹。
- **Committed canary 是驗收權威**：驗收 = repo 裡的可重跑檢查（`gate-pack/`、`lh_runtime/*_canary.py`），不是模型說了算。
- **Promotion 永遠人持有**：push、merge、publish 不由 loop 執行。
- **多模型分層**：contract 的 `models` 欄位讓執行用 coding CLI、判斷用推理 CLI，彼此獨立、可各自計價。

## 流程圖

### Goal 生命週期

```mermaid
flowchart LR
    E["event（指令/排程/上階段完成）"] --> R{去重<br/>idempotency key}
    R -->|新| C[candidate]
    R -->|重送| O[回傳既有結果]
    C -->|envelope 核准| A[active + queued run]
    C -->|缺燈/越界| H[human_required]
    A -->|驗收燈綠| D[completed]
    A -->|retry 耗盡| S[stopped]
    A -->|报红-RED| H
    H -->|人處理| C
    D --> N[派生下一階段 event]
```

### Run 執行（serial，一次一條）

```mermaid
flowchart LR
    Q[queued run] --> W[worker tick<br/>單 holder lease]
    W --> CL[disposable clone<br/>@ pinned commit]
    CL --> X[executor CLI<br/>codex / claude / kimi]
    X --> V{acceptance lamp<br/>verification_argv}
    V -->|exit 0| RC[receipt + usage 入帳]
    V -->|失敗| RT[retry<br/>上限 max_attempts]
    RT -->|耗盡| ST[stopped]
    RC --> N2[goal completed → 下一階段]
```

### 多模型分層（可選）

```mermaid
flowchart TB
    subgraph 執行層
        M1[models.execute<br/>coding CLI] --> RUN[run 執行]
    end
    subgraph 判斷層（轉折點）
        M2[models.judge<br/>推理 CLI] --> P{封閉三選一<br/>select / human_required}
        P -->|合法| SEL[選定下一條 runnable]
        P -->|越集/異常| F[退回決定性選路]
    end
    RUN -.同一 store 計價.-> COST[(usage/cost<br/>按真實模型 id)]
    M2 -.-> COST
```

不設 `models.judge` 時整個 loop 走純決定性選路，行為不變。

## 安裝

需求：**Python 3.12+** 與 **Node.js**（npm script 只是 shell/Python 的薄包裝）。

```bash
git clone https://github.com/justinyu73/loop-hybrid-2.git
cd loop-hybrid-2
npm test        # 跑全部確定性 gate（必須全綠）
npm run lint    # shell 語法 + Python 編譯檢查
```

要執行真實 coding agent，需任一已登入的 CLI：`codex`、`claude` 或 `kimi`。

## 使用

### 1. 建立專案 contract

複製 [`project_runtime_contract.example.json`](project_runtime_contract.example.json) 到你的專案，填入
`project_id`、`campaign`（stage、驗收燈、允許路徑）、`source_repo`、`base_revision`、以及可選的
`models`（execute / judge / judge_model）。

### 2. Dry-run（不觸碰 provider）

```bash
python3 -B lh_runtime/goal_loop_run.py \
  --contract /path/to/project_runtime_contract.json
```

印出解析後的執行計畫，不呼叫任何模型。

### 3. 真實執行（有界）

```bash
python3 -B lh_runtime/goal_loop_run.py \
  --contract /path/to/project_runtime_contract.json \
  --executor codex --execute \
  --max-cycles 12 --max-runtime-seconds 900
```

- executor 只在 disposable clone 裡工作；輸出止步於 PR。
- 每個 attempt 產生 receipt（含 usage）；`status_snapshot_out` 指向的檔案會得到即時狀態投影。
- `runtime/loop-pause`（或 contract 的 `pause_flag`）存在即於下一個 tick 安全停止。

### 4. 驗收紀律

「完成」只由 committed canary / lamp 證明；模型輸出永不構成驗收。
驗收失敗、依賴斷裂、scope 擴張一律轉 `human_required`，由人接手。

## License

[MIT](LICENSE) — copyright 2026 Loop Hybrid contributors.
