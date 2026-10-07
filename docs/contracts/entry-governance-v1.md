# 入口與文件治理 v1

狀態：active。本文件是 `gate-pack/docs_structure/canary.py`、`gate-pack/docs_contracts/canary.py` 與 `gate-pack/contract_seal/seal.py` 的依據。核心價值：系統要能長跑；上下文只保存摘要與快照；不讓文件把系統壓垮。

## 1. 原則

| # | 原則 | 機制 |
|---|---|---|
| P1 | 單一入口 = `AGENTS.md`（不超過 40 行） | 內容：建置與測試指令、現況一段話、禁忌清單。不設索引檔，目錄本身就是索引 |
| P2 | 文件分三層 | `docs/contracts/`：長期契約，修改需核准，並且必須重封（見第 3 節）。`docs/active/`：一個 track 一份。`docs/archive/`：已完成的紀錄。`docs/` 根目錄只放 `README.md` |
| P3 | 上下文只讀摘要與快照 | 每個 session 開頭只讀 `AGENTS.md`，契約按需單份讀取。任何「依序讀 N 份檔」的指示一律刪除 |
| P4 | 狀態只記一處 | run、goal、成本等可推導的狀態，一律由 store 與快照投影，不得手寫。active 文件只放指向與解讀 |
| P5 | 結構由 gate 執法，語意由人審 | `docs-structure`：三層目錄存在、根目錄沒有零散文件、active 一個 track 一份、文件總數上限只降不升、`AGENTS.md` 的行數與禁用字樣。`docs-contracts`：契約只能提到程式中真的存在的東西，並標記差異。`contract-seal`：契約內容與封印相符 |
| P6 | 一次性產物瘦身 | 調查與審查的結論併入 active 或契約，原產物進 archive |
| P7 | 文件不複製指令 | `AGENTS.md` 是唯一手寫的活指令；其他文件只能指向它，不得複製指令性內容 |
| P8 | 執法工具隨慣例同步 | 目錄慣例改變時，相關 gate 與封印基準在同一批變更中更新，並經核准 |

## 2. 契約文件的寫法

- 依公開版現行程式撰寫。提到的 repo 路徑、schema ID、CLI 旗標與 `LH_*` 環境變數，都必須能在程式中找到（由 `docs-contracts` 檢查）。
- 每份契約以 `## 與現行程式的差異` 收尾：逐條列出原設計與現行程式不一致的地方；沒有差異就寫「無」。
- 不寫宿主、廠商、私有路徑或內部代號。

## 3. 契約封印

「修改契約需要核准」這句話本身擋不住任何人：沒讀到它的 agent 不受影響，讀到它的 agent 可以連同檢查一起改。封印給檢查一個改不動的錨點：`docs/contracts/seal.json` 記錄每份契約的 digest（`lh-contract-seal/v1`）。

- **範圍由 repo 決定**：`gate-pack/contract_seal/seal.py` 列出受追蹤、以及未追蹤但未被忽略、符合指令所給樣式的檔案（預設 `docs/contracts/*.md`），不由封印檔決定。封印檔少了一筆、或記錄的範圍比要求的窄，都判為 broken。
- **判為 broken 的情況**：契約沒有被封印、已封印的契約不存在、digest 不符、封印檔讀不到或範圍不符。
- **重封不被阻擋，但必須看得見**：`reseal --sealed-by <名稱> --reason <文字>` 一律重算整個範圍，沒有只重封單一檔案的選項。owner 核准的變更修改契約時，在同一個 PR 重封，PR 中列出封印的 diff。
- digest 先把 CRLF 正規化為 LF，Windows 與 Linux 的 checkout 結果相同。
- 封印只證明位元組沒有悄悄改變，不判斷內容的語意；語意仍由人審（P5）。

## 與現行程式的差異

- 原設計允許各 CLI 目錄存放鏡像的公約檔，並設有批准 ledger。公開版沒有任何 CLI 公約檔，也沒有批准 ledger；核准以 PR 與 owner 合併為準；公開版另以契約封印讓契約的修改在 diff 中可見。
