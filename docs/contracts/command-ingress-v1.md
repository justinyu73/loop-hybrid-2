# 指令入庫 v1

狀態：active。本文件描述外部如何把一個指令送進 Goal store，以及如何讀回它的狀態。依據為 `lh_runtime/command_ingress.py` 與 `lh_runtime/goal_store.py`。

## 1. 目的

指令入庫是引擎唯一的「往下送」入口：呼叫者只登記事件，不直接操作 Goal 或 Run。後續的 admission、派工與驗收全部由引擎依 contract 決定。

## 2. 送出一個事件

```bash
python3 -B lh_runtime/command_ingress.py --goal-store /path/to/goals \
  --source operator --event-type manual_intent --event-id cmd-1 \
  --payload '{"campaign_id":"example-campaign","stage_id":"feature","intent":"..."}'
```

- 事件類型只能是 `manual_intent`、`stage_completion`、`scheduled_tick`、`external_verdict`、`restart` 其中之一。
- `manual_intent` 與 `stage_completion` 必須在 payload 中指明 `stage_id`。
- 去重 key 是 `--event-id`，或 `--idempotency-key`。同一個 key 重送時，回傳既有結果；key 相同但來源、類型或內容不同，一律拒絕。
- 新事件的狀態為 `event_received`。後續轉移見 `goal-lifecycle-v1.md`。

## 3. 讀回狀態

```bash
python3 -B lh_runtime/command_ingress.py --goal-store /path/to/goals --run-store /path/to/runs \
  --status --event-key <event_key>
```

回傳 `lh-command-status/v1`，內容包括：
- 事件狀態，以及事件的來源、類型、payload digest；
- 對應的 Goal 與其狀態；
- 執行鏈：從來源事件到 Goal 事件，以及最新的 Run 狀態。

找不到事件時回傳 `event_state: unknown`，不會猜測。

## 4. 驗收燈

`lh_runtime/command_ingress_canary.py` 與 `lh_runtime/intent_derivation_canary.py` 涵蓋以下行為：入庫、去重、衝突拒絕、讀回，以及「指令 → candidate → admission → dispatch → completed」整條鏈。

## 與現行程式的差異

- 原設計是控制平面與引擎之間的雙向 command bus，包含控制平面事件與多專案 fleet 派工邊界。公開版只保留引擎側的入庫與讀回，沒有控制平面。
- 原設計允許控制平面直接寫入特定控制事件。公開版只接受上列五種事件類型。
