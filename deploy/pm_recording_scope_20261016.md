# Polymarket observer 录制范围可配置 — 2026-10-16

分支 `claude/magical-austin-a97023`，基于 `feat/pm-maker-v3-20261016`（`5c125c1`）。只扩大公开数据录制层，供 v3 纸面做市使用。
纸面专用：不接钱包、不签名、不下单、不碰 `PAPER_AUTO`。未合并、未部署。

## 改动

- `recording.py`：`scoped()` 改为读 `POLYMARKET_OBSERVER_RECORDING_ASSETS` / `_INTERVALS`。默认值是 `BTC,ETH` / `5m,15m`；变量不设、为空或只有逗号空格时都用默认值，所以不设变量时行为不变。名称大小写不敏感，不认识的名称直接丢掉。YES/NO 市场始终录制。实际生效的范围写在状态字段 `recording_scope` 里，拼写错误可以从那里看出来。录制范围只能在 `UPDOWN_ASSETS/INTERVALS` 的发现结果里再缩小，不能超出。
- `windows.window_seconds` 按规则文本取值：Chainlink 60 s TWAP 记 60，币安 K 线记 0；规则解析不出时按周期标签兜底（1h/1d 记 0，其余记 60），与 maker 的 v2 升级兜底一致。记为 0 的窗口不会和 Chainlink TWAP tick 关联，所以 `price_to_beat`、`computed_outcome` 不会被错误数据源填上；官方结果仍由 Gamma 轮询写入。
- 迁移 `20261016_polymarket_recording_scope.sql`（已注册到 bootstrap，排在 recording/backtest 之后）：删除 `window_seconds` 上原有的 `CHECK (=60)`（按定义查找，不依赖约束名），新增 `qd_pm_windows_settlement_window CHECK (window_seconds IN (0,60))`。可重复执行，不改数据，已有行保持 60。
- `updown.discover`：修正 SOL 1h/1d 的 series slug。Gamma 上这两个序列叫 `solana-up-or-down-hourly/daily`（其余都是 `sol-…`、`btc-…` 等），代码原来拼的是 `sol-up-or-down-hourly/daily`，查不到，所以 SOL 1h/1d 从来没被发现过，录制范围再大也拿不到。**这条会改变默认行为**：只要 `UPDOWN_ASSETS/INTERVALS` 保持默认（含 SOL、1h、1d），observer 会多订阅最多 4 个市场（8 个 token），并为它们写 samples。默认录制范围不含 SOL，所以不会多写 books/trades。实测 20 个组合都能发现，没有被拒的。
- `maker.RecordedFeed.outcome`（见下文"代理结算"）：只改了 1d 的代理取价时点，报价、阶段、公允价逻辑都没动。

## 部署顺序

1. **先**执行 `python -m app.commands.migrate`（compose 里 observer 依赖 `migration` 服务，正常流程会自动先跑）。如果没迁移就打开 1h/1d 录制，`window_seconds=0` 的插入会违反旧约束，写线程会一直重试同一批，后面的写入全部卡住。默认范围不会写 0，不受影响。
2. 设置 `POLYMARKET_OBSERVER_RECORDING_ASSETS=BTC,ETH,SOL,XRP`、`POLYMARKET_OBSERVER_RECORDING_INTERVALS=5m,15m,1h,4h,1d`，重启 observer。重启后，当前所有在范围内的市场都会补写 windows 行（`ON CONFLICT` 不会重复）。
3. 观察 `recording_scope`、`recording_rows_per_minute`、`recording_dropped`、写队列 `dropped`/`writer_lag_seconds`/`last_write_error`，以及 books 表的增长和清理积压。

回滚：删掉这两个变量即恢复原范围。新约束放宽了取值范围，可以保留，不需要回滚迁移。

## 规则文本核对（2026-10-16 公开 Gamma 只读）

| 组合 | 结算源 | 结算价时点 | 官方结果延迟（近 4 个） |
| --- | --- | --- | --- |
| 1h | Binance BTC/USDT 1H K 线 | 开盘价 vs 收盘价，即 eventStartTime / endDate | 结束后 11–18 分钟 |
| 1d | Binance 1 分钟 K 线 | 两天美东中午 12:00 那根 1 分钟 K 线的**收盘价**，即 eventStartTime+60 s、endDate+60 s；相等按 50-50 | 12–13 分钟 |
| 5m、15m、4h | Chainlink 60 s TWAP 数据流 | w=60，四个币种相同 | 4h 约 1 分钟 |

## 代理结算（`RecordedFeed.outcome`）

先查 `windows.official_outcome`。录制范围包含 1h/4h/1d 和 SOL/XRP 后，这些市场也有了 windows 行，官方结果可以写进来；之前没有行，只能走代理。不过 maker 在 t1 之后第一次轮询就会结算，而官方结果要晚约 15 分钟，所以对 1h/1d 来说，实际起结算作用的是用币安样本算的代理结果（`estimated=True`）。

- **1h**：原逻辑已经正确，取本市场 samples 中 `reference_observed_at` 落在 [边界−10 s, 边界] 内最新的币安价，分别作为开盘价和收盘价的代理，`>=` 判 Up。行为不变。
- **1d**：原逻辑取的是边界前的价格，比规则规定的结算价早 1 分钟；而且本市场在 endDate 之后就不再写 samples，所以 endDate+60 s 的价格在本市场的 samples 里根本取不到。现在的做法：`maker_model.print_lag()` 识别出"1 minute candle"规则后，等到 endDate+60 s 才结算；取价改为在 [边界+60 s−10 s, 边界+60 s] 内找同币种任意市场的币安样本（比如次日的日线、当前的小时线），查询用 `observed_at` 时间索引限定范围。相等判 `Tie`（0.5）。没有样本就返回未知：有库存的一直等，没有库存的到 `settle_timeout_s` 后按未结算处理，不会推断结果。

**留给策略方的后续事项（本次不改）**：1d 规格里的 t0/t1 仍是边界本身，比真实结算时点早 60 s。影响是公允价方差少算 60 s（相对 86400 s，只在最后几分钟有意义，而那时已处于 FLAT），开盘价也早读 1 分钟。另外，1h/1d 可以考虑在 t1 之后等官方结果（约 15 分钟）再结算，不必立即用代理结果。

## 容量影响与预计行数

- **WS 分片：没有影响。** 订阅集合本来就是发现到的全部市场（YES/NO + `UPDOWN_*` 允许的所有 Up/Down），录制范围只决定写哪些行，不新增订阅（上面 SOL slug 的修正除外）。全范围 Up/Down 最多 40 个市场 / 80 个 token（`UPDOWN_MAX_MARKETS=40` 正好等于 4 币种 × 5 周期 × 每组合 2 个），加上 YES/NO 40 个 token，远低于单分片 600 的软目标，仍是默认 4 个分片。
- **books 实测**：2026-10-16 01:07–01:12 UTC 采样约 5 分钟（亚洲时段，交易清淡），只读订阅公开 WS，按 recorder 的规则计数（前 5 档有变化、每个 token 至少间隔 1 s）。当时 slug 修正前发现了 26 个 Up/Down 市场、52 个 token：

  | 范围 | token 数 | books 行/秒 | 折合行/天 | trades 条/秒 |
  | --- | --- | --- | --- | --- |
  | 默认（BTC/ETH 5m/15m） | 16 | 4.75 | 约 41 万 | 0.53 |
  | 全部已发现组合 | 52 | 11.69 | 约 101 万 | 0.57 |

  按 token 平均：5m/15m 每秒 0.25–0.37 行，1h 每秒 0.13–0.40 行，4h 每秒 0.05–0.12 行，1d 每秒 0.01–0.15 行。5 分钟内 1h/4h/1d 一笔成交都没有。加上 SOL 1h/1d 后估计每秒约 12 行。美国时段可能高出几倍，容量规划仍按下面的上限算。YES/NO 部分不受录制范围影响。
- **books 上限**：每个 token 每 `BOOK_MIN_INTERVAL_MS`（1 s）最多写 1 行，而且只在前 5 档变化时写。默认范围 56 个 token，上限 4,838,400 行/天；全范围 120 个 token，上限 10,368,000 行/天（3 天保留期内约 3,110 万行）。
- **windows**：默认 768 行/天；全范围 4 ×（288+96+24+6+1）= 1,660 行/天，30 天约 5 万行。
- **清理**：`RECORDING_CLEANUP_BATCH=100000` 是每张表每 10 分钟的上限，即每天最多删 1,440 万行。全范围 books 按上限算每 10 分钟约 72,000 行，有 1.39 倍余量，不需要调整。换算公式：每轮行数 ≈ token 数 × 600 × 1000 / `BOOK_MIN_INTERVAL_MS`，batch 应至少为它的 1.3 倍。调低录制间隔或调大 `MARKET_COUNT` 时要相应调大 batch。
- **暂存缓冲 `RECORDING_BUFFER_MAX=10000`**：只暂存两次评估之间（250 ms）收到的 trade 和 TWAP 记录，books 直接进写队列（按 token 合并）。
- **写队列**：`WRITE_QUEUE_MAX=10000`、批大小 500，不需要调整。books 每个 token 最多占 1 项。
- **官方结果轮询**：每 60 s 最多查 10 个窗口。全范围每小时约 69 个窗口结束（每分钟约 1.2 个），结果都在约 15 分钟内出，积压很小。
- **maker 读取**：`RecordedFeed` 每 250 ms 对全部市场 token（至多 80 个）按 `(token_id, received_at)` 索引各取最新一行盘口，开销随 token 数线性增长，可以接受。

## 验证

本地 PG18 临时容器（`postgres:18-bookworm`，18.6，`127.0.0.1:5432/qd_automation_test`，只用于测试，用完删除），在 `backend_api_python` 下用主仓库的 `.venv` 执行：

- 新增单测：recording 16 个（默认范围不变、名称规范化、默认范围不录 1h、全范围录 books/trades/windows 且 `window_seconds` 按规则取值、规则到窗口的映射），`print_lag` 1 个，`RecordedFeed.outcome` 2 个（1h 用本市场边界样本；1d 等到 +60 s 后读同币种样本，包含 Tie/Down/官方结果优先），discovery slug 1 个，bootstrap 注册 1 个。
- 新增集成测试 3 个：迁移可重复执行并放行 0、拒绝 30；全范围下币安窗口不会和 Chainlink TWAP 关联，books/trades 写入 1h 和 SOL；1d 代理结算的真实 SQL（同币种、+60 s 窗口、排除收盘后和其他币种的样本、Tie、官方结果优先）。
- `python -m app.commands.migrate` 在空库上全量执行两次：第一次 applied，第二次 `20261016_polymarket_recording_scope: unchanged, skipped`；最终约束为 `CHECK ((window_seconds = ANY (ARRAY[0, 60])))`。
- `-k "polymarket or db_bootstrap or observer"`：带库 460 passed（0 skipped）；不带库 433 passed、27 skipped（集成测试按预期 skip）。
- `ruff check .`：All checks passed；`git diff --check`：通过。
- 用真实 Gamma 只读核对：20 个组合都能发现、没有被拒；规则解析结果为 Chainlink 组合 w=60，1h 币安组合 w=0，1d 币安组合 w=0、tie=0.5、print_lag=60。
