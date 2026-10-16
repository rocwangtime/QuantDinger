# Polymarket observer：读路径降本 + 按序列分组的分片均衡（2026-10-16，仅纸面）

基线 main `6071e79`（`observer.py`、`ws_balance.py` 与生产 `e93bcd1` 一致）。只改 observer 的 WS 接收与分片、相关测试、`env.example`、Paper Lab 文档和本文；新增离线回放脚本。没有改 maker、PAPER_AUTO、钱包、实盘或部署配置，没有连接生产或公开行情。

## 结论先说

slow consumer 的直接原因是**读 socket 的线程拿不到 GIL**，不是分片本身。分片不均会放大问题，但只做均衡（#62–#70 的思路）救不了。这次三处一起改：

1. 读路径：一次读 64KB 并把缓冲里的帧处理完再回到 select；跳过 websocket-client 的纯 Python UTF-8 校验；观察器进程的 GIL 切换间隔从 5ms 降到 0.5ms。
2. 计量：入站时按组计数。组 = 一个 Up/Down 序列的全部窗口（当前 + 下一个），或一个 YES/NO 市场。换期时资产 ID 变了，但组和它的速率不变。
3. 迁移：整组移动，每轮最多 4 组；只在“比理想布局差 10% 以上”或“超过 2 倍而 2 倍可达”时才动；重启时用上次持久化的组速率直接按负载放置。

## 根因与证据

### 1. GIL 护航效应卡住读线程（slow consumer 的直接原因）

websocket-client 读一帧要做 select、读 2 字节帧头、读负载三次系统调用，每次都释放 GIL。进程里只要有一个 CPU 密集线程（评估器、各分片 worker、纸面账户、录制），每次重新拿 GIL 最多等一个切换间隔（默认 5ms），一帧就要 10–15ms，单个 socket 只能读每秒约 100 帧。服务端发送缓冲堆满后就以 slow consumer 断开。

另外，没装 wsaccel 时 websocket-client 用纯 Python 逐字节校验 UTF-8。本机实测：600 字节的 price_change 要 48µs（JSON 解析只要 2µs），7KB 的 book 要 527µs（JSON 解析 30µs）。这些都发生在读线程上。

回放复现（同一合成分布，加 1 个同进程 CPU 密集线程代表其他工作）：基线 5 分钟内 26 次 slow consumer、22 次重连，每个分片实际只读到 53–117 msg/s，盘口新鲜度 p95 10.8 秒；加 2 个时 p95 23.7 秒（最大 38.5 秒）。这和生产的 25–40 秒、频繁断线吻合。#63 把热门市场单独放一个分片后排队 2.6–3.6 秒，#66 合并帧后 p95 反而 104 秒，也都符合“读线程/worker 拿不到 GIL”的解释：它们没有减少每帧的 GIL 交接次数。

### 2. 按资产记速率，Up/Down 换期后估算失真

热门市场是 5m/15m 的 Up/Down，每 5/15 分钟换一批资产 ID。旧代码按资产记 EWMA 速率，新窗口从 0 开始；换期时旧窗口被移除，它还没结算的计数也一起丢了。基线回放中分片估算为 491/105/119/89，实际是 397/85/651/238。#68/#70 修正了入站计数，但仍是按资产记，换期问题还在。

### 3. 迁移逻辑无法稳定收敛

旧逻辑每次把整体重新 LPT 打包，再用滞回门槛决定是否应用；权重本身不准时会把多个热门市场堆到一起（#68：30 个资产挪到同一分片）。#70 限制每轮最多挪 8 个资产，但热门市场换期后永远是“新资产”，被冷启动保护挡住不能迁移，所以最重分片仍是最轻的 2.6–4.3 倍。

## 修改内容

### 接收路径（`observer.py`）

- `read_ahead_app()`：在 websocket-client 的 `WebSocketApp` 上换一个 dispatcher。底层每次读 64KB，`frame_buffer` 本身会保留多读的部分；dispatcher 先处理完缓冲里已有的帧（以及 SSL 的 `pending()`），再去 select。这样一次 GIL 交接能处理一批帧。依赖的内部接口不存在时，退回库自带的 dispatcher。
- `run_forever(..., skip_utf8_validation=True)`：文本帧以 bytes 交给回调。`json.loads` 在 C 里做 UTF-8 校验，非法帧直接丢弃。`PONG` 同时识别 str 和 bytes。
- 回调里解析一次 JSON，先计入入站计数，再把解析好的对象放进现有 FIFO；worker 不再重复解析。回调不拿 Pipeline 锁，不做 I/O。
- `tune_switch_interval()`：观察器进程创建连接前，把 `sys.setswitchinterval` 设为 `POLYMARKET_OBSERVER_GIL_SWITCH_MS`（默认 0.5ms，范围 0.1–5，0 表示不改）。observer 是独立进程（`polymarket_observer` 命令），不影响 Web 进程。
- `POLYMARKET_OBSERVER_WS_URL` 可覆盖公共 WS 地址，只用于离线回放。

### 分组与均衡（`ws_balance.py`）

- `balance_key(market)`：Up/Down 用 `up_down:{asset}:{interval}`，YES/NO 用 `market:{id}`。同一组的所有资产始终在同一分片，所以每个市场的 YES/NO 也在同一分片。
- 入站计数直接累加到组上：多资产帧的一条消息成本平分给它携带的已归属资产，各分片负载相加等于实际 msg/s。旧所有者在迁移交接期间收到的重复帧不计。第一次采样直接用窗口平均值，之后用 EWMA（半衰期 30 秒）；中途出现的组按出现时间算窗口。
- 目录刷新（换期）时，已知组保留原分片，只在该分片上发增量 unsubscribe/subscribe，不计为迁移；新组按权重放到最轻的分片。冷启动没有先验时，20 秒后先采一次样。
- `plan()`：先算一个参照用的“理想布局”比值（按权重 LPT 后再做局部搜索）。只有当前最重/最轻比值超过 `REBALANCE_RATIO`（1.5），并且比理想值差超过 `REBALANCE_HYSTERESIS`（10%），或者超过 `REBALANCE_TARGET_RATIO`（2）而 2 可达时，才开始搜索。搜索只考虑涉及最重或最轻分片的单组移动和两组互换，按“比值优先、平方和次之”贪心选择，每轮最多 `REBALANCE_MAX_GROUPS`（4）组，冷却 `REBALANCE_MIN_SECONDS`（60 秒）。未完成首次测量的组不移动。一个组单独就超过别的分片时，理想值本身很高，不会来回抖动。60 个组时一次规划约 6ms。
- 状态里新增 `ws_group_rates`（各组 msg/s）、`ws_load_ratio`、`ws_unmeasured_groups`、`ws_rebalance_ratio`、`ws_rebalance_target_ratio`。`ws_group_rates` 随状态行每 10 秒持久化，重启时作为先验，只用于首次放置，不会触发迁移。

### 配置项变化

| 变量 | 旧默认 | 新默认 | 说明 |
| --- | --- | --- | --- |
| `WS_REBALANCE_MIN_SECONDS` | 120 | 60 | 两轮迁移之间的冷却 |
| `WS_REBALANCE_HYSTERESIS` | 0.25 | 0.1 | **含义变了**：现在是“比理想值差多少才动”，原来是 (max-min)/mean 的改善门槛 |
| `WS_REBALANCE_RATIO` | — | 1.5 | 低于此比值不动 |
| `WS_REBALANCE_TARGET_RATIO` | — | 2 | 可达时一定压到此比值以下 |
| `WS_REBALANCE_MAX_GROUPS` | — | 4 | 每轮最多移动的组数 |
| `GIL_SWITCH_MS` | — | 0.5 | 观察器进程的 GIL 切换间隔 |

（变量名都带 `POLYMARKET_OBSERVER_` 前缀。）如果生产 `.env` 照抄了旧 `env.example`，里面会显式写着 `MIN_SECONDS=120`、`HYSTERESIS=0.25`。保留也能用，只是冷启动收敛慢一些；建议上线时删掉这两行，或改成新默认。

## 回放验证

### 端到端网络回放（`backend_api_python/scripts/pm_ws_replay.py`）

独立进程在 127.0.0.1 模拟公共 market 频道：按资产订阅/退订，新订阅先发 book 快照，PING/PONG，按每个资产的速率发 Poisson 分布的 price_change（一半同时带另一边）、book（6%）、last_trade_price（4%）。Up/Down 按 5 倍速换期（5m 窗口 60 秒、15m 窗口 180 秒），目录按同样倍速每 12 秒刷新。服务端某连接未发出的积压超过 1MB 时，发 1008 "slow consumer" 并断开。客户端是真实的 `MarketConnections`、websocket-client、FIFO、worker、`Pipeline`（录制打开、数据库换成空实现）和评估器，全在一个进程里，与生产一样共用 GIL。分片负载按服务端实际发出的消息计算。

输入是合成分布：28 个市场、56 个资产、合计约 1,372 msg/s，最热市场两边合计约 367 msg/s，前 3 个约 80%。5 分钟，前 30 秒为预热，“稳态”指 120 秒以后的 30 秒窗口。

| 场景 | 代码 | 各分片 msg/s（全程） | 30 秒窗口最重/最轻 | slow consumer | 重连 | 盘口新鲜度 p95 | 接收队列等待 p95 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 无额外负载 | 基线 | 234/404/174/560 | 7.5–36 | 0 | 0 | 2.7s | 0.4ms |
| 无额外负载 | 新 | 378/241/384/367 | 首窗口 2.11，稳态 1.49–1.54 | 0 | 0 | 3.3s | 0.2ms |
| 1 个 CPU 线程 | 基线 | 53/100/117/62 | 有分片断线时为无穷大 | **26** | **22** | 10.8s | 8.8s |
| 1 个 CPU 线程 | 新 | 372/240/384/372 | 稳态 1.47–1.53 | 0 | 0 | 2.8s | 3ms |
| 2 个 CPU 线程 | 基线 | 51/79/38/84 | 有分片断线时为无穷大 | **19** | **16** | 23.7s（最大 38.5s） | 19.9s |
| 2 个 CPU 线程 | 新 | 379/239/384/363 | 稳态 1.50–1.56 | 0 | 0 | 3.0s | 5ms |
| 2 个 CPU 线程 | 新 + 先验（模拟重启） | 375/367/356/270 | 全程 1.36–1.41，0 次迁移 | 0 | 0 | 3.0s | 6ms |

分项（同一合成分布、2–4 个 CPU 线程）：只开预读时，4 个 CPU 线程下仍是 0 次 slow consumer、socket 积压约 30KB，但 worker 排队 p95 6.8 秒；再加 0.5ms 切换间隔后降到 9ms。只调切换间隔、不开预读时，1ms 在 2 个 CPU 线程下仍有 11 次 slow consumer，0.5ms 则为 0。纯负载加压（不加 CPU 线程）：流量 ×5（6,858 msg/s）时基线服务端积压峰值 227KB，新代码为 0。

复现命令（在 `backend_api_python` 下）：

```bash
.venv/bin/python scripts/pm_ws_replay.py --synthetic --seconds 300 --settle 120 --gil-threads 2
.venv/bin/python scripts/pm_ws_replay.py --rates ../deploy/pm_ws_asset_rates_20261016.json --seconds 300 --gil-threads 2
.venv/bin/python scripts/pm_ws_replay.py --synthetic --code /path/to/baseline/backend_api_python
```

### 单元级 2 小时回放（`tests/test_polymarket_ws_replay.py`）

驱动真实的 `MarketConnections.sync/tick`，假 socket，模拟时钟，按真实 5m/15m 节奏换期、每 60 秒刷新目录，每秒按 Poisson 计数进入入站计数器。如果存在 `POLYMARKET_WS_ASSET_RATES` 或 `deploy/pm_ws_asset_rates_20261016.json`，就读这个文件，否则用合成分布。

- 冷启动：第 20 秒、第 80 秒各迁移一轮（14、12 个资产），从第 90 秒起 2 小时内每 30 秒采样都 ≤2，稳态最大 1.51，之后 0 次迁移。
- 带先验重启：全程 ≤1.39，0 次迁移。
- 所有时刻 YES/NO 同分片；没有关闭任何 socket。

规划器离线评估：合成分布从 200 种随机冷启动布局出发，最多 3 轮、共 12 组移动，收敛到 ≤1.53（理想 1.39）。400 个随机重尾分布（12–40 组）中，理想值 ≤2 的 230 个里有 229 个收敛到 ≤2，最多 7 轮。

### 测试

- 定向（`test_polymarket_ws_balance.py`、`test_polymarket_ws_pipeline.py`、`test_polymarket_ws_replay.py`）：55 passed，约 2.3 秒。新增的测试覆盖：换期只在序列所在分片上发增量订阅且不计迁移；入站计数在 FIFO 拒收时也计入，每帧只解析一次，`skip_utf8_validation` 已传入，PONG/非法 UTF-8 不入队；预读 dispatcher 一次写入的 300 帧在没有新数据时也全部送达，底层读调用少于帧数的 1/10（去掉“先处理缓冲”这一步后该测试会失败）；冷启动/重启/噪声三种 2 小时回放。
- 全量 `pytest -q`：4444 passed、24 skipped、59 warnings，78 秒。测试进程的 `DATABASE_URL`、`AUTOMATION_TEST_DATABASE_URL`、`QD_TEST_POSTGRES_DSN` 指向 localhost 的 `qd_automation_test`。
- `ruff check backend_api_python`、`git diff --check`：通过。

## 验收对照

| 验收项 | 结果 | 说明 |
| --- | --- | --- |
| slow consumer 0 次、重连很少 | 回放中 0 次、0 次重连 | 包括 2 个同进程 CPU 线程的场景；基线在同一场景下 19–26 次 |
| 最重分片 ≤ 最轻 2 倍 | 稳态 1.47–1.56；带先验 1.36–1.41 | 冷启动第一轮迁移前（≤20 秒）和第一、二轮之间约 1 分钟内为 2.1 左右 |
| 盘口新鲜度 p95 < 60 秒 | 回放 2.8–3.3 秒 | 生产 p95 还包括冷门市场自然静默，需上线后看 `book_age_p95_ms` |
| 用流量分布文件做回放 | **未完成** | 见下 |

## 没做到 / 没验证的

- **实测分布文件没拿到。** `pm_ws_asset_rates_20261016.json` 只在 box 上，不在 git 里，本机也没有对应主机的 SSH 配置。上面的回放用的都是按描述构造的合成分布。脚本和单测都会自动读取这个文件：放到仓库 `deploy/` 下或设置 `POLYMARKET_WS_ASSET_RATES` 即可。文件格式未知，`load_rates()` 兼容了几种常见结构（按资产或按市场、带 slug 的 Up/Down 自动识别为序列）；如果格式不符需要再调。
- 本机是 Apple Silicon，比生产机快。CPU 密集线程只是“其他工作”的替代，不是生产负载的精确模型。服务端的 slow consumer 阈值未知，1MB 积压是近似值，所以“0 次”是相对这个模型的结论。
- 没做真实公网行情的长时间运行。
- 如果某个序列单独就超过其他所有组的总和/3，2 倍目标在数学上无法达到；这时规划器停在可达的最优附近，不会抖动。

## 上线后看什么

`slow_consumer_count`、`close_reason_counts`、`reconnect_count`、`ws_load_ratio`、`ws_shard_loads`（应接近各连接的实际 msg/s）、`ws_group_rates`、`ws_unmeasured_groups`、`ws_receive_queues[*].lag_seconds / drops / resyncs`、`book_age_p95_ms`、`rebalance_count`（稳态下应几乎不增长）。

回滚：回退本 PR 即可，无数据库迁移。状态里多出的字段旧代码会忽略。
