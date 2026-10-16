# Polymarket observer WS 深度分析与修复（2026-10-17，仅纸面）

## 结论与证据边界

本次修复读取回调的阻塞风险、迁移期间的全局盘口锁，以及针对重尾流量失效的迁移目标。**不能据现有证据宣布 #73 的两次远端 slow_consumer 已被唯一归因或永久消除。** `slow consumer: send buffer full` 证明远端某个发送队列满；它不等于本地 FIFO 满，也不足以证明 GIL 是唯一原因。此前 #73 报告将 GIL 称为已确认直接原因，证据强度不足，需要纠正。

实际流量文件有 56 个资产、28 个市场，归因速率合计 **681.789 msg/s**。市场 `5721631` 为 **367.434 msg/s（53.89%）**，接着是 `5721627` **148.030**、`5721468` **114.782**；前三个合计 **92.44%**，并非先前合成分布的 80%。文件没有市场名称、系列或窗口时间，不将这些 ID 擅自标成 BTC/ETH 或推断换期。使用字段是 `msg_per_s`，不能把 `frames_touching_per_s` 两腿相加：同帧经常同时触及两腿，会重复计数。

保持整市场/系列不可拆时，峰值的物理下界为 `max(最大组速率, 总速率/连接数)`。4、6、8 条连接的下界在这份文件中都为 **367.434 msg/s**；单纯加连接不能降低它。4 条连接的 max/min 比值甚至有数学下界 `3*367.434/(681.789-367.434)=3.507`，所以“最重/最轻 <=2”不可能成为本次验收目标。可用的重启先验布局约为 **367/148/115/51**，比值约 7 仍是健康布局。

## 基线、范围与复现材料

先执行 `git fetch origin`。第一次看到 main `0f7678c`（#74 回退 #72/#73），从该 main 建立 `fix/pm-ws-deep-analysis-20261017`，只取回 #73 观察器相关文件继续修复。工作期间 main 经 #75 恢复为 **`7944d76`**，再次 fetch 后已将分支 rebase 到此版本；它包含 #73 **`9a6f71a`**。最终 PR 相对 main 不包含恢复 #72 或 maker 的差异。

范围是 `observer.py`、`ws_balance.py`、观察器配置说明、离线回放及其测试和本文。maker、paper_account、PAPER_AUTO、钱包、生产配置、数据库结构均没有修改。没有合并或部署。SSH 只读执行日志、状态 SELECT、ss、ps、vmstat、cgroup 和版本查询；观察期间生产由其他工作流程从 `e93bcd1` 切换到 `7944d76`，不是本分支部署。

材料：

- [原始实测速率](pm_ws_asset_rates_20261016.json)，原文件原样纳入版本控制，结束“文件不在 git 所以使用合成替代”的情况。
- [p31 监测](pm_ws_deep_analysis_20261017/p31_mon.log)、[p32 监测](pm_ws_deep_analysis_20261017/p32_mon.log)：用户提供的历史证据，不是本次重新运行部署脚本。
- [本次生产状态样本](pm_ws_deep_analysis_20261017/production_status_20261016T175146Z.json)、[两次 TCP 样本](pm_ws_deep_analysis_20261017/production_tcp_20261016T174521Z.json)：后者去除远端地址，仅保留时间、端口、队列和计数。时间均注明 UTC；2026-10-16 UTC 对应本地 10-17。
- `pm_ws_deep_analysis_20261017/replays.json` 保存本次回放结果。它是实验输出，非线上性能承诺。

## #60–#73 的经验与这次的区别

| 历史方案 | 已知失败或限制 | 本次处理 |
| --- | --- | --- |
| #60 接收与处理拆线程 | FIFO 256，溢出后 invalidation/整连接恢复 | 保留解耦；保留现有弹性有界队列与同连接快照恢复 |
| #61/#62 | 初期换期重建连接；#62 已改成差量订阅，效果较好但仍有长时间 slow_consumer | 保留同 socket、未迁移盘口和历史；不主动定时重连 |
| #63 热点独占/拆腿 | 历史出现 1/2/2/43 资产、处理排队和 16388 丢帧；只改变分片不保证读路径通畅 | 保持两腿和整个系列同连接；不靠拆腿“消峰”；同时检查读路径与 CPU |
| #66 合并/恢复 | 假 callback 的 2500 msg/s 不包含 TCP/TLS/上游缓冲；不能证明远端不会溢出 | 使用独立服务端进程和真实 TLS、websocket-client、FIFO、worker、Pipeline |
| #68/#70 | 计数位置和换期新资产冷启动使权重变化；重打包/迁移仍不稳 | 保留 #73 的系列权重；区分静默和缺失交付，坏窗口冻结权重 |
| #73 | 预读、关闭 Python UTF-8 扫描、0.5ms 切换有效缓解读路径；但未拿到本文件，回放合成流量；仍按 max/min 规划，实际候选只需约 1% 改善 | 使用实测重尾；按热点共租负载/峰值规划，实际改善滞后、连续确认、全局和每组冷却 |

## slow_consumer：服务器、带宽、读取、GIL 与 CPU

### 服务端 send buffer 是什么

可观察路径是：发布/扇出队列 → 服务端 WS/TLS writer → 服务端 TCP → origin/edge/网络 → 客户端 TCP → SSL/帧缓冲 → `on_message` → FIFO → worker → 盘口锁。任一环节的持续服务能力低于输入或短期调度停顿，都可能让上游发送积压。

可用流体模型 `dQ/dt = 入站字节率 - 有效发送字节率`，但队列也可能按消息数或写入 deadline 限制，不能仅用字节数推断。**Polymarket 没有在所查公开协议中公布此错误所对应组件、队列容量或每连接带宽上限。** 本次不能访问其服务端内部指标。报告不把旧脚本的 1MB、SO_SNDBUF=64KB 或新脚本的 256 帧说成交易所真实阈值。

[Polymarket 官方仓库 issue #122](https://github.com/Polymarket/rs-clob-client-v2/issues/122) 中，其他使用者也报告无 JSON/策略处理的只读客户端收到同一 1013，且部分 TCP 采样未见耗尽接收窗口；这是使用者的一手实验，**不是维护者确认的实现说明**。它进一步说明仅凭错误文字不能唯一定位本机 CPU。查阅的[公开 market 协议](https://github.com/Polymarket/agent-skills/blob/main/websocket.md)说明订阅和每 10 秒 PING，没有给出这种故障的容量合同。

因此实际修复目标是消除已知的本地阻塞、降低单连接非必要负载、稳定迁移并保留可定位证据；无法通过客户端代码证明远端再也不溢出。

### 生产只读采样

2026-10-16 17:05–17:07 UTC，生产还是 `e93bcd1`，2 vCPU、17 个 observer 线程、Python 3.12.15 / websocket-client 1.9.0，未设置容器 CPU 配额，`nr_throttled=0`。一次 docker stats 为 **89.14% CPU**（约 0.89 个核），ps 生命周期平均约 50%；两者采样定义不同。vmstat 的短窗口观察到整机忙到 **99%**，有并发备份/其他服务活动。不能从“2 核未持续打满”推出“没有 GIL/调度竞争”。

同一旧版本的状态为应用 FIFO depth 0–2、drops 0、最近队列等待约 25–731µs，而 TCP Recv-Q 一条连接约 **102059 字节**、另一条 9056 字节。它证明存在**入 FIFO 之前**的积压，不是加大应用 FIFO 就能消除。但这是旧版本的瞬时样本，不能作为 #73 关闭瞬间的追踪。

17:51:46 UTC 读取的 `7944d76` 状态（已恢复 #72/#73，启动于 17:07:02 UTC）为 **22 次迁移、1 次重连、1 次 slow_consumer**；日志关闭发生在 **17:23:14.587871 UTC**，随后 generation invalidated 的 drops=0。状态采样时接收约 1142 msg/s、各 FIFO depth/drops 都为 0、最近本地等待约 31–336µs。**该状态比关闭晚约 28 分钟，不能反推关闭前没有短暂积压。** 高频迁移在最新部署中仍有直接证据。

17:45:21/17:45:37 UTC 的两次 TCP 只读采样 Recv-Q 均为 0，CPU 约 6%；这是安静窗口，不是压力下带宽测试。`ss -ti` 的 `send ...bps` 是本机发送方向的拥塞窗口估算，不能拿它当行情入站速率或交易所每连接限速。SSH 未运行公网压测、附加 profiler 或修改 sysctl/进程。

### 读路径和 GIL：可量化的本地贡献

websocket-client 逐帧 select、读头/扩展长度/负载会多次释放和重取 GIL；CPU 密集线程可延迟读线程重新运行。[CPython 的切换间隔说明](https://docs.python.org/3.12/library/sys.html#sys.setswitchinterval)明确实际调度还取决于操作系统和运行中的内部调用。不能把“5ms 一次”当每次 I/O 固定惩罚，也不能把 0.5ms 当调度延迟上界。

本机 Python 3.12.15 微基准（10000 次，3 轮取最佳均值；它不是生产 profiling）：

| 消息 | 字节数 | websocket-client Python UTF-8 验证 | json.loads | 提取资产 |
| --- | ---: | ---: | ---: | ---: |
| 两腿 price_change | 628 | 62.25µs | 3.29µs | 0.35µs |
| 39 档 book | 3169 | 321.84µs | 16.51µs | 0.25µs |

即使 367 msg/s 每帧都是 628 字节，UTF-8 扫描约占 23ms CPU/s；考虑部分 book 后会更高。这个成本是真实的，但单独不足以解释全部断连。GIL 交接、TLS/内核等待、其他线程长时间占用和流量突发仍需组合观察。#73 已做的 64KiB 预读、处理已有帧缓冲和 SSL.pending、`skip_utf8_validation=True`、0.5ms 切换继续保留；JSON 解析负责合法 UTF-8 检查。

本次进一步让 `on_message` **只计原始帧/字节并入有界 FIFO**，不解析、不取 balancer/Pipeline 锁、不执行 I/O。解析及组归因由 worker 完成一次。回归测试同时持有两把下游锁，验证接收调用仍能完成，所有数据帧只由 worker 解析。扩容硬上限和缺口后的新快照要求不变。

预读内部接口不兼容时，现在真正回退库的 SSL-aware dispatcher，并在状态中暴露 `read_ahead_active`；不再静默保留一半自定义路径。

## 迁移为什么不收敛，如何修复

p32 从 `59.6/1019.5/191.0/500.8`、rb=1 到 `97.5/231.4/118.3/92.8`、rb=7，约 5 分 41 秒，期间 2 次 slow_consumer。**两次向量总量从 1771 降到 540 msg/s**，不是固定负载下的静态分箱问题；需求波动、交付不足、窗口变化都会改变观测权重。不能把向量变化全部归功于迁移。

#73 有三处不稳：触发门槛比较当前比值与“理想”比值，但最终候选只要改善约 1% 就接受；只有 60 秒全局冷却，没有每组冷却或连续确认；重连会提前采样，断线/恢复或积压交付不足可被当成真实冷却。此外 max/min 优先会花很多迁移改善最轻分片，而不减少最热 socket 的服务需求。

新策略：

1. 目标按“热点的共租流量、峰值负载、平方和”排序。dominant 组阈值为 `max(100 msg/s, 总量*15%)`，离开热点集合有 10% 滞后，最多占 N−1 个连接，保留共享连接。保持整系列/整市场、两腿不拆。
2. 峰值达到物理下界的 110% 内、热点共租负载低于 10 msg/s 时停止。实际峰值改善必须同时至少 10% 和 10 msg/s；热点隔离也要求实际改善通过相应门槛。**容忍几 msg/s 的冷门共租，避免为了清走近零资产反复迁移。** 如确需严格独占可配置 `REBALANCE_MIN_MSG_PER_S=0`，但会增加小额迁移，不是推荐默认。
3. 同一最高负载方向/热点集合连续两个完整采样才动作；默认 60 秒采样、120 秒全局冷却、每个已迁移组 300 秒冷却。冷启动首次 20 秒采样，下一次约 80 秒确认；重连不再绕过采样期限。每轮最多 8 个组，各组在一轮内最多移动一次，避免 #73 4 组预算下多轮清理冷门残余。显式旧配置不会被覆盖。
4. 原始消息/字节统计覆盖拒收帧；组速率只使用可靠交付窗口。断线、溢出、超过 1 秒的本地积压会冻结相关组速率，并跳过一个完整恢复窗口；不以零交付稀释热点 EWMA，不在这些窗口迁移。目录换期保留系列速率、owner、冷却；清理退出组避免速率/owner 历史无界增长。
5. 发送订阅/退订及失败关闭移出全局 Pipeline.lock。源先退订、目标后订阅仍保持；所有权原子发布时，新目标 activation 设为未来时间，发 subscribe 前设实际边界，旧目标积压帧被拒绝。只有移出/移入资产清盘口；保留资产在慢网络发送期间继续处理。同步假 socket 测试验证网络阻塞时保留盘口可取得锁、提前到达目标的快照不被接纳。

默认仍 4 条连接；`WS_SHARDS` 支持 1–8，资产数量触发的增长上限仍为 8。增加连接会增加 TLS/socket/reader/worker 开销，共用同一个 GIL，没有自动获得更多 Python CPU。独立 reader 进程可以隔离 GIL，但会引入 IPC 背压、跨进程生命周期和恢复协议；当前实测本地读取容量足够，未在本 PR 引入未经验证的多进程架构。

## 新指标与判读

`ws_receive_queues[*]` 新增原始 `received_frames/received_bytes`、两次 tick 间的 `receive_rate/receive_bytes_per_second`、`callback_max_seconds`、`parse_max_seconds`、Linux `tcp_pending_bytes`、`read_ahead_active/read_calls`。帧/字节和最大值按连接累计，重连重置；rate 是 tick 窗口而非 EWMA；TCP 指标是瞬时加密 TCP 未读字节，不含 SSL/frame_buffer 已解密积压。最大耗时包括被抢占的等待，不是 CPU profile。

新增进程 `ws_process_cpu_cores`（两次 tick 的 process CPU/wall-time）、`ws_gil_switch_ms`，以及 `ws_peak_lower_bound`、`ws_hot_groups`、`ws_hot_co_tenant_msg_per_s`、`ws_rebalance_pressure_samples`、`ws_group_cooldown_seconds`。旧 max/min 和 RATIO/TARGET_RATIO 字段保留作兼容诊断，后两个不再控制规划。

判别顺序：TCP 积压上升但 FIFO 空，优先查看读线程调度/回调/网络；FIFO 和 queue_wait 上升则看 worker/Pipeline；两端都低仍发生 1013，保留准确 UTC、连接/关闭原因及这些指标，无法排除 origin/edge/扇出队列。单次 0 队列不能证明关闭前没有阻塞。

## 离线回放、结果与限制

回放输入是**真实归因速率分布**，消息体、book/price_change 比例和 Poisson 到达仍是模拟，因为输入不是逐帧录制。实测没有系列信息，因此这一组网络回放不伪造真实市场换期；两小时换期另用明确标注的合成系列。冷门/零速率资产的盘口年龄会随时间增长，不能拿跨资产 book_age_p95 当传输延迟门槛。

服务端独立进程，客户端使用真实 websocket-client、TLS、自定义 dispatcher、原始 FIFO、worker、Pipeline/录制与 evaluator；固定两个 CPU affinity 核、两个同进程 CPU 压力线程、0.5ms GIL 间隔。外部 HTTP 禁用，References 替换为空 worker，数据库换成 NullDb，纸面账户关闭；不会访问交易所或订单接口。服务端可分别设每连接字节服务率、消息数/字节积压阈值，以区分不同 send-buffer 模型。

早期队列模型曾把每个模拟 tick 的全部生成消息先入队，再发出；它制造了与读者无关的生成批次溢出，相关探索输出**不纳入验收**。最终模型未限带宽时立即写 transport；限带宽时才在有界 publisher FIFO 等待服务。真实 TCP/SSL 背压由 transport 积压体现。关闭采用 1013 并尝试有界 flush，避免旧脚本直接 abort 丢失关闭原因。

结果表和最终检查见本文末尾。短时 localhost TLS 压测证明本机对应模型的容量和稳定性，不证明生产路径或 Polymarket 内部队列容量。

### 网络回放量化结果

所有正常容量场景的服务端/客户端 slow_consumer 均为 0，重连/丢帧也均为 0；下表单独列出故意过载的反例。负载列是结束时的组 EWMA 估算，原始 JSON 同时保留服务端全窗口实际速率和每 30 秒峰值；冷启动全窗口平均跨越迁移，不能当稳态分配。

| 场景 | 时间 | 结束时每连接负载 msg/s | 迁移 | 服务端 slow / 重连 | 本地入队至盘口锁延迟 p95 |
| --- | ---: | --- | ---: | ---: | ---: |
| #73，4 连接冷启动 | 180s | 114/370/149/52 | 3 | 0 / 0 | 8.43ms |
| 修复，4 连接冷启动 | 300s | 116/368/147/50 | 1 | 0 / 0 | 7.88ms |
| 修复，4 连接有先验 | 120s | 368/148/113/53 | 0 | 0 / 0 | 5.83ms |
| 修复，6 连接有先验 | 120s | 370/147/114/21/19/13 | 0 | 0 / 0 | 8.38ms |
| 修复，8 连接有先验 | 120s | 361/146/115/21/18/4/4/4 | 0 | 0 / 0 | 7.81ms |
| 修复，4 连接有先验，流量 ×3 | 120s | 1102/441/345/158 | 0 | 0 / 0 | 1594.57ms |
| 修复，8 连接有先验，模拟每连接 128 KiB/s | 90s | 367/147/114/21/20/4/4/4 | 0 | 4 / 3 | 11.85ms |

“p95”是每秒采样的各连接**最近帧本地延迟最大值**的分位数，不是所有逐帧延迟的 p95。回调最大耗时同样含线程调度等待。上述环境不是专用 benchmark 主机，绝对时延受同时运行的其他本地工作影响。

- #73 在约 20.3、80.6、141.0 秒各迁移一次；修复版 300 秒中只在约 **80.7 秒迁移一次**，后面超过 3 分半没有再次迁移。修复版首两个 30 秒窗峰值约 488/485 msg/s，稳定后约 358–376 msg/s，接近不可拆组的下界。两者在这个最终模型中都没有复现线上 1013，所以不能声称本次已经重现或根治 #73 的所有关闭。
- 先验布局最热组在单独连接，负载约 369/148/113/53，0 次迁移。6/8 连接的最热连接仍约 369/361，未获得峰值吞吐收益；支持按需配置，而不盲目增加默认连接数。
- ×3 输入约 **2045 msg/s**，最热连接实际约 **1104 msg/s**，约 88.7MB/120s 的模拟 WS 字节（约 5.9Mbit/s，含初始快照及结束附近流量）。TCP/TLS 接收没有 slow_consumer、没有丢帧，但 worker 本地等待 p95 **1.59 秒**、最大 **2.58 秒**。这是处理路径容量边界，不能把“读 socket 没断”说成所有新鲜度指标都达标。
- 128 KiB/s 是**人为容量反例，不是 Polymarket 的已知限速**。8 连接仍有 4 次服务端 1013、3 次重连；最大积压约 188KB、消息 FIFO 达到 256 帧，尚未达到 1MB 字节限额。这说明按消息数满也可关闭，且单个不可拆热点超出单连接能力时，加空闲连接无法修复。正常环境证明了本机可读取更高流量，不能因此排除生产网络/远端内部的瓶颈。

### 两小时模拟时钟与规划成本

驱动真实 `MarketConnections.sync/tick`，另用假 socket 和模拟时钟跑 7200 秒：实测分布冷启动只在 **80 秒移动 8 个整组（16 个资产）**，此后无迁移；90 秒后的实际峰值不超过物理下界的 **1.003 倍**，热点连接剩余共租流量约 2.57 msg/s，低于默认 10 msg/s 容忍值。重启先验、噪声以及明确标注的 5m/15m 合成换期各有独立测试；不是将无名称的真实市场冒充为合成系列。

规划器另做本机一次性计时：28/60/120 个组分别约 **2.7/13.3/35.3ms**，每轮最多移动 8 组。规划不在接收回调内执行、不持有 balancer 锁；仍消耗该进程 CPU/GIL，大规模目录不能将此数字线性外推。未增加不断重跑、优化最小分片的后台规划循环。

### 复现命令

在 `backend_api_python` 执行，使用本机 venv 和只在 /tmp 的本地测试证书。证书不纳入仓库，也不用于生产。单次回放绑定两个 CPU；可按机器可用 affinity 修改 `taskset`。

```bash
openssl req -x509 -newkey rsa:2048 -nodes -keyout /tmp/pm-ws-key.pem \
  -out /tmp/pm-ws-cert.pem -days 2 -subj /CN=localhost
PYTHONPATH=. taskset -c 0,1 .venv/bin/python scripts/pm_ws_replay.py \
  --rates ../deploy/pm_ws_asset_rates_20261016.json --seconds 300 \
  --warmup 15 --settle 90 --gil-threads 2 --shards 4 \
  --tls-cert /tmp/pm-ws-cert.pem --tls-key /tmp/pm-ws-key.pem
```

重启/容量比较加 `--prior --shards 6` 或 `8`；CPU/处理压力加 `--scale 3 --prior`；人为单连接容量反例加 `--bandwidth-kib 128 --shards 8 --prior`。比较 #73 时将 `git archive 9a6f71a backend_api_python` 解到临时目录，用本分支的已修正回放脚本加 `--code /tmp/pm-ws-73/backend_api_python`；不需要修改被测 #73 的代码。原 #73 自带 loader 不认识 `msg_per_s`，直接使用会报缺字段，不能用它产生本次实测结论。

假时钟/回归测试：

```bash
.venv/bin/pytest -q tests/test_polymarket_ws_balance.py \
  tests/test_polymarket_ws_pipeline.py tests/test_polymarket_ws_replay.py
.venv/bin/ruff check app scripts tests
```

## 最终验证与尚未证明的部分

最终全量 `cd backend_api_python && .venv/bin/pytest -q`：**4459 passed、24 skipped、59 warnings，144.21 秒**。`ruff check app scripts tests`（CI 相同范围）和 `ruff check backend_api_python`：**All checks passed**；`git diff --check` 通过。包括全局/每组冷却、实际改善滞后、热点出口滞后、断线与恢复窗口冻结、两小时实测/合成换期、慢发送不阻塞保留盘口、目标订阅前旧帧拒绝、读取不等待解析/下游锁、预读 fallback、原有快照恢复与订阅差量回归。

全量测试进程的 DATABASE_URL、AUTOMATION_TEST_DATABASE_URL、QD_TEST_POSTGRES_DSN 均显式指向 localhost 专用 `qd_automation_test`，没有改部署配置。最终原始全量日志 `/tmp/pm-ws-full-verified.log`，没有将临时测试运行输出混入版本库。一次早期全量运行发现既有假时钟 fixture 不实现 process_time，新增 CPU 指标被循环异常捕获；已改为独立导入 process_time，原假时钟换期测试和最终全量均通过。

现有证据确认并修复了本地读回调的下游锁依赖、网络发送持有全局盘口锁，以及重尾条件下的迁移控制问题。没有实际服务器 sender/edge/fan-out 队列的 enqueue/dequeue 指标，也没有 #73 原两次关闭前的同步 TCP/线程调度追踪，因而无法唯一定位那个远端 buffer、公布其阈值或承诺生产永不再出现 1013。生产仅做只读采样，未来是否部署以及远端问题的相关追踪由后续独立决策处理；本 PR 不合并、不部署。
