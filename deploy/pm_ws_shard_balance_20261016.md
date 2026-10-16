# Polymarket observer：入站速率分片均衡（2026-10-16）

基线 main `3e7d3147fec2e33f8772f75060ac3a727e5484bd`，#66 已撤回；新分支 `fix/pm-ws-shard-balance-20261016`。只修改 `observer.py`、`ws_balance.py`、observer 测试和本文。没有修改 maker、钱包、PAPER_AUTO、运行配置；不合并、不部署。只进行离线验证及 localhost 专用测试库测试，没有连接生产或公开行情。

## 查明的事实与尚缺的证据

用户提供四个 shard 的实际负载约为 **444 / 597 / 1300 / 122 msg/s**，slow_consumer 发生在 shard 2，盘口新鲜度 p95 约 40s；#66 的合并计数为 0，p95 升到 104s。这些是用户提供的观测，不是本次重新采集的指标。四分片平均 615.75 msg/s，shard 2 占总输入约 52.8%，约为平均的 2.11 倍；资产数量接近不能证明输入均衡。

### 当前究竟按什么分片

- #60 使用整市场资产数，实测速率仅用于诊断；#61/#62 已接回整市场 EWMA 加权均衡。当前 main 保留 #62：首次没有样本时等权分配，`WS_ASSETS_PER_SHARD=600` 决定最低所需连接数；有样本后 `tick()` 每 60s 尝试按整个市场的 token 权重 LPT 打包，保留 YES/NO 同 shard，用等价分片匹配减少迁移，改善超过 0.25 且距离上次迁移至少 120s 才应用。不是纯资产数，也不是单独拆出热门市场。
- 关键计量位置错误：`RateBalancer.receive()` 位于 worker 的 JSON 解析之后、当前帧获取 Pipeline.lock 之前；但同一个 worker 必须完成前一帧的盘口锁等待和发布，才会取下一帧。其计数是**消费速率**，不是实时入站速率。FIFO 积压时低估当前输入，硬上限后拒收/丢弃的帧不会进入权重。热点负载因此可能被低估，使已有加权算法难以及时看到失衡。
- 旧计量把一个多 token 帧对每个 token 各记一次，市场权重相加后实际是 token-frame 速率；不同市场的单帧 token 数不同会影响与实际 msg/s 的对应关系。
- shard 2 不是算法预设的特殊分片。冷启动等权布局、已有 owner 保留、市场实际活跃度差异，以及上述滞后计量，足以产生/维持这种失衡。**现有资料没有事故时的逐市场入站速率、完整 owner 快照及连续均衡决策记录，不能证明具体哪个市场导致 shard 2 最重，或断言所有 slow_consumer 都由这一问题造成。** 新增市场级负载指标供后续获准运行时核对。

### #66 为什么看起来没有合并

查阅 #66 commit `705598cb272d595afc471fac77e32898abbbd520` 中的代码及 `deploy/pm_ws_slowconsumer2_20261016.md`，并参考 #60–#63 的 PR 说明和仓库三份前期复盘。

1. #66 中唯一的 `coalesced` 指标在 `WriteQueue`，统计持久化队列中尚未写出的同市场 research/book 记录替换。`merge_frames()` 从未增加或暴露 WS 合并计数。因此状态上的 `coalesced=0` 不能证明 WS 合并没执行；recording 或写队列没有压力时它本来就可能为 0。
2. 从历史 commit 提取原始 `merge_frames` 函数（AST，仅执行该函数）离线运行：1000 条同资产、同 side/price 更新输出 1 条；1000 条不同资产更新输出 1000 条。函数可工作，但只压缩**同批**重复价位；跨资产、不同价位和跨批更新不能直接覆盖。它在等待盘口锁之前就固定了最多 512 帧的批次；等锁期间新增积压不参与该批压缩，仍逐个压缩结果调用 Pipeline.receive。原验证刻意重复少量价位且人为给每次发布增加 3ms，不能代表真实盘口、纸面历史或共享锁争用的成本。
3. #66 完全未改分片计量：仍使用消费侧 token-frame 权重。它不能修复这里的入站失衡观测问题。不能从离线合并成功推断生产 p95 必然下降。
4. 缺少 #66 运行期间原始帧、批大小/实际重复率、CPU 和锁等待分布，**不能精确归因 104s p95**。盘口新鲜度是 `now - exchange_at`，包含冷门市场静默、上游延迟和本地排队，不等于处理延迟。不能把错误计数当作“合并从未调用”的证明。本次不重新引入 #66 的合并、REST gap journal 或丢旧留新恢复机制。

## 最小修复

### 入站测量与均衡解耦

`ReceiveQueue.put()` 在任何 FIFO 接纳/拒绝判断之前，增加实际收到的帧数，并保存原始帧引用到独立 `deque(maxlen=2048)`。回调不解析 JSON，不获取 Pipeline.lock、RateBalancer.lock，不做 HTTP、DB 或订阅命令；计量锁仅用于有界内存更新。超过采样容量自然覆盖旧样本，FIFO 和采样缓冲均保持有界；上限按帧数，不是按字节数。

`tick()` 通常每秒提取一次新增实际帧数和样本，在接收锁之外解析并识别当前 owner。未超过 2048 帧时逐帧精确归属；超过时使用最近 2048 帧的资产占比乘以该周期的实际帧数。一个帧涉及多个当前 owner token 时平分其一帧成本，重复 token 去重。无资产的控制帧、坏 JSON、非 owner 帧没有可归属权重，不强塞给某个市场。这里精确的是总入站数量，**高负载时市场占比是有界近期样本估计**，不是全量精确账本；周期内流量构成突变可能偏置，不能据此保证任意负载完全均衡。

样本累计到原有 EWMA pending，再由原有周期、hysteresis、cooldown、整市场 LPT 和 owner 匹配决定迁移。移除 worker 对 balancer 的重复计数，避免把入站与消费混加。保留原有 daily `shard_msg_totals` 的消费计数语义。

迁移继续使用现存 socket 的 unsubscribe/subscribe 差量；不拆 YES/NO、不设热门市场独占规则、不主动整批 close/reconnect。未迁移资产的盘口和队列继续保留，迁移资产仍需新 snapshot。单一不可拆市场超过单 shard 能力时仍无法完全均衡。

### 接收路径不等待盘口清理

原 gap recovery 的 `with pipeline_lock, inbox.lock` 把盘口、recording metadata 和纸面历史清理也放在接收锁内。现在 inbox.lock 仅保护有界 FIFO 清理，随后释放，再清理盘口。回调仍可能短暂竞争内存队列互斥锁，但不再等待盘口/历史清理或外部 I/O。原有缺口 epoch、同 socket snapshot 恢复、停止及真实 wire failure 行为保留。

### 新指标

- `ws_receive_queues[i].received`：当前连接实际入站帧数，含 FIFO 拒绝的帧；PONG 不计。重连后重置。
- `ws_receive_queues[i].receive_rate`：最近计量周期实际入站 msg/s；与消费计数区分。
- `ws_top_market_loads`：最多 10 个市场的 id、当前 shard 和 EWMA 帧成本贡献。多 token 帧平分，因此它是可相加的估计权重。
- `ws_shard_loads`：继续展示归属 token EWMA 的分片汇总，现在来源是入站样本；应与实际 `receive_rate`、depth、drops、queue_wait_seconds、lock_wait_seconds 一起看。

## 验证

定向命令：`cd backend_api_python && .venv/bin/pytest -q tests/test_polymarket_ws_balance.py tests/test_polymarket_ws_pipeline.py`。

**51 passed，1.23s**。新增覆盖：

- 无 worker、FIFO 仅 2 帧，输入 3000 帧仍测得 3000 msg/s；FIFO 拒绝 2998 帧，计量样本限 2048；回调禁止 JSON 解析；读取样本后不会重复计量。
- 多资产帧总权重守恒、重复 token 去重；坏 JSON、控制及非 owner 帧不产生市场权重。
- 16 个整市场合成输入 **444/597/1300/122 msg/s**，四个 FIFO 均为 2 帧且不消费。tick 仍看到真实输入结构并迁移市场，使预测负载 imbalance < 0.25；所有 YES/NO 同 shard，仅发订阅差量，未使连接 invalid。该合成输入假设每个原 shard 的四个市场流量相近，不代表生产市场流量构成。
- 假 socket 的真实 callback/worker 恢复路径：人为阻塞盘口清理，回调仍在 0.5s 断言期限内返回并计量。既有盘口锁争用、换期连续性、局部 wire failure、配对、冷却、snapshot/epoch 测试继续通过。

最终全量 `pytest -q`：**4425 passed、24 skipped、59 warnings，161.69s**（包含最后补充的盘口清理阻塞回归）。测试进程的 `DATABASE_URL`、`AUTOMATION_TEST_DATABASE_URL`、`QD_TEST_POSTGRES_DSN` 均显式指向 localhost 专用 `qd_automation_test`，不修改运行配置。

`backend_api_python/.venv/bin/ruff check backend_api_python` 和 `git diff --check` 通过。临时原始日志 `/tmp/pm_shard_focused.log`、`/tmp/pm_shard_full_final.log` 不提交。

本次没有实流 soak 或生产验证，不能声称生产 p95 已下降或 slow_consumer 已消失。PR 仅供评审，不合并、不部署。
