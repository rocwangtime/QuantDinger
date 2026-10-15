# Polymarket observer 队列恢复、加权分片与新鲜度分析（2026-10-15）

基线：main `114c96c`（PR #60）。本次只修改公开行情 observer、诊断指标、示例配置和测试；不修改实盘、下单开关、PAPER_AUTO 或任何密钥。不合并、不部署。生产数字“11 分钟 17 次 generation invalidation、drops 约 760–1000、shard3 消息约 shard2 的 7 倍、盘口 p95 14.5 秒”来自任务描述，本次未登录服务器采集新的生产数据。

## 失效与断连根因

PR #60 将 socket 接收和盘口处理拆开，但 `ReceiveQueue` 默认只有 256 个原始帧。worker 仍需解析、测量 token 速率、等待 Pipeline.lock、更新盘口/recording metadata/纸面历史。短暂突发或锁等待即可耗尽缓冲；按资产数量均分不能消除热点分片的突发。

旧路径有两个独立触发条件：

1. `put_nowait` 遇到 Full：设置永久 `invalid`，清空所有已排队帧，此后每个入站帧也直接丢弃并增加 dropped。
2. worker 取出帧时发现本地等待超过 2 秒：设置同一 `invalid` 并直接 `socket.close()`。

worker 的循环在 invalid 后结束；finally 清盘口、时间元数据及纸面历史。主循环 tick 发现 invalid 后记录 `generation invalidated`，再次 close；连接线程退出后按退避重新建连。因此这是应用主动关闭，并非该日志证明上游 slow-consumer 关闭。溢出后的 drops 包含被清空的帧、后来继续到达的帧及退出清理，760–1000 并不意味着有这么大的队列，也不能单靠此数区分 Full 与 >2 秒触发。反复重连又制造全订阅快照突发、重新暖机和 5–120 秒退避，可能形成正反馈。具体首次突发由 CPU、历史更新还是锁等待主导，现有证据不足以唯一确定。

## 队列修复

- 初始 FIFO 从 256 增至 2048 帧，满时按倍数扩容到 16384 帧硬上限；均可配置。保留所有增量的顺序及原始接收时间，不把增量当完整快照覆盖。
- 删除“本地排队 >2 秒就断连”的规则。交易所时间的新鲜度及配对时间差检查继续生效，排队不会被伪装成新行情。
- 达到硬上限时增加 epoch，暂停接收入队，worker 丢弃积压并清除该分片的盘口、recording metadata 和纸面历史，在**同一连接**上 unsubscribe/subscribe 请求完整快照。收到快照前，增量不能凭空重建盘口。
- epoch 在 Pipeline.lock 内重新检查，避免溢出前已出队、正在等锁的帧跨越恢复边界。接收回调只操作短队列锁；不解析 JSON，不等待 Pipeline.lock，不执行网络或数据库操作。
- 终止性的 invalid 仍用于真实关闭、成员变动、worker 异常和停止流程；这类情况仍允许正常重连。恢复命令发送失败也按 worker 异常处理。
- 状态补充各分片 `capacity`、`resyncs`、`lag_seconds`（包含入站到取得盘口锁的等待），保留 depth/drops/high_water。drops 跨连接累加；resyncs/capacity/lag 为当前连接指标。

硬上限按帧数限制，不是按字节限制。持续超过处理能力时仍会丢失观察并重复请求快照，修复不承诺无限吞吐。新默认缓冲可能增加内存使用；需要未来授权部署后观察实际帧大小、队列水位、恢复频率及处理延迟。同连接恢复已用假 socket 验证，未进行公开 WS 实流 soak。

## 加权分片

旧 `RateBalancer.propose()` 已计算 token EWMA 速率，但管理器忽略其返回值；实际 `shard_markets()` 只按资产数放置，tick 注释明确“placement stays in whole-market count bins”。因此消息量长期不均不会自行纠正。

本次采用每市场所含 token 的 EWMA 帧率之和作为权重（每 token 最小 .01，未观测/冷门市场也有正权重）。优先保留原有 owner，仅移动能减少最高/最低负载差的整个市场，保持同市场 YES/NO 在一个分片。每 60 秒检查，至少间隔 120 秒，只有 `(max-min)/mean` 改善超过 .25 才应用并累计 rebalance_count。不直接应用旧的逐 token 建议，避免拆开配对。相同订阅集合的 catalogue refresh 不重新分配，也不会绕过冷却。新市场加入/离开仍需要更新订阅；成员变化保留原有 generation 重建逻辑。

速率统计来自已解析帧、按 token 每帧最多计一次；它是处理负载代理，不是字节/CPU 耗时精确估计。硬溢出被丢帧不进入 token 采样，极端过载时可能低估热点。不可分割的单一热点市场也可能无法均衡。分片调整仍有少量必要的成员变更重连，区别于队列压力不再强迫断连。

## p95 14.5 秒的含义与成因边界

`Recorder.metrics()` 原 `book_age_p95_ms` 是所有有盘口且有合法历史时间的 token 的 **now - exchange_at** 横截面 p95，不是 WS 到达耗时 p95，也不是 evaluator 耗时。它包含冷门盘口最后一次更新后自然增长的静默年龄，且没有按新鲜度门槛筛掉这些旧盘口。配对门槛会拒绝旧行情，不会将其从此诊断分布移除。

可能贡献：上游生成到到达的延迟、最后一帧到达后的静默、局部排队/锁等待，以及频繁失效/退避导致缺失新更新。断连清除盘口也会改变分布的样本集，因此重连减少后该 p95 未必单调下降。单凭 14.5 秒不能判断新增队列是唯一原因，不能声称修复后已在生产降低该值。

为后续判别新增：

- `book_receipt_age_p95_ms`：now - 最后接收时间，显示没有新更新的时长；
- `book_arrival_lag_p95_ms`：最后更新的 received_at - exchange_at，显示到达前延迟；
- 各分片 `lag_seconds`：最后处理帧到盘口锁获取的本地等待，搭配 depth/high_water/capacity；
- 原 book_age p50/p95、token 数和 pair_rejections 保持。

前两项同样是“当前各 token 最后更新”的横截面分位数，不是每条入站消息的时间窗分位数，也不能用两个 p95 相减做精确分解。若 receipt age 高而 arrival lag/队列低，倾向静默市场；arrival lag 高而队列低，倾向上游/传输；队列与本地 lag 高则是消费积压证据。仍需部署后按 token/时间窗口采样才能定量分摊 14.5 秒。

## 验证

- 定向测试：82 passed，覆盖弹性 FIFO 顺序、硬上限、旧 epoch 拒绝、锁等待时接收线程可继续入队、原 socket 恢复及只用新快照重建、7 倍热点加权改善、市场配对、周期应用/冷却及新鲜度指标语义。
- 首次未配置数据库的全量运行：4135 passed、184 skipped、1 failed；失败为现有 trading recovery 测试访问数据库时缺少 DATABASE_URL。随后使用本机专用 `qd_automation_test` 测试库重跑；没有修改该无关测试。
- 最终全量：`cd backend_api_python && .venv/bin/pytest -q`，4300 passed、24 skipped、59 warnings，151.64 秒。仅向该进程设置 DATABASE_URL、AUTOMATION_TEST_DATABASE_URL、QD_TEST_POSTGRES_DSN，均指向本机专用 qd_automation_test 测试库；未改任何部署配置。
- `backend_api_python/.venv/bin/ruff check backend_api_python`：All checks passed。
- `git diff --check`：通过。
- 完整原始测试输出位于本次工作环境 `/tmp/pm_regen_pytest_verified.log`（临时验证产物，不包含于 PR）。
