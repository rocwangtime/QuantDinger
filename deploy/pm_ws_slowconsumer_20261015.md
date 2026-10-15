# Polymarket slow consumer 与热门市场处理（2026-10-15）

基线 main `dac13bf`（PR #62），分支 `fix/pm-ws-slowconsumer-20261015`。只修改公开行情 observer、测试和本文；未连接生产、未修改实盘/PAPER_AUTO、未写入密钥、未合并或部署。测试仅使用本机专用 qd_automation_test。

## 证据与根因边界

用户提供上线四小时：remote_close 11、slow_consumer 10、stale 配对拒绝 5464、盘口年龄 p95 33.8s。参考工作区 `deploy/phase24_deploy.log` 的 14:25/14:33 观测：资产分布均为 `[2,42,2,2]`，shard0 已经只有一个双腿市场；累计 high_water 4805，14:33 最近帧 queue_wait 1.5529s，lock_wait 约 0.000000832s。该 queue_wait 是最近帧值，不能从单次快照推导平均等待。14:25/14:33 全局消息率约 1356/1173 每秒，失衡 2.492/0.894。日志未包含当时每 shard 的市场 ID，无法诚实地指定某个市场编号。

这些证据支持“不可拆双腿热门市场形成单 socket/worker 热点”，不支持“shard0 市场太多”或“此次积压主要由盘口锁等待造成”。原 LPT 已将热门市场隔离，但始终整市场放置，给单市场负载设置了无法突破的下限。盘口处理还会逐行重复写相同资产/方向/价位、反复把同资产添加到 changed；录制循环在检查采样间隔前复制所有盘口。Python worker、读 socket 的线程共享进程资源；这是一条可能导致读取不及时、服务端 slow consumer 的压力路径，但缺少四小时期间的 CPU profile、socket 读取延迟和服务端 trace，不能宣称已证明全部十次关闭的唯一原因。remote_close 也不能视为同一种故障。

## 修改

- 至少 64 行且前八行存在重复价位的 price_change 帧按资产、规范化方向、价位保留最终更新，changed 每资产一次；小帧走直接应用路径。同价位删除、不同方向/资产保持独立。原始事件仍传给 recording 和纸面历史，不丢成交、不跨交易所时间或快照合并，不跳过队列内事件。
- 录制采样未到间隔的盘口不再在 Pipeline.lock 内深复制；评估需要的盘口仍正常复制。分片计算缓存市场权重，避免排序/装箱时重复计算。
- 超过平均市场负载 1.25 倍的热门市场先独占 shard；若拆其 token 能把峰值负载降低至少 10%，允许双腿分别订阅到不同 socket。普通市场保持配对。仍由同一个 Pipeline 根据两腿交易所时间做 age/skew 校验。周期迁移沿用 hysteresis、cooldown、源先退订/目标订阅、仅清理迁移资产和重新取得快照，不重连其他 socket。单 token 过热仍是下限，不能保证任意负载均衡。
- `ws_hot_markets` 提供前十市场的 ID、EWMA token-frame 权重和 shard 集合；权重仍是消息活跃度代理，不是 CPU 时间。`ws_receive_queues` 增加当前连接累计 `process_seconds`、`frames_processed`、`process_max_seconds`（秒）；处理耗时包括 JSON 解析、统计、锁等待和发布，排除出队前等待。区间平均处理耗时可由两个快照的累计值差分计算。与 queue_wait/lock_wait 一起定位下一次压力，不把最近帧值冒充分位数。

## 过期判断

现有 pair_rejection 已用交易所 timestamp 与实际评估时间比较，未直接将 queue_wait 加入 age。真实排队会让交易所行情在消费时变旧，因此不能通过改用入站时间把旧盘口当成当前可用报价。本次保留 2s age、缺失/未来时间拒绝和双腿 skew 限制。33.8s p95 同时可能包含冷门市场静默，不能全部归因于热分片排队；不扩大过期线、不提高队列硬上限掩盖压力。

## 验证

新增测试覆盖热门市场独占、主导市场拆腿、普通市场保持配对、资产全集完整、重复价位最后写入/删除及不同方向、原始事件保留、录制节流前不复制、排队后的交易所旧盘口继续拒绝。原有迁移归属/时间边界/换期不重连和队列恢复测试继续运行。

本机微基准：取 `dac13bf` 的 ws_apply_event 与当前实现，1000 次应用各含 1000 条同资产同价位更新的帧，基线 0.222s、当前 0.141s，约降低 36%。这是重复写入的局部路径，不代表端到端吞吐或生产恢复保证；没有做生产或公网 soak。

最终代码验证：

- 全量 `cd backend_api_python && .venv/bin/pytest -q`：**4311 passed、24 skipped、59 warnings，136.40s**。仅为测试进程指定 localhost 专用 qd_automation_test 的 DATABASE_URL、AUTOMATION_TEST_DATABASE_URL、QD_TEST_POSTGRES_DSN。
- 定向 websocket pipeline/balance：**52 passed**。
- `backend_api_python/.venv/bin/ruff check backend_api_python`：通过；`git diff --check`：通过。
- 根目录 `ruff check .`：107 项既有问题；用 `git archive dac13bf` 干净副本比较同为 107 项，未修改无关示例/脚本。

原始验证日志位于工作环境 `/tmp/pm_slow_verified.log`、`/tmp/pm_slow_focused_final.log`、`/tmp/pm_slow_ruff.log`、`/tmp/pm_slow_ruff_root.log`、`/tmp/pm_slow_ruff_baseline.log`，不纳入 PR。
