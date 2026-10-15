# Polymarket observer 第三轮：换期增量订阅（2026-10-15）

基线 main `38b5b61`（PR #60/#61）；分支 `fix/pm-ws-rollover-20261015`。参考 `deploy/phase23_deploy.log`、`deploy/pm_ws_regen_20261015.md`。仅修改公开行情 observer 和纸面 maker、测试；没有登录生产、修改实盘/PAPER_AUTO、写入密钥、合并或部署。生产维持用户说明的回滚版本 `8b2d424`。

## 根因及修复

生产记录的 16 次重连全部伴随应用 generation invalidation，时间接近五分钟市场换期。#61 的 `apply_assignment()` 对成员变化主动 close，并清空受影响分片全部盘口/历史；市场换期与加权 rebalance 都会触发该路径。旧版本的订阅差量辅助函数虽然还在，却没有被这个管理器使用。

现在成员变化只发送 `old-new` unsubscribe、`new-old` subscribe：所有源先退订，再在目标订阅。存量 socket、接收线程、处理 worker、队列及不迁移资产的盘口/历史保持连续。只有退出或迁移资产会清理盘口、recording metadata、纸面历史。所有权发布和消费者使用同一个可重入 Pipeline.lock，订阅前发布 owner，订阅时记录资产接收时间边界，拒绝目标队列里迁移前的旧帧。源 worker 也只能发布当前所有资产。迁移后增量不能凭空生成盘口，仍需新快照。

真实网络关闭、发送失败、worker 异常仍按单连接恢复；订阅发送失败不会关闭其他分片。停止时正常关闭连接。硬队列溢出的同连接快照恢复仍保留，发送命令与成员更新使用同一 state lock。没有通过扩大 16384 硬上限掩盖拥塞。

原加权算法只允许从当前最高负载分片搬一个市场到最低负载分片，遇到需要交换大市场的分布会提前停止。现在比较保留 owner 的布局与按整个市场 token EWMA 权重降序放置的 LPT 布局，只有后者改善时采用，并用动态规划匹配等价分片，尽量保留 owner。同市场 YES/NO 仍在一起；未观测的新 token 暂用已观测正速率的中位数，避免换期资产因零权重集中到同一分片。周期调整仍受 hysteresis 和 cooldown 限制。

rebalance 计数统一在实际应用 owner 迁移时更新：周期 rebalance 和 catalogue 更新导致的存量资产迁移都累计一次；初始分配、只新增/移除、重复相同 assignment 不算。状态 `rebalance_count` 从 0 初始化，与 UTC daily/session 统计同步增加。

纸面 maker 旧逻辑只有零单边库存且两腿 remaining 都大于零才保留报价。已经正确暂停过量腿、保留配对腿后，下一次轮询仍因库存不为零而整组撤挂。现在允许保留这种有效配对报价，保留 joined/排队位置/成交去重状态；价格或参考价显著变化、行情过期、停止报价时间、风控拒绝仍会正常撤单。换期只影响退出市场及新增市场，不会主动重建其他市场报价。

## 状态指标及限制

`ws_receive_queues[shard]` 继续提供 depth/drops/high_water/capacity/resyncs/lag_seconds，并增加：

- `queue_wait_seconds`：最近出队帧从入站到出队的等待，单位秒。
- `lock_wait_seconds`：最近处理帧等待 Pipeline.lock 的时间，单位秒。
- `lag_seconds`：最近有效发布帧从入站到取得盘口锁的本地总等待，单位秒。

这些是当前连接最近帧的值，并非分位数或峰值；应结合 depth/high_water 和原有新鲜度指标解读。冷门盘口自然静默仍会抬高 book_age p95。消息权重取自处理过的帧，硬溢出丢帧会低估负载；单一不可拆市场超过分片处理能力时不能保证完全均衡。修复不宣称已把生产失衡 2.32 或 drops 降低；没有进行实流 soak，不能替代获准后的生产观察。

## 回归验证

新增/调整测试覆盖：市场循环 rediscovery 同 socket 订阅差量；原接收线程及原 worker 处理换期新快照且 reconnect_count 为 0；连续成员变化保留原盘口和积压；迁移前目标旧帧被拒绝；全部源退订先于目标订阅；大市场交换脱离局部最优；市场配对/owner 保留/周期冷却；rebalance 状态及 daily 计数一致；发送失败仅影响单 shard；队列和锁等待指标；纸面单边库存配对报价跨轮询及其他市场加入不撤挂。既有硬溢出同 socket 恢复测试继续执行。

所有数据库验证只使用 localhost 专用 `qd_automation_test`，测试进程设置 DATABASE_URL、AUTOMATION_TEST_DATABASE_URL、QD_TEST_POSTGRES_DSN，没有改部署配置。一次全量运行的无关 seed migration 查询发生 statement timeout，单独重跑通过（1 passed，2.77s）。旧的“换期必须重连”测试已更新为本轮要求。

最终全量：`cd backend_api_python && .venv/bin/pytest -q`，**4306 passed、24 skipped、59 warnings，148.05 秒**。定向测试 69 passed。

`backend_api_python/.venv/bin/ruff check backend_api_python`：All checks passed。`git diff --check`：通过。额外根目录 `ruff check .`：107 项既有错误；用 `git archive 38b5b61` 的干净副本验证同为 107 项，位于 docs/examples、mcp_server、scripts 等原有文件，不扩大本轮修复范围。

原始验证日志：工作环境 `/tmp/pm_rollover_focused_final.log`、`/tmp/pm_rollover_pytest_verified2.log`、`/tmp/pm_rollover_ruff.log`、`/tmp/pm_rollover_ruff_full.log`、`/tmp/pm_rollover_ruff_baseline.log`。临时日志不纳入 PR。
