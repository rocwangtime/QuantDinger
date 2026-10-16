# Polymarket observer slow-consumer 第二轮修复（2026-10-16）

基线 main `9c009c5`；已核对 `observer.py`、`ws_balance.py` 与 `dac13bf` 无差异。分支 `fix/pm-ws-slowconsumer2-20261016`。只修改 `observer.py`、observer 测试及本文；不修改 `maker.py`、`paper_account.py`、实盘、PAPER_AUTO、钱包或配置，不写密钥，不合并、不部署。验证只使用离线假 socket、合成重放及 localhost 专用测试库，没有连接公开行情或生产服务。

## 证据与范围

用户提供：19:50 后断开 172 次（remote_close 101、slow_consumer 37），02:00–03:00 断开 80 次，盘口年龄 p95 约 24.8 秒；这些不是本次重新采集的生产数据。参考仓库内 `pm_ws_stability_20261015.md`、`pm_ws_regen_20261015.md`、`pm_ws_rollover_20261015.md` 及用户提供的 #60/#61/#62/#63 结果。#62 的换期差量订阅和整市场加权分片保留；不引入 #60 的队列满即断连、#61 的换期整批重连，或 #63 的热门市场拆腿/特殊独占规则。

现有接收回调已经只调用 `ReceiveQueue.put(raw)`。瓶颈仍包括逐帧解析、逐帧等待 Pipeline.lock 和逐帧发布盘口/纸面历史。旧硬上限恢复会暂停接收、丢弃全部积压、清空整个分片并重新订阅整个分片。本次针对这两条路径；不能用离线结果证明全部远端断连已消失，也不能据此声称生产 p95 已下降。

## 实现与数据一致性

- 接收回调仍只入原始帧队列；JSON 解析、资产识别、合并、REST 和订阅命令都在 shard worker。回调不等待 Pipeline.lock，不做网络/数据库工作。
- worker 每批最多取 512 帧，在盘口锁之外解析、统计原始消息和合并。按资产保留最后一个完整 snapshot；snapshot 后的增量按 `(side, price)` 保留最后一条，包括 size=0 的删除。不同价位的增量不能直接互相覆盖，否则会伪造盘口。合并为 O(更新条数)，保留各条最终更新的原始接收时间和交易所 timestamp；不把不同资产或不同价位统一盖成新时间。最后在一次盘口锁内发布该批合并结果。
- 队列仍沿用 2048→16384 的现有配置，不扩大容量。到硬上限后淘汰最旧原始帧并接纳新帧，不暂停接收、不使连接 invalid、不增加连接 epoch、不 close socket。
- 被淘汰的原始帧进入有界 loss journal，由 worker 解析以定位受影响资产。journal 容量为 `max(1024, ceiling)`；journal 耗尽或丢失帧无法解析时，只能保守恢复整个当前分片，仍不重连。接收线程不可能在“不解析且有限内存”的前提下无限精确保留被丢帧的资产身份。队列和 journal 按帧数有界，并非按字节限制。
- 只清除受影响资产的旧盘口、recording metadata 和纸面历史，通过公开 `POST /books` 批量请求这些资产的完整 REST 快照。其余资产保留盘口及排队更新。REST 失败也不关闭 socket。
- REST 和 WS 没有可用的共同序号，不能把请求期间的 WS 增量直接拼到 REST 结果上。REST 完整盘口作为独立快照发布，metadata 标为 `rest`；随后在同一 socket 上只对受影响资产 unsubscribe/subscribe，记录订阅时的接收时间边界。订阅之前排队的旧帧不能跨界发布；新的增量也必须等 WS 完整 snapshot 后才可应用。REST 失败时资产保持缺失直到 WS snapshot，不允许增量凭空建簿。
- 若 worker 等 Pipeline.lock 期间发生溢出，该已取出的批次也记入恢复 journal；不会默默漏掉该批涉及的资产。真实连接失效、停止及 wire send 失败继续沿用原有连接处理。
- `shard_markets` 和 `ws_balance.py` 均未修改。同市场 YES/NO 同 shard；保留差量换期订阅、EWMA 整市场加权、hysteresis 和 cooldown。已有配对和加权负载测试继续执行。

合并会减少中间状态及纸面历史中的重复价位观察，这是有意的过载控制；最终价位、删除和 snapshot 边界必须与按序处理一致。`Pipeline.messages` 现在统计合并后的发布事件，DailyStats 和 RateBalancer 对正常出队帧仍逐原始帧统计；被淘汰且未正常处理的帧不计入 token 权重，极端溢出时权重仍可能低估热点。

## 离线验证

重放入口已包含在 `tests/test_polymarket_ws_pipeline.py::test_sustained_2500_messages_per_second_no_slow_consumer`，无需新脚本或录制文件。运行：

```bash
cd backend_api_python
.venv/bin/pytest -q -s tests/test_polymarket_ws_pipeline.py tests/test_polymarket_ws_balance.py
```

使用 fake WebSocketApp 的真实 on_message 回调、真实队列/worker/Pipeline。对 worker 的每次 Pipeline.receive 人为增加 3 ms 处理耗时：逐帧串行处理的理论上限约 333 msg/s，明显低于输入。分别对 1 个资产和 48 个资产（24 个完整配对市场、同一个 shard）各持续 4 秒输入 10,000 条价位更新，按每 10 ms 25 帧投递；断言 JSON 不在接收调用线程解析，所有资产的最终盘口等于各自最后更新，发布次数少于原始帧的一半，队列有界，0 drops、0 resync、0 reconnect、0 slow_consumer，socket 未关闭。

最近一次定向结果：**52 passed，9.69 秒**。单资产重放 **2499.9 msg/s**，high_water **25**，最后一次本地 lag **3.33 ms**；48 资产重放 **2499.7 msg/s**，high_water **400**，最后一次本地 lag **300.98 ms**。两组 drops/resync/reconnect/slow_consumer 均 **0**。lag 是末条指标，不是 p95。这里的 slow_consumer=0 是离线计数和接收吞吐断言，不是远端服务器压力测试结论。

其他覆盖：独立价位保留、删除、snapshot 覆盖旧增量、多资产和时间戳保留；盘口锁阻塞时接收仍入队；硬上限丢旧留新；3000 帧突发的 journal 内存上限与保守恢复标志；REST 成功/失败均不重连、只恢复丢失资产、未受影响的另一腿保留、拒绝 REST/WS 重叠增量后由 WS snapshot 恢复；显式连接 generation 过期帧拒绝；整市场分片及差量换期回归。

全量命令只给测试进程设置 `DATABASE_URL`、`AUTOMATION_TEST_DATABASE_URL`、`QD_TEST_POSTGRES_DSN`，均指向 localhost 专用 `qd_automation_test`；未改部署环境或其他配置。全量 `pytest -q`：**4330 passed、24 skipped、59 warnings，147.13 秒**。

`backend_api_python/.venv/bin/ruff check backend_api_python`：All checks passed。`git diff --check`：通过。临时原始日志：`/tmp/pm_sc_focused.log`、`/tmp/pm_sc_full_final_verified.log`，不提交测试日志。

## 结论边界

本次证明在上述有额外处理成本的 2500 msg/s 离线负载下，接收不解析、不被盘口锁阻塞，合并消费能跟上输入；强制溢出不会由队列压力主动断开 socket。未做实流长时间 soak、服务器 CPU/GIL 分析或网络故障注入，不能保证上游永不报告 slow_consumer，也不能保证持续超过处理能力时不丢帧。REST 的有界请求耗时可能继续增加 worker 积压，journal 耗尽时会扩大恢复范围；这时优先保证数据不跨缺口拼接和 socket 持续接收。盘口年龄 p95 仍包含冷门市场自然静默，需未来获准部署后结合 queue lag、drops、resync 和断连原因观察。本 PR 不部署。
