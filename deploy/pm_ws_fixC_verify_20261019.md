# PR #78 C 方案（liveness backup）独立验证 — 2026-10-19

对象：`fix/pm-ws-b-proc-per-group` @ dcfc5b8（只读验证，未 merge、未部署）。
原始数据：`deploy/pm_ws_fixC_verify_20261019/`（gateC.jsonl、gate_summary.txt、ana1/ana2.json、脚本、pytest_full.txt）。
仅为验证做了一处本地改动：`QD_REPLAY_BOOK_EVERY`（见 verify_only_book_every.diff），**没有**推到 PR。

## 结论
**按现状不建议部署。** CPU 在 2 核上平均帧率下勉强够用，但存在一个正确性 bug：
**failover 之后，新 primary 的盘口会停住，要等下一条 WS `book` 才恢复**，而生产环境的 `book` 间隔动辄几秒到几分钟。
这个 bug 修好、并且实测 failover 业务空窗 ≤1.5s 之后，可以在 2 核上部署，前提是接受突发时段 p99 变差（见第 4 节）。

## 1. Diff 复审
已确认没问题的部分：
- backup 只做 envelope/sequence 检查（cheap_envelope）。
- `PRIMARY_FIRST` 环境变量默认关闭，由 child 从 env 读取。
- 重启退避 0.5·2^(n-1)，上限 30s，第 3 次起告警。
- IPC 丢弃：实时发 `dropped` 消息 + `_resync_dropped` → request_snapshot，parent 端用 `resync_assets` hold，任何 book（含 REST）都会清除。
- `PROCESS_PER_GROUP=0` 回滚路径逻辑未改，全量测试通过。
- 没有 round18 代码：ws_payloads.py 只是在末尾追加了函数。
- 没有碰下单、钱包、签名、CLOB 写路径。

问题：
- **(a) 阻塞问题。** `_promote_liveness_backup` 会设 `state['need_book']=set(required)` 并调用 `request_snapshot`。
  - 但 REST 快照的 `publish` 是直接 `_PIPELINE.receive(merged)`，**不会清 need_book**。
  - need_book 只在 `process_frames → _release_resync` 收到 WS `book` 时才清。
  - 结果：盘口停在 REST 快照，期间所有 price_change 都被丢弃，直到每个 required asset 都来过一条 WS book。
  - `hold_until_snapshot` 只要某个 item 还碰到 pending asset 就整条丢弃，所以空窗取 Up/Down 两边的最大值。
  - 修法：在 `request_snapshot.publish` 里，于 `_PIPELINE.receive` 之后对 merged 执行 `hold_until_snapshot(merged, state['need_book'])`，空了就 pop。
  - 另外要加兜底：如果 FeedMerge 过滤掉了 REST book，或 REST 失败，则 complete_recovery/超时（约 1.5s）后改为 resubscribe 当前 assets。resubscribe 时服务端会立刻推 book。
  - 因为 price_change 的 size 是该价位的绝对值，另一种做法是干脆不 hold。
- **(b)** liveness backup 仍会创建 ShadowBook（observer.py ~2218）。REST publish（~1676）和 subscribe 之后（~1768）仍会调 `redundant_receive`，可能把 ready 置 False，或者走旧的 shadow 晋升逻辑。建议对 liveness 模式加 guard。
- **(c)** backup 不再跑 byte_silence/data_suspect，envelope 时间戳也没用上，所以 backup 没有交易所延迟检查，只剩 data_health 采样和 22s PONG。
- **(d)** 重启计数永不清零：累计崩溃 3 次之后，以后每次崩溃都会告警并退避到 30s。建议健康运行 N 分钟后清零。
- **(e)** `_charge` 让每帧多一次 process_time + RLock，回滚路径也受影响，开销小。

## 2. PG18 全量测试
4852 passed, 26 skipped, 0 failed（312.8s，EXIT=0）。

## 3. 校准 gate（2 核，cpuset 6-7，steal 模拟慢核，bg 0.44 核模拟 celery，FULL_DRAIN，30s，每个热点组各 kick 一次 primary）

CPU 口径：观察器峰值取 10s 窗口最大值，换算成生产核数。

| 场景 | 通过 | primary p99 | 观察器峰值 | 总 CPU / 2 |
|---|---|---|---|---|
| s=0.78 + parent load 0.2，3300/s，pf0 | 3/3 | 31–38ms | 1.25–1.28 | 1.69–1.72 |
| 同上，pf1 | 2/2 | 32–43ms | — | — |
| s=0.65（无 parent load），3300，pf0 | 2/2 | 66–139ms | 1.37 | 1.81 |
| **s=0.65 + parent load，3300，pf0** | **1/2**（另有 1 次因 box 卡死作废） | **151–544ms** | 1.50–1.55 | **1.94–1.98** |
| s=0.78 + load，4950（1.5x），pf0 | 0/2 | 1.19–1.27s | 1.54–1.55 | ~1.98 |
| s=0.78 + load，4950，pf1 | 0/1 | 1.41s（BTC5m ready 0.48） | — | — |
| s=0.65，4950 | 0/2 | 3.0–3.5s | 1.56–1.57 | 2.00 |

- 所有轮次 1008=0、重启=0，BTC5m ready 约 0.998。合成场景下（每 5 帧一条 book）failover 空窗 ≤0.09s。
- 云端 agent 说的"约 1.0 核"是整段 wall 的平均值；我测的平均是 1.10–1.14，**10s 峰值 1.25–1.28**。
- s=0.65 加 parent load（最接近生产的满负荷）时，3300/s 下 2 核已基本吃满（1.94–1.98 / 2），余量≈0。
- **按生产实测的 book 间隔回放**（`QD_REPLAY_BOOK_EVERY=2000`，s=0.78+load，3300，pf0）：
  - 2/2 未通过：failover 空窗 1.6–5.3s，ready 降到 0.92。
  - p99 18–21ms，观察器 0.94–1.01 核（book 少了，解析更便宜）。
  - 这就是 (a) 的复现。真实间隔比这个回放还要长。

## 4. 生产级数据：book 间隔与帧率（box 只读公开 WS 抓取，BTC/ETH × 5m/15m，当前 + 下一期 token）

抓了两段，19:46–19:55 和 20:02–20:13 SGT。中间 box 网络卡顿过，带来一些尾部 inf。

每条帧约 612–666 字节，和生产的约 630 字节一致。

| 组 | 单连接帧率 mean / p99 / max | book 间隔 p50 / p95 / max | failover 后业务空窗（到下一条 WS book，取 Up/Down 最大）p50 | 空窗 >1.5s 的比例 |
|---|---|---|---|---|
| BTC5m | 470–560 / 1.75k / 2.2k | 0.06–0.08s / 0.84–1.1s / 39–58s | 5–7s | 78–83% |
| BTC15m | 345–405 / 1.4k / 1.85k | 0.4–0.7s / 4–7s / 290–510s | 32–56s | 83–87% |
| ETH5m | 175–235 / 0.8k / 1.1k | 0.8s / 8–11s / 136–147s | 57–73s | 89–91% |
| ETH15m | 150–220 / 0.6k / 0.8k | 1.5–3.6s / 15–33s / 818s | 约 470s 至 ∞ | 94–97% |

- **结论：book 间隔 p95 远超 1.5s。C 必须在晋升时立即用 REST book（GET /book，公开只读）或 resubscribe 来解除 hold。**
  - 代码已经在晋升时请求了 REST 快照，但没有用它解除 hold（见 1(a)）。
  - 推荐方案：REST book 落地即释放该 asset；REST 失败或被过滤时，超时后 resubscribe 兜底。
- **每秒物理帧数**（含 backup，×2）：mean 2.3–3.6k，p90 5.4–6.0k，p99 7.6–9.3k，峰值 9.4–9.8k。
  - 超过 3300 的秒数占 31–51%。
  - 超过 4000 的占 21–38%。
  - 超过 4950 的占 12–20%。
  - 前 188s 无卡顿的干净样本是上限那一端：51% / 38% / 20%。
  - 生产没有 24h 逐秒历史。p35 小时统计显示 09:19–19:20 会话均值稳定在约 900 条 merged msg/s。
- 含义：1.5x 档（4950/s）在 2 核上必失败（p99 1.2–3.5s），而这类秒数占 12–20%。突发时 primary 排队会堆到秒级，平均帧率下则没问题。这是 2 核的硬上限，C 也解决不了。

## 5. 建议

**现在：不要部署。** 先让云端 agent 修：
1. (a) 晋升后用 REST book 释放 need_book，加超时 resubscribe 兜底。加一个测试：promotion 之后 price_change 能在 REST 快照后 ≤1.5s 内恢复应用。
2. (b) 对 liveness backup 跳过 redundant_receive 和 ShadowBook。
3. (d) 健康运行一段时间后把重启计数清零。

然后用生产实测的 book 间隔重跑 D 档（`QD_REPLAY_BOOK_EVERY=2000`，或直接回放实测间隔），要求 failover 空窗 ≤1.5s。

修好后可按以下步骤部署（需要 roc 批准，我不执行）：
1. 把 PR #78 merge 进 `fix/pm-ws-stability`。
2. 在 qd-sg（`ssh -F ~/.ssh/config qd-sg`，目录 /srv/quantdinger/git/QuantDinger）上：
   - 先确认回滚镜像还在：`docker image inspect quantdinger-backend:rollback-20261018p50-99d4415`。
   - 按现有流程构建新镜像，打 tag，例如 `quantdinger-backend:pmws-c-<sha>`。
3. 只重建观察器：
   ```
   FUTU_BACKEND_IMAGE_REF=quantdinger-backend:pmws-c-<sha> docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml -f docker-compose.futu-paper.images.yml up -d --no-deps polymarket-observer
   ```
4. 回滚，两种方式：
   - 设 `POLYMARKET_OBSERVER_PROCESS_PER_GROUP=0`，再用同一条命令 `up -d --no-deps polymarket-observer`，即回到单进程路径。
   - 或把 `FUTU_BACKEND_IMAGE_REF` 设回 `quantdinger-backend:rollback-20261018p50-99d4415` 再 up。
5. 部署后检查（前 30 分钟密集看，之后每小时一次）：
   - `docker stats`：观察器 + 全机 CPU，两核合计持续 >1.8 就要警惕。
   - 内存：可用约 2GB，每个 child 约 60–70MB。
   - 状态里看：`ws_group_processes` 是否都存活、`ws_feed_restart_alert`、`ws_ipc_dropped` / `ws_ipc_resync_assets`、1008/1013/1001 计数、`backup_promotions` 和 `failover_ms`。
   - 每次晋升后各 asset 的 book 新鲜度：need_book 应在 ≤1.5s 内清空。
   - BTC5m ready 比例 ≥0.99，小时级业务空窗统计。
   - 确认仍是 paper，没有任何下单或钱包调用。

## 其他
- C_s65L_3300_pf0_t1 因 box 卡死约 295s 作废（全部 cgroup CPU≈0），已用 r2/r3 重跑。
- qd-sg /tmp 清理仍待 roc 批准，未执行。
