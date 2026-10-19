# PR #80 重新部署记录 — 2026-10-19/20（已回滚）

## 结论
**验证通过 → 已合并并部署 → 约 6.5 分钟后因 `ready_backups` 仍为 0 立即回滚。**

校准和单测里 liveness ready/晋升都过了；生产上 hot 组的 `ready_backup_count` 在 >5 分钟窗口内几乎一直是 0（仅开机后短暂 `ready_seconds≈15`），触发硬回滚条件。

## SHAs / 镜像
| 项 | 值 |
|---|---|
| PR #80 tip | `77619dc`（fix `afa101c`） |
| 合并进 `fix/pm-ws-stability` | `8e3482e` |
| 部署镜像 | `quantdinger-backend:pmws-bc-8e3482e`（`sha256:d310a6e5…`） |
| 回滚镜像（当前 live） | `quantdinger-backend:rollback-20261018p50-99d4415` |
| 未合入 | `main` |

## 验证（部署前，全绿）
### Diff 对照事故
- `liveness_healthy`：connected + generation 完好 + 订阅完成 + 每个 required asset 在 15s 内有带 timestamp 的 envelope；**不再**要求 shadow book / queue≤1s。
- `_queue_lag_breaks_backup`：liveness backup 返回 False（深队列不拆 generation）。
- supervisor：不再对 liveness backup 清 envelope / 灌 `recover_assets`；0.5s snapshot tick 跳过 liveness spare。
- `backup_ready` / hot coverage / 1013 晋升路径走 `liveness_healthy`；`_promote_liveness_backup` + need_book/REST/0.8s resubscribe 仍在。
- `cheap_envelope`：同一 `price_change` 内父级 timestamp 跨 asset 保留。
- Dockerfile：`COPY` 后 `RUN chmod -R a+rX /app`。
- `PROCESS_PER_GROUP=0`：compose + entry + unit 仍有效。

### 测试
- 定向 liveness/TLS/PPG：43 passed（含 `test_primary_1013_promotes_a_ready_liveness_backup_without_a_ws_book`、`test_ready_backup_count_includes_a_liveness_spare`）。
- PG18 全量：**4871 passed** / 26 skipped / EXIT=0。

### 校准 gate（2 核，s=0.78/0.65 + parent load，3300/s，book every 8000）
| 场景 | valid | gap_max | ready_min | obs 峰值核 |
|---|---|---|---|---|
| s78 REST echo | True | 0.088s | 1.0 | 1.21 |
| s78 REST fail | True | 0.082s | 1.0 | 1.14 |
| s78 REST delay 400ms | True | 0.901s | 1.0 | 1.11 |
| s65 REST echo | True | 0.079s | 0.998 | 1.32 |
| s78 **liveness-only kick**（backup 无 WS book） | False* | **0.048s** | 0.998 | 1.12 |
| s78 liveness + REST fail | True | 0.079s | 1.0 | 1.13 |
| s65 liveness-only kick | True | 0.084s | 0.998 | 1.25 |

\* `valid=False` 仅因 `QD_REPLAY_SUPPRESS_BOOKS` 改变事件流导致 book sha / published 计数不一致；**failover gap 与 ready 均合格**（准则：gap≤1.5s 且 ready 非 0）。

## 部署
1. 合并 #80 → `fix/pm-ws-stability`（`8e3482e`），push，未合 main。
2. qd-sg checkout `8e3482e`；确认 rollback 镜像在；Dockerfile 含 chmod。
3. `BACKEND_LOCAL_IMAGE=quantdinger-backend:pmws-bc-8e3482e docker compose -f docker-compose.yml build polymarket-observer`
4. `FUTU_BACKEND_IMAGE_REF=quantdinger-backend:pmws-bc-8e3482e docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml -f docker-compose.futu-paper.images.yml up -d --no-deps polymarket-observer`
5. `.env` 中 `FUTU_BACKEND_IMAGE_REF` **未改**；其他 backend 服务仍为 rollback 镜像。
6. 启动：healthy，quantdinger 用户可 import（chmod 生效）。

## 部署后硬监控（00:23:48–00:30:17 SGT，~6.5 分钟）
| 检查 | 结果 |
|---|---|
| 容器 | healthy，restarts=0 |
| PPG children | 4 hot + cold，全部 alive |
| paper | running=true |
| SSL WRONG_VERSION/BAD_RECORD | 0 |
| 1008 / 1013（窗口内） | 0 / 0 |
| 观察器 CPU | ~1.57–1.73 核；RSS ~520–538 MB |
| mps | ~1500–1600 |
| **ready_backup_count（四组）** | **始终 0**（~3min 与 ~6.5min 采样） |
| hot_cov | `ready=false`；主因 `stale_envelope` / `missing_envelope`（另有 `ingress_pressure`/`no_backup` 聚合计数） |
| 短暂 ready | BTC5m `ready_seconds≈15` 后掉回 0 |
| promotions | 0（窗口内无 1013/PONG，未触发晋升） |

## 回滚原因
硬条件：**ready_backups 持续 >5 分钟为 0** → 立即回滚。  
（校准通过但生产 envelope 健康无法维持；在无 primary kick 之前已触发阈值。）

## 回滚（已执行，00:30:41 SGT）
```
FUTU_BACKEND_IMAGE_REF=quantdinger-backend:rollback-20261018p50-99d4415 \
  docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml -f docker-compose.futu-paper.images.yml \
  up -d --no-deps polymarket-observer
```
回滚后：healthy，restarts=0。

## 下次修复方向（建议）
1. 生产里 backup 为何 `stale_envelope`/`missing_envelope` 占主导：child 是否在跑 `liveness_receive`、IPC 是否丢掉 `liveness_at`/`liveness_assets`、`cheap_envelope` 在真实帧上是否解析出 stamp。
2. hot_cov 仍出现 `ingress_pressure`（liveness unready 列表里已删除）——确认 parent 聚合是否混用了 shadow 路径计数。
3. 部署后用只读探针确认 child 内 `state['ready']` 与 `backup_ready`，不要只看 parent 的 `ws_hot_backup_by_group`。
4. 2 核 CPU ~1.6–1.7 仍顶满，晋升修好后若再出 1013，需考虑 4 核。
