# PR #78+#79 部署记录 — 2026-10-19（已回滚）

## 结论
**验证通过 → 已部署 → 生产约 16 分钟后回滚。**

根因：liveness backup 在生产上始终 `ready_backups=0`，hot primary 故障时**从未晋升**（`ws_backup_promotions=0`），只能走重连 + REST，BTC5m 业务空窗最大约 **30.6s**（PONG timeout），另有多次 1013，空窗约 1.4–13s。

## SHAs
| 项 | 值 |
|---|---|
| PR #78 tip | `7d36508`（代码 `595784c`） |
| PR #79 tip | `90f9aef`（叠在 7d36508） |
| 合并进 `fix/pm-ws-stability` | `a24d80d`（#78）+ `fdb14ed`（#79） |
| 部署镜像 tag | `quantdinger-backend:pmws-bc-fdb14ed` |
| 镜像 digest（含 chmod 修复层） | `sha256:f0429a87c9c23aab1ea2669de0d341f01b263d2c5a5e290193bb1ee1932a9ee7` |
| 回滚镜像 | `quantdinger-backend:rollback-20261018p50-99d4415`（`sha256:dd3b509c7085…`） |
| 未合入 | `main` |

## 验证（部署前，全绿）
- A：`need_book` 只在 pipeline lock 内、REST book 写入后、且仅对 applied assets 释放。回归测试存在。
- B：`request` 返回 False → 立即 resubscribe；fallback 0.8s 后每 1.5s；`held_changes` 上限 256。
- C：compose 传入 `POLYMARKET_OBSERVER_PROCESS_PER_GROUP`；入口先读 `.env` 再 `setdefault`。
- 定向测试 39 passed；PG18 全量 **4866 passed** / 26 skipped。
- 校准 spot-check（s=0.78+parent load，book every 8000）：
  - REST fail：`valid=True`，gap_max **0.079s**
  - REST delay 400ms：gap_max **0.536s**
- PPG=0 路径：unit + compose 测试通过。

## 部署步骤（已执行）
1. 合并 #78、#79 → `fix/pm-ws-stability`（`fdb14ed`），未合 main。
2. qd-sg：`git checkout fdb14ed`；确认回滚镜像在。
3. `BACKEND_LOCAL_IMAGE=quantdinger-backend:pmws-bc-fdb14ed docker compose -f docker-compose.yml build polymarket-observer`
4. 首次启动失败：`PermissionError` 读 `/app/app/startup.py`（构建上下文里部分文件是 mode 600，容器以 `quantdinger` 用户跑）。已立即回滚到 rollback 镜像。
5. 在镜像上加一层 `chmod a+r`，保留同一 tag，再部署。
6. `FUTU_BACKEND_IMAGE_REF=quantdinger-backend:pmws-bc-fdb14ed docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml -f docker-compose.futu-paper.images.yml up -d --no-deps polymarket-observer`
7. `.env` 里的 `FUTU_BACKEND_IMAGE_REF` **未改**（命令行内联）。其他 backend 服务仍是 rollback 镜像。

## 部署后窗口（约 22:57–23:14 SGT，~16 分钟）
| 检查 | 结果 |
|---|---|
| 容器 | healthy，restarts=0 |
| PPG children | 4 hot + cold，全部 alive，restarts=0，dropped=0 |
| SSL WRONG_VERSION / BAD_RECORD | **0** |
| 1008 | **0** |
| paper_account.running | true，error=null；无下单/钱包日志 |
| 观察器 CPU | 约 146–186%（2 核上约 1.5–1.9 核） |
| RSS | 约 420–450 MiB |
| mps | 约 1770 |
| **backup promotions** | **0** |
| **ready_backup_count** | **始终 0**（hot_cov ready=false） |
| hot primary failures | BTC5m×3、BTC15m×2；日志均为 `ready_backups=0` |
| 业务空窗（merged_gap_ms） | 1.4s / 3.5s / 7.0s / 13.4s / **30.6s** |

`connected=false` 是顶层旧字段；`ws_connected=7`，`last_message_age≈0.02s`，流量正常。

## 回滚（已执行，23:14 SGT）
```
cd /srv/quantdinger/git/QuantDinger
FUTU_BACKEND_IMAGE_REF=quantdinger-backend:rollback-20261018p50-99d4415 \
  docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml -f docker-compose.futu-paper.images.yml \
  up -d --no-deps polymarket-observer
```
回滚后：`image=rollback-20261018p50-99d4415`，healthy，restarts=0。

## 阻断项（下次部署前必须修）
1. **liveness backup 从未 ready / 从未晋升。** 回放 kick 测不出：生产里 `_promote_liveness_backup` 要求 `state['ready']`，而 liveness 路径下 ready 上不来（hot_cov 长期 catchup/ingress_pressure/snapshot_repair）。需要：
   - 确认 backup `ready` 在 envelope/data_health 下会置位；
   - 或晋升条件改为 liveness 专用（connected + 近期 envelope），不要沿用 shadow ready；
   - 加回归：primary 故障时必须走 backup 晋升，且 `ready_backups>=1`。
2. **2 核 CPU 顶满**（观察器约 1.5–1.9 核）→ 1013 / 队列堆积。与校准一致。修晋升后若仍频繁 1013，需要 4 核或再降负载。
3. **构建权限：** Dockerfile 在 `COPY` 后应 `chmod -R a+rX /app`，避免宿主 umask/600 文件再次导致 PermissionError。

## 仍有效的回滚命令
同上；compose 已支持 `POLYMARKET_OBSERVER_PROCESS_PER_GROUP=0`（本次未用，因整镜像回滚）。
