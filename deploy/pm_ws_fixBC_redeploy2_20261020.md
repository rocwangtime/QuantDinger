# PR #81 重新部署记录 — 2026-10-20（GO，仍在线）

## 结论
**验证通过 → 已合并并部署 → 硬监控约 19 分钟通过，保持在线。**

根因确认并修复：真实 Polymarket `price_change` 的 key 顺序是 `price_changes` → `timestamp` → `event_type`；旧 `cheap_envelope` 按出现顺序 flush，第一个 asset 的 stamp=None 被丢掉 → 约 15s 后 `stale_envelope`。现按 top-level object 整段扫描，timestamp 赋给该对象内所有 asset。

## SHAs / 镜像
| 项 | 值 |
|---|---|
| PR #81 tip | `483cbf3`（fix `9c8dcb0` + probe `f56d6aa`） |
| 合并进 `fix/pm-ws-stability` | `1ebc6c3` |
| 部署镜像 | `quantdinger-backend:pmws-bc-1ebc6c3`（`sha256:cad1d240…`） |
| 回滚镜像（未用） | `quantdinger-backend:rollback-20261018p50-99d4415` |
| 未合入 | `main` |
| 当前观察器 | `pmws-bc-1ebc6c3`，healthy，~19 min+ |

## 验证
### 真实帧复审（box 公开 WS，BTC 5m）
- 捕获 8 帧；key 顺序均为 `['market','price_changes','timestamp','event_type']`。
- 新 `cheap_envelope`：每帧 2 tips，Up/Down **均有 stamp**，`missing_required=[]`。
- 合成：prod 顺序 / lab 顺序 / event_type 居中 / 单 asset → 行为正确；单 asset 时另一边不出现 → `missing_envelope` 仍会失败。

### parent hot_cov vs ws_child_ready
- `ws_hot_backup_by_group`：组级汇总，可能 `ready=false` 但 `ready_backup_count=1`。
- `ws_child_ready`：每 child 的 spare 列表（ready / envelope_age / blockers / seen_assets），**以它为准**判断 spare 是否可晋升。

### 测试
- 定向（envelope 顺序、child_ready、liveness/1013）：48 passed。
- PG18 全量：**4876 passed** / 26 skipped。
- flaky retire：`wait until the old primary is retired`（483cbf3）。

### 校准（prod key-order wire_event，s=0.78/0.65+parent，3300，book every 8000）
| 场景 | valid | gap_max | ready_live_drops | ready_min |
|---|---|---|---|---|
| s78 echo | True | 0.078s | **0** | 1.0 |
| s78 fail | True | 0.088s | **0** | 1.0 |
| s78 delay400 | False* | 1.063s | **0** | 1.0 |
| s65 echo | True | 0.093s | **0** | 0.998 |
| s78 liveness-only kick | True | 0.096s | **0** | 1.0 |
| s78 liveness+fail | True | 0.086s | **0** | 1.0 |
| s65 liveness-only | True | 0.087s | **0** | 1.0 |

\* delay400 `valid=False` 因 primary p99 380ms / published 短缺（hold flush），**ready_live_drops=0 且 gap≤1.5s**，按准则通过。

## 部署
```
# qd-sg /srv/quantdinger/git/QuantDinger @ 1ebc6c3
BACKEND_LOCAL_IMAGE=quantdinger-backend:pmws-bc-1ebc6c3 docker compose -f docker-compose.yml build polymarket-observer
FUTU_BACKEND_IMAGE_REF=quantdinger-backend:pmws-bc-1ebc6c3 \
  docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml -f docker-compose.futu-paper.images.yml \
  up -d --no-deps polymarket-observer
```
- `.env` 中 `FUTU_BACKEND_IMAGE_REF` **未改**。
- 其他 backend 服务仍为 rollback 镜像。
- 启动：01:25:49 SGT，healthy，restarts=0。

## 硬监控（01:25–01:45 SGT，~19 min）
| 时间 | BTC5m child ready | 四组 ready_count | promotions | 1013/1008 | SSL | CPU 核 | RSS |
|---|---|---|---|---|---|---|---|
| ~2 min | true（env_age≈0） | 均为 1 | 0 | 0/0 | 0 | ~0.59† | 509 MB |
| ~10 min | true | 均为 1 | 0 | 0/0 | 0 | 1.66 | 528 MB |
| ~19 min | true | 均为 1 | 0 | 0/0 | 0 | 1.38 | 542 MB |

† 启动初期采样偏低；稳态约 1.4–1.7 核（2 核机器）。

- 2 分钟内：四组 `ws_child_ready.spares[].ready=true`，`missing_assets=[]`，`envelope_fresh=true`。
- 全程 BTC5m 未掉到 0；无 hot primary failure（窗口内无 1013/PONG），故未观察到晋升；ready 路径已生产验证。
- paper_running=true；无下单/钱包日志；PPG children 全部 alive、restarts=0。

## 回滚命令（如需）
```
FUTU_BACKEND_IMAGE_REF=quantdinger-backend:rollback-20261018p50-99d4415 \
  docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml -f docker-compose.futu-paper.images.yml \
  up -d --no-deps polymarket-observer
```

## 备注
- 2 核上观察器稳态约 1.4–1.7 核，突发 1013 仍可能；建议继续盯 promotions / merged_gap。
- 未合 `main`。
