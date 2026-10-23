# pm_maker_unlock_reset — Phase 1 交付报告（2026-10-23）

范围：paper-only；roc 批准时间 2026-10-23 12:52 SGT。基线 628ba86，目标分支 fix/pm-maker-unlock-reset，拟 PR base fix/pm-ws-stability。未部署、未合并、未操作 qd-sg/生产库、未运行生产重置。未改变 WS/Futu、PAPER_AUTO 或 live/order 开关；未接触钱包/签名/CLOB 凭据。

## 根因证据与实现

依据 `pm_maker_status_20261022.md` §1：四格均已有至少 150 个有效影子成交市场，冷启动不能再入；10-17 后 stale 合计无一天低于 .02；5m quotable 约 .074/.079（报告范围 7–9%）；开盘前盘口为零，LOOKAHEAD=0 与 preopen-only 冲突。提案 §0/4/8/9 显示毒性规则仍需单独 shadow 验证，本次不实施或启用。

1. **原因拆分**：新 market summary 记录 book_missing/book_stale/updown_skew/crossed/spot_stale，以及 reason_polls；评分增加对应 *_share，保留 stale/stale_share。默认选市只以 book_stale_share < .02 判定 stale，旧合计是全部五种 DATA_FAULTS，不是同一指标。旧 summary 或覆盖不完整的在途 summary 保守以合计作为各原因上界，不把未知当零。报价当下五种坏数据仍拒绝/撤单；没有放宽坏盘口保护。
2. **成熟格子 reentry**：录制范围内 active 少于 2 时，成熟但未正式准入的格子可以 source=reentry/full、两日 probation 重入。当前为按 cell 排序的确定性补位，并非盈利排名；同时保留 cooloff、breakers、max_active、每币上限。probation 内不因成熟负样本立即踢出；到期仍不 admissible 则退出并 cooloff，不能每天续期。正式 filters/LCB>0/成交量/as_edge_ratio 条件全部保留。reentry 报价沿用 probation_size_mult=.5，selection audit 含 source/reason/probation_until。
3. **LOOKAHEAD 对齐**：BTC/ETH 5m/15m 默认 cold_start 均为 full。显式 legacy preopen 在 LOOKAHEAD=0 的 select 中降为 full，entry 的 reason=preopen_without_lookahead 随 selection audit 保存。不增加录制范围、lookahead、订阅数或 WS 负载。
4. **5m quotable**：单独阈值 .075，取实测 .074/.079 的中点附近（并非声称有完整 p50 分布）。其他 interval 仍 .20。z_hard_max=.03、opening phases、每次报价的毒性/库存/风险保护未变；不以扩大毒性容忍换取报价。此阈值只影响选市统计，负收益格子仍不能正式 admitted；本轮靠明确授权的 probation 重入恢复纸面试跑。
5. **资金重置**：Account.reset_capital(now, reason='roc_approved_2026-10-23', clear_selection_blocks=True) 先拒绝 holdings/pending、locked/positions 绝对值超过 1e-8、未结算 maker qty 或 live 虚拟挂单；拒绝时不改状态。资金取 config.initial_capital（默认 5000），审计 before/after/reason/actor=ops_command。state.events 与独立 ledger 都有 capital_reset。
6. **安全落库**：Store.reset_capital 是独立事务，先拿 worker 同一 advisory key 202610115001 的事务锁，再 SELECT FOR UPDATE；worker 正在写、账户不存在、contaminated 名称均拒绝；账户更新、ledger INSERT、当日 daily upsert 同事务，异常 rollback。整个路径没有 retention DELETE，保留 trades/legs/旧 daily/ledger。
7. **重置后不会立刻重锁**：归档旧 selection/halt 到 ledger detail，清空 halt/cooloff/breakers，保留 truthy 的待复审空 selection，避免先 cold_start 再被成熟历史全部踢入 cooloff。保留 maker days/markets/影子统计；用 reset 时风险基线抵扣旧累计日/周/单格亏损，当前日止损标记清除，当日账户 start_equity/peak=5000。原始历史 PnL 不改写，评估新周期请从 capital_reset 的权益起算。历史 proxy settlement 后续更正仍可入账，不能保证重置后永无历史更正。

## 默认参数与回滚

以下 maker 参数均可由 `POLYMARKET_PAPER_MAKER_` 前缀 env 覆盖；env.example 与文档已同步（未改真实 .env）。

| 参数 | 默认值 | 作用/回滚 |
|---|---|---|
| COLD_START | BTC:15m,ETH:15m,BTC:5m,ETH:5m | full，与 LOOKAHEAD=0 一致 |
| REENTRY_MIN_ACTIVE | 2 | 0 关闭重入；现有 reentry 到期再退出 |
| REENTRY_PROBATION_DAYS | 2 | UTC 日期粒度，不是严格 48 小时 |
| PROBATION_SIZE_MULT | .5（保留） | base_size=10 时测试为每腿 5 股 |
| STALE_METRIC | book_stale | stale 恢复旧合计口径 |
| MAX_BOOK_STALE_SHARE | .02 | 与旧合计阈值不同口径，同数值 |
| MAX_STALE_SHARE | .02（保留） | STALE_METRIC=stale 时生效 |
| MIN_QUOTABLE_SHARE_5M | .075 | .20 恢复 5m 旧阈值 |
| MIN_QUOTABLE_SHARE | .20（保留） | 其他周期 |
| Z_HARD_MAX | .03（保留） | 本轮不改毒性容忍 |
| MAX_ACTIVE / MAX_ACTIVE_PER_COIN | 6 / 3（保留） | 重入同样遵守 |
| 账户 INITIAL_CAPITAL | 5000（保留） | 使用 POLYMARKET_PAPER_ACCOUNT_INITIAL_CAPITAL |

代码回滚：revert 本轮实现提交/恢复旧镜像即可恢复旧选择逻辑；JSONB 新字段为附加字段，无 schema migration。资金重置是审计明确的新账期，不能靠回滚镜像恢复原权益；不提供自动逆向重置，若需恢复须另审账并确保空仓。停止纸面报价可由既有 maker 开关进行，但本任务没有操作开关。

## 验证

先写失败测试，再实现：原实现上新增 9 项全部失败（死锁、preopen、不存在原因字段、不存在 reset/command），失败日志已在执行记录；独立 Git 测试提交 da4fdee。

核心验证命令（backend_api_python 下，使用 /tmp/pm-r22-venv/bin/python）：

```sh
QD_TEST_POSTGRES_SINGLE=/tmp/pm-r22-pg/bin/postgres python -m pytest tests/test_polymarket_maker*.py tests/test_polymarket_paper_account.py tests/test_polymarket_paper_capital_reset_command.py tests/integration/test_polymarket_capital_reset_single.py -q
```

结果 **165 passed**；相关 ruff 与 git diff --check 通过。覆盖：四格 markets>=150 且 filters 失败的旧空集/新 reentry/full；cooloff/recorded scope/breakers/数量上限；probation 到期不续；legacy preopen+LOOKAHEAD0；五原因计数与旧/部分覆盖回退；reentry 开盘后确有每腿 5 股虚拟订单；reset 后 begin/review 不重锁；空仓默认 5000/自定义 6000；holdings/pending/余额拒绝；contaminated 拒绝；worker 锁失败；账户不存在；写 ledger 失败 rollback；before/after/actor；旧日保留。

PG18 普通服务因沙箱禁止 TCP 和 Unix socket 创建而无法启动，改用真实 postgres --single 测试迁移与 Store 实际生成的 SQL：成功提交 5000/资本审计、ledger 约束故障后账户更新一并回滚、旧 trades/legs/daily/ledger 四行全部保留。单用户测试不等同于双客户端并发验收；互斥失败分支由单测验证。

广测首次结果：**1117 passed, 6 skipped, 7 failed, 5 errors**；全部 12 项失败/错误均为现有 socket/HTTP/WS 回环测试创建 socket 时 PermissionError: Operation not permitted（无 WS 代码变更）。6 skipped 为未配置 QD_TEST_POSTGRES_DSN 的旧 PG 客户端集成测试。日志 `/tmp/pm-maker-unlock-pytest.log`。排除这 12 项环境受阻测试后的离线广测：**1121 passed, 6 skipped, 12 deselected, 1 error**（`/tmp/pm-maker-unlock-pytest-offline.log`）；唯一 error 为既有 WS rotation fixture teardown 线程退出竞态。对 `test_polymarket_ws_round8.py` 与 `test_polymarket_ws_rotation.py` 两组原样复跑，**43 passed**，无失败。没有为让广测通过修改 WS 或跳过该竞态测试。最终核心 165 项全绿；无法宣称本沙箱内全量 socket 集成验收通过。

## 部署后操作与只读验收（本任务不执行）

由父代理审完并部署后：**先停止 polymarket-observer 或确认 paper worker 未写，运行下列命令，再启动**。锁冲突要停止，不应绕过锁或直接 SQL 改余额。

```sh
cd backend_api_python
python -m app.commands.polymarket_paper_capital_reset
```

默认 account_id=global；--account-id 可指定其他普通纸面账户，但包含 contaminated 的名称一律拒绝，没有绕过参数。生产执行前必须确认 initial_capital 的有效配置为 5000；运行只打印账户 before/after 摘要，不输出连接配置。

```sql
BEGIN READ ONLY;
SELECT account_id,(state->>'free')::numeric free,
       (state->>'locked')::numeric locked,(state->>'positions')::numeric positions,
       ((state->>'free')::numeric+(state->>'locked')::numeric+(state->>'positions')::numeric) equity,updated_at
FROM qd_polymarket_paper_accounts WHERE account_id='global';
SELECT event,observed_at,free,locked,positions,equity,detail
FROM qd_polymarket_paper_ledger
WHERE account_id='global' ORDER BY id DESC LIMIT 5;
SELECT state->'maker'->'selection' AS selection,
       state->'maker'->'halt' AS halt,
       (SELECT count(*) FROM jsonb_object_keys(state->'holdings')) AS holdings,
       (SELECT count(*) FROM jsonb_object_keys(state->'pending')) AS pending
FROM qd_polymarket_paper_accounts WHERE account_id='global';
SELECT observed_at,event,detail FROM qd_polymarket_maker_events
WHERE account_id='global' AND event='selection' ORDER BY observed_at DESC LIMIT 2;
SELECT day,stats->'maker'->>'quotes' AS maker_quotes
FROM qd_polymarket_paper_daily WHERE account_id='global' ORDER BY day DESC LIMIT 2;
COMMIT;
```

期望一个 selection 周期内录制范围 active 非空、mode=full、出现 source=reentry/probation；新账期初始 equity=5000（启动有成交后会变化），满足报价当时保护条件时 quotes>0。UTC 00:05（SGT 08:05）复审，若重启在复审时刻之前需等下一次复审。quotes>0 还依赖新鲜盘口/现货和风险条件，不能仅由 active 非空保证。检查 PAPER_AUTO 仍不存在/未开（仅检查该配置项，勿输出整个环境）；taker 仍关闭，WS 行为无故意改变。

## Phase 2 接口骨架（仅说明，无可执行开关）

后续 PR 可新增 shadow_toxicity_enabled=false，shadow_z_quantile=.80（仅前日及更早数据），shadow_min_remaining_fraction=.30，shadow_spot_momentum_window_s=3 / threshold_bps=.5，shadow_requote_cooldown_ms=300–500。只在 shadow ladder 入口/撤单路径使用，与 paper live 虚拟订单隔离；需要独立 <=1s spot 来源和样本外验证。本 PR 没有加入这些参数或规则，不能用本轮恢复纸面报价宣称毒性优化已验收。

## 交付状态与限制

工作区源码改动均在 /workspace/quantdinger/wt_maker；指定中文报告写在 /workspace/quantdinger/deploy，并同步一份进本分支 deploy 供 PR 审阅。

原 worktree 的 Git 元数据实际指向禁止修改的 /workspace/quantdinger/QuantDinger/.git/worktrees/wt_maker，git add 被只读沙箱拒绝，因此没有改写该元数据或其他 worktree。使用 /tmp/pm-maker-unlock-delivery 的独立 Git 副本保存本分支提交（基线同为 628ba86）；源 worktree HEAD 仍为原基线，代码文件是已修改状态。

CLI 到 GitHub 的 DNS 不可用；GitHub 连接器 create_blob 也被自动审批拒绝，返回 “MCP tool call requires approval, but approval policy is never”。因此不能在本环境完成 push/PR；不得将本次报告视为已开 PR。保留同名本地分支、Git bundle/patch 与 PR body，供有网络和权限的父代理继续。代码提交 `79282b0`（失败测试提交 `da4fdee`）；bundle/patch 存于 `/workspace/quantdinger/deploy/pm_maker_unlock_reset_20261023.bundle` / `.patch`，PR 正文为同目录 `.pr.md`。未 force push、未合 main、未合并、未部署。

风险：reentry 有意在 probation 下绕过正式盈利准入，历史证据仍显示负期望，纸面可能继续亏损；本轮没有改善每股经济性。两日 probation 到期若正式证据不达标会重新退出、cooloff；重入不是永久强制开仓。
