# Polymarket paper maker optimization — 2026-10-15

分支基于 `f4c68a5`，本次以 `git revert -m 1 f4c68a5` 回退 PR #63；observer 和 ws pipeline 测试已核对与 `dac13bf` 完全一致，保留 PR #64 做市逻辑。分支：`feat/pm-maker-opt-20261015`。
仅改纸面账户、虚拟 maker、参数示例和测试；未改实盘、PAPER_AUTO、钱包、签名，无密钥写入。未合并、未部署。无需前端变更：纸面账户现有状态接口返回 maker 配置、筛选历史和收益元数据。

## 行为

- `POLYMARKET_PAPER_ACCOUNT_TAKER_ENABLED=false` 默认禁止新的纸面 taker signal/attempt；代码保留，已有 pending/holding 继续按原逻辑清理、结算。可用账户 config 的 `taker_enabled` 显式开启；运行 worker 的配置变更需重启后生效。
- 到期前默认 60 秒撤掉会增加单边库存的报价，只允许不超过当前库存差的补齐腿；成交路径也检查边界，防止旧报价在下一轮轮询前开新单边。原有最后 30 秒全部停报规则仍生效，可配置。
- outcome midpoint 或参考现货快速跳价、买盘深度低于阈值或相邻快照深度骤降时撤单，并冷却。参考现货的普通小幅变化沿用原有重报价规则。
- 最近成交的虚拟 maker 被吃数量决定两腿偏斜及单侧暂停，信号超时后清空；库存过多的腿报价更低，补齐腿向 ask 下一个 tick 靠近，永不主动跨价。已有过量腿暂停规则保留。
- 动态报价和上限 `max(min_combined_bid, combined_bid - time_discount*(1-remaining/duration) - volatility_discount*volatility)`；波动为近期窗口参考价相对当前价的最大绝对变化。预算向下取整到 tick，始终小于 1；只在实际 tick 预算变化时因此重报，避免无意义丢失队列位置。
- 按 `asset:interval` 累计真实纸面成交量、完整配对数量和 markout 逆选损失，存入原纸面账户 JSON 状态；重启、市场清理后仍保留。完整配对率为 `2*paired/filled`，逆选为 `adverse/filled`。样本达到阈值后，必须同时满足配对率、逆选阈值和白名单才可新开；不合格市场仍可补齐已有库存（但受行情安全撤单约束）。
- 历史指标仅使用已观测成交及后续 mark，不读取未来行情。旧版本日统计没有币种/周期细分，不能可靠反推，因此新 cohort 初始按配置样本阈值预热；已有旧数据不伪造为新 cohort。历史是账户生命周期累计统计，并非收益预测；缺失 mark 延用 `mark_missing` 报告。
- 单边 cap 默认每市场 20、全局 100，继续预留所有挂单的最坏单边成交风险，并在 fill 时复核；配对和已有仓位结算不受新开禁令阻断。

## 参数

布尔总开关：`POLYMARKET_PAPER_MAKER_ENABLED=false`（沿用）。其余 maker 参数支持 `Maker(account, config)` 或以下 env 前缀 `POLYMARKET_PAPER_MAKER_` 加大写参数名；完整示例见 `backend_api_python/env.example`。数值必须有限且非负，关键数量/窗口须大于零；报价和、比例及白名单另有校验。

| config key | 默认 | 含义 |
| --- | --- | --- |
| combined_bid | .99 | 动态报价和最高值，必须 <1 |
| min_combined_bid | .90 | 动态报价和下限，且不高于最高值 |
| time_discount | .02 | 到期进度风险折让 |
| volatility_discount | 2 | 近期参考价相对波动折让系数 |
| pairing_seconds | 60 | 只补齐库存的到期窗口（秒） |
| stop_seconds | 30 | 全部停止报价的到期窗口（秒） |
| market_cap / global_cap | 20 / 100 | 每市场/全局单边份额上限 |
| quote_shares | 10 | 单次每腿最大份额 |
| inventory_skew | .02 | 库存差占市场 cap 比例对应的报价偏移 |
| spot_bps | 3 | 普通现货变动重报阈值（bps） |
| spot_jump_bps | 50 | 相邻现货快照跳价撤单阈值（bps） |
| jump_bps | 1000 | 相邻 outcome midpoint 跳价撤单阈值（bps） |
| thin_shares | 1 | 每腿记录内买盘总深度最低份额 |
| depth_drop | .75 | 相邻快照买盘深度下降比例阈值 |
| cooldown_ms | 1000 | 安全撤单冷却；持续薄盘会延长 |
| signal_window_ms | 30000 | 行情与被吃数量的近期信号窗口 |
| flow_shares | 20 | 某腿近期被吃份额达到该值后暂停该腿 |
| flow_skew | .01 | 两腿近期被吃数量差/flow_shares 的偏移系数 |
| history_min_shares | 20 | 启用质量阈值的 cohort 累计成交份额 |
| min_pair_rate | .5 | 最低完整配对率 |
| max_adverse_per_share | .02 | 每成交份额最高逆选损失 |
| whitelist | BTC:5m,BTC:15m,ETH:5m,ETH:15m | 允许新开的币种/周期组合 |
| max_age_ms | 2000 | 盘口最大年龄 / mark 延迟容忍 |
| mark_seconds | 5 | 成交后的 markout 观察时距（秒） |
| rebate_fee_rate | .07 | 仅 rebate 情景计算的 crypto fee equivalent 参数 |
| rebate_pool_fraction | .20 | 仅 rebate 情景计算的资金池比例假设 |

## 费率与收益核查

2026-10-15 核对官方文档：

- https://docs.polymarket.com/trading/fees
- https://docs.polymarket.com/programs/maker-rebates

官方当前 crypto maker fee 为 0；fee equivalent 按数量、费率、价格及其补数计算。每日返利按本市场 maker 权重占比乘返利池分配，不能只凭自己的 fill 推定实际返利。crypto 20% 是返利池规则，绝非对每笔自己成交的保证收入。

因此 `net_pnl` 只含配对及结算的实际纸面损益；不计 rebate，也不重复扣减已经包含在成本内的未配对结算损失。`net_pnl_including_estimated_rebate` 兼容字段现在等于 `net_pnl`；`estimated_rebate=null`，`rebate_status=unknown_pool_not_credited`。旧的固定比例值仅作为 `rebate_scenario_only` 单独展示，不进入现金或收益。报告附两条官方来源。纸面账本不模拟真实 rebate 池、每日分配或实际钱包入账。

## 回退与测试前提修复

PR #63 上线后出现订阅分片 1/2/2/43、热门市场拆双腿排队 2.6–3.6 秒、单分片丢 16388 帧、过期拒绝约 3400 次/小时（线上观测由任务提供）。本分支回退该 PR 的 observer、专项测试及对应文档，恢复 dac13bf 行为；未触碰运行环境。

两个 Postgres 集成测试的旧前提为 .99 报价和。默认时间折让在 fixture 的 10/300 秒进度使预算向下取 tick 为 .98，首腿报价从 .49 降为 .48；录制盘口只覆盖 .50，.48 同价队列未知，成交路径正确保守拒绝。账本重试及录制结算测试因此显式设置测试环境 TIME_DISCOUNT=0，恢复穿价成交前提，不改变生产默认值或安全门槛。账本报告断言同时更新为返利情景不入账、estimated_rebate=null 的现有策略。

新增回归验证默认时间折让的 .98 预算、.48 未知队列同价拒绝及 .47 穿价可成交；另以相同有效套利机会对照验证 taker 开启产生 attempt，关闭时重复 signal 不产生 attempt、事件或 pending。

## 验证

本地 PG18 测试库：127.0.0.1:5432/qd_automation_test；DATABASE_URL、AUTOMATION_TEST_DATABASE_URL、QD_TEST_POSTGRES_DSN 均指向该库，不涉及生产库。命令在 backend_api_python 下执行，使用 .venv。

- 全量 `.venv/bin/pytest -q`（以上三个数据库变量均设置）：**4325 passed, 24 skipped, 59 warnings**，183.97 秒。
- 全后端 `.venv/bin/ruff check .`：**All checks passed**。
- `git diff --check`：通过。
- `git diff dac13bf -- backend_api_python/app/services/polymarket/observer.py backend_api_python/tests/test_polymarket_ws_pipeline.py`：无差异。
- 吃单默认关闭、库存上限 20/100 不变；未改实盘/PAPER_AUTO/钱包，未写密钥，未合并、未部署。
