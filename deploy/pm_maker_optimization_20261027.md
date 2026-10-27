# Polymarket 纸面做市优化 — 2026-10-27

> 纸面 only。没有钱包、签名、真实订单、CLOB 凭据、地区绕过。没有改 Futu / HK-US，没有改 PAPER_AUTO。没有合并，没有部署。
> 基线分支 `fix/pm-ws-stability`（`ad017a0`）。本文件是给 roc 审阅用的。
> 单位：¢/股，除非写明是美元。m60 = 成交优势 + 成交后 60 秒 markout。LCB 是按市场聚类、90% 单侧下界（`lcb_alpha=.10`），和选市用的是同一个函数。

## 0. 结论

**不要把现有对称做市重新打开，也不要上新的报价规则。**

仓库里没有 72 小时盘口，本环境也没有生产库，所以本轮没有跑出一组新的、下界大于 0 的样本。已发表的回放（`pm_maker_redesign_proposal_20261023.md`，10-20..10-22，四个格子）里，最好的可事先撤单组合 A+B+C 仍是 **m60 −2.42¢**，90% 区间 **−2.80..−2.05**，整段为负。线上影子和重置后的纸面成交落在同一区间。在这组证据上，把准入放宽到“必须有报价”就是继续做负期望交易。

本 PR 只做三件与证据绑在一起的事：

1. **负 LCB 不再自动 reentry。** 10-26 起四格都是 LCB < 0，probation 结束后进了 3 天 cooloff，所以当天真实报价是 0。旧默认会在 cooloff 结束后再挂两天半仓。那两天挂的是同一本已经被影子否定的书。现在默认不挂。影子梯子照旧跑（大约每天 220 万条），不占资金。
2. **记下 `touch_stable` 影子分档**（安静盘口、非扫单、挂满至少 1 秒的第 0 档）。只记账，不改变真实报价。这是还没被完整重放否定、但也还没被证明为正的下一档实验。
3. **只读筛选脚本** `python -m app.commands.polymarket_maker_policy_lab`。`go: true` 只是报告字段，不会改配置，也不会下单。

`touch_stable_live` 和 `admission_shadow=touch_stable` 默认都关着。在 lab 对生产导出给出 `go: true` 之前，不要开。

## 1. 诊断（对照代码，不是新的回放）

重置点 2026-10-23 13:21 SGT，权益从 5000 到约 4909，四天大约 −91 美元。Phase2 报告在 10-24 上午读到的数：

| 窗口 | 成交 | 分解净额 | 大约 ¢/股 |
|---|---|---|---|
| 10-23 重置后 | filled_join 990 | net −56；结算 −30.75 | 约 −5.7 / −3.1 |
| 10-24 至查库 | filled_join 35 | net −1.45 | 约 −4.1 |

成交优势大约 +0.9¢/股，60 秒逆向大约 −2 到 −5¢/股。亏损来自被扫穿，不是手续费。Maker 费是 0。`rebate()` 与文档一致，按 `0.07 × p × (1−p) × 0.20`，p=0.5 时大约 **0.35¢/股**，而且**不进现金、不进 PnL**。

选市为什么是 0 张真实单（`maker.select`）：

- 正式准入要 filters、LCB > 0、≥150 个有成交市场、≥1000 股、`as_edge_ratio ≤ 0.7`。四格 LCB < 0，所以 `admissible` 为假。5m 的可报价时间大约 8%，刚过 `min_quotable_share_5m=.075`；15m 大约 37%，但 LCB 更差。
- Phase1 的 reentry 在 `active < 2` 时忽略 LCB，用半仓 probation 两天。10-26 到期仍不可准入，于是 cooloff 3 天。cooloff 期间候选被排除，所以真实报价是 0。影子报价不看 `selection.active`（`Maker.shadow` 只看盘口和时段），所以影子仍在约 220 万条/天。
- 合格影子大约 **−484 美元/天**。这和 10-22 状态报告里 10-17..10-22 合格影子合计 −1882 美元 / 57049 股（**−3.30¢**，优势 +0.04¢，as60 **−3.09¢**）是同一件事。

报价为什么盖不住逆向选择（`maker_model.half_spread` / `constrain`）：

- `h_min=.01`。毒性项是 `max(0.01, Z − 费率护城河)`。p=0.5 时护城河约 1.75¢，Z 很小的时候半价差被压在 1¢。
- `open_max_behind_ticks=3`：模型价离最佳买价超过 3¢ 就**不挂**，而不是挂深。真挂出去的单都在盘口附近，优势只有大约 0.5–1¢。
- 回放里 98% 的影子股数是扫单，as60 −4.06¢；非扫单 as60 −1.13¢，m60 大约 0（约 1.46 万股）。挂深 1–3 档结算仍是 −3.3 到 −3.9¢，因为只有价格跳过去才成交，优势同时被吃掉。

所以“把门槛放宽、让它继续报”不会提高赚钱的概率。它只是把已经测过的负期望再做一遍。

## 2. 为什么没有正期望变体可以上线

下面全是已发表的样本外结果，不是本轮新跑的数。本轮没有盘口文件，不能假装又测了一遍。

| 规则 | 留下的成交 | m60 ¢ | 90% 区间 |
|---|---|---|---|
| 全部影子 | 1.00 | −3.52 | — |
| A 剩余时间 < 30% | 0.84 | −2.96 | −3.34..−2.60 |
| B Z > p80 | 0.77 | −2.76 | −3.11..−2.42 |
| C 3 秒现货逆向 > 0.5bp | 0.64 | −2.83 | −3.26..−2.40 |
| A+B+C | 0.46 | −2.42 | −2.80..−2.05 |
| 多特征 OLS 最好的 10% | 0.10 | −1.75 / −1.41 / −1.55 | 三折都为负 |
| 库存上限、第二腿限价、挂深 | — | 约 −2.2..−3.9 | 下界仍 ≤ 0 |

返佣上限 0.35¢ 填不上这 1.4¢ 以上的缺口。规则 C 还要 ≤1 秒的现货；线上 REST 中位大约 5.2 秒，不够用。Taker 套利和公允偏离两个方向保持关闭，这里不恢复。

`touch_stable` 想逼近的是“非扫单、而且单子已经挂住”的那一档。已发表的非扫单 m60 大约是 0，不是显著大于 0；而且那是成交之后才能看见的子集，不是下单前能保证只吃到的流。悲观排队下，排在后面的单本来就几乎只在被扫穿时成交。所以这一档**可能仍然 ≤ 0**。它现在只写入 `market_summary.shadow.touch_stable`，供 lab 证伪。在 `go: true` 之前，它不是策略。

## 3. 改了什么

默认行为：真实报价路径、半价差、延迟、`complementary_prints=verify`、数据质量门槛（`book_stale_share < .02`、5m 可报价 `.075`、其他周期 `.20`）都不变。`touch_stable_live=0`，`admission_shadow=eligible`。

| 参数 | 新默认 | 作用 |
|---|---|---|
| `REENTRY_NEGATIVE_LCB` | `0` | 成熟格子 LCB ≤ 0 时不再 reentry。`1` 恢复 10-23 的半仓重入 |
| `ADMISSION_SHADOW` | `eligible` | 选市打分用的影子桶。只有 lab 通过后才考虑改成 `touch_stable` |
| `TOUCH_STABLE_MS` | `2000` | 最佳买价连续未变的毫秒数，用来打标签 |
| `TOUCH_STABLE_REST_MS` | `1000` | 成交时单子至少已经活了这么久（含 250ms 挂单延迟） |
| `TOUCH_STABLE_LIVE` | `0` | `1` 才让开仓单等到盘口安静再挂。对冲腿不走这条 |

另外：

- 影子成交审计在原有 15 个字段后面追加 `at, rest_ms, sweep, spread_ticks, touch_age_ms, touch_stable`。旧消费者只读前缀的话不受影响。事件多了 `cell`。
- 真实成交审计多了 `sweep / rest_ms / touch_age_ms`，不进 PnL。
- 某个格子**现在**已经 `admissible` 时，旧 cooloff 不再挡住它。负 LCB 的格子不会因此被放行。
- `selection.config_hash` 和当前配置不同时，清掉 cooloff。库里还没有这个字段的旧状态**不会**被清掉，所以这次部署不会提前结束已经在走的 cooloff。
- 每天复审会在 selection 事件里写下 `reentry_skipped`：按旧规则本来会重入、现在留下不挂的格子。

`touch_stable` 的定义（`qualifies_touch_stable`）：第 0 档、当时 `eligible`、成交价等于我们的买价（不是打穿）、`rest_ms ≥ 1000`、从最佳买价上次变动到这笔成交至少 2000ms。扫单和字段缺失都不算。

## 4. 预期效果和区间

**没有一个新变体的 90% 下界被测成大于 0。** 不要把本 PR 读成“预期转正”。

| 变化 | 预期 | 区间 |
|---|---|---|
| 停止负 LCB reentry | 下一次 cooloff 结束后，不再用半仓把同一本负期望的书挂回去。避免的是已观测到的亏损，不是新的 alpha | 合格影子约 −3.3¢/股（10-17..10-22，57049 股）；A+B+C 的 90% 区间整段低于 −2¢。重置后纸面大约 −3 到 −6¢/股。影子合格层大约 −484 美元/天 |
| `touch_stable` 只记账 | 真实报价频率不变，仍由 LCB 决定，目前是 0 | 未测。最近的已发表参照是非扫单 m60 ≈ 0，不能当成正期望 |
| 正 LCB 可以穿过旧 cooloff | 只有打分桶自己过了准入才生效。以现在的 eligible 桶，四格都过不了 | 不适用 |

报价频率：影子层保持现状。真实层在 eligible LCB 转正之前保持 0。这是有意的。用 `REENTRY_NEGATIVE_LCB=1` 换频率，等于再做一遍 −2¢ 以下的书。

## 5. 爆炸半径

| 改动 | 碰到谁 | 碰不到谁 |
|---|---|---|
| 负 LCB 不 reentry | 下一次复审（UTC 00:05）里，录制范围内、样本已满、LCB ≤ 0、而且当前 active 少于 2 的格子。现在四格正在 cooloff，这条要等 cooloff 结束才看得到差异 | 已经 `admissible` 的格子；冷启动（样本 < 150）；影子梯子；账户现金 |
| `touch_stable` 字段 | `market_summary` / `shadow_fills` 的 JSON 变大。每个市场多个一个全 0 或有数的桶 | 真实订单价格、数量、延迟、成交判定 |
| `TOUCH_STABLE_LIVE=1`（默认关） | 开仓单多一个 `touch_unstable` 跳过原因 | 默认部署 |
| `ADMISSION_SHADOW=touch_stable`（默认关） | 选市改看另一个桶。旧 summary 没有这个桶时，那些市场不算样本，不会被当成 0 亏损去拉低均值，只是不计入 | 默认部署仍看 `eligible` |
| 配置哈希变化时清 cooloff | 只有状态里**已经写了**旧 `config_hash` 且随后又改了 maker 参数 | 这次上线的第一笔复审（旧状态没有该字段） |
| lab / replay 的 `before_settle` | 不传回调时，回放和以前一样 | 实盘进程不调用 lab |

Futu、PAPER_AUTO、钱包、签名、CLOB、真实下单开关都没改。

## 6. 回滚

- 恢复旧的“负 LCB 也重入”：`POLYMARKET_PAPER_MAKER_REENTRY_NEGATIVE_LCB=1`，然后重启 paper worker。不需要改代码。
- 其余新参数保持默认即可，行为与改前的报价一致。
- 代码回滚：revert 本 PR。多出来的 JSON 字段是附加的，没有 migration。已写下的 `touch_stable` 桶可以留在历史 summary 里，旧代码不读它。
- 不要为了回滚去改权益。本 PR 不动资金。

## 7. 部署后看什么

先不要改任何新参数。部署后的第一天应该仍然是：真实 `quotes` 为 0（四格还在 cooloff，或者 cooloff 结束后 `reentry_skipped` 里有它们），影子 `shadow_quotes` 仍是每天百万级，权益不再因为新的 maker 成交往下走。

每天 UTC 00:05 的 selection 事件：

- `reentry_skipped` 应包含 LCB 仍为负的成熟格子。
- `active` 在四格 LCB 转正之前应继续没有这四个格子。
- `scores.*.lcb` 仍 ≤ 0 时，不要手工把它们塞回 `active`。

影子桶（部署之后新结算的市场才有非空的 `touch_stable`；旧 summary 没有这个键）：

- `touch_stable` 的股数应该明显小于 `eligible`。如果它接近 0，说明安静盘口上根本没接到非扫单，这条实验没有样本，继续关着。
- 看每股 pnl 和 as60，不要只看一两天的结算残差。

账户：`free + locked + positions`。maker 成交应为 0，直到某一桶真的过了准入。

## 8. 你需要跑的只读导出

本环境没有生产库。下面两条都是 `SELECT`，不要包在会写库的事务里。时间按需要改。

按格子拆开“缺失 / 陈旧 / 可报价”，确认 19% 到底是 `book_missing` 还是 `book_stale`。选市门槛只看 `book_stale_share < 0.02`，不看两者之和。

```sql
SELECT detail->>'cell' AS cell,
       count(*) AS markets,
       sum((detail->>'polls')::numeric) AS polls,
       sum((detail->>'book_missing')::numeric) / nullif(sum((detail->>'polls')::numeric), 0) AS book_missing_share,
       sum((detail->>'book_stale')::numeric) / nullif(sum((detail->>'polls')::numeric), 0) AS book_stale_share,
       sum((detail->>'quotable')::numeric) / nullif(sum((detail->>'open')::numeric), 0) AS quotable_share,
       sum((detail->'shadow'->'eligible'->>'pnl')::numeric) AS eligible_pnl,
       sum((detail->'shadow'->'eligible'->>'shares')::numeric) AS eligible_shares,
       sum((detail->'shadow'->'touch_stable'->>'pnl')::numeric) AS touch_stable_pnl,
       sum((detail->'shadow'->'touch_stable'->>'shares')::numeric) AS touch_stable_shares
FROM qd_polymarket_maker_events
WHERE account_id = 'global' AND strategy = 'maker_fair_value_v3' AND event = 'market_summary'
  AND observed_at >= TIMESTAMPTZ '2026-10-20 00:00:00+08'
GROUP BY 1
ORDER BY 1;
```

影子成交（给 lab）。部署本 PR **之前**的事件没有 rest/sweep/touch 字段，只能给 `eligible` 打分；`touch_stable` 会是 0 笔。要评 `touch_stable`，用部署之后的导出，或者用盘口 CSV 走 `--data`（`maker_replay.EXPORT_SQL` 那四张表，外加可选的 `klines_<ASSET>USDT.csv.gz`）。

```sql
\copy (
  SELECT json_build_object(
           'observed_at', observed_at, 'market_id', market_id, 'event', event, 'detail', detail)::text
  FROM qd_polymarket_maker_events
  WHERE account_id = 'global' AND strategy = 'maker_fair_value_v3' AND event = 'shadow_fills'
    AND observed_at >= TIMESTAMPTZ '2026-10-27 00:00:00+08'
  ORDER BY observed_at
) TO 'shadow_fills.jsonl'
```

```sh
cd backend_api_python
python -m app.commands.polymarket_maker_policy_lab --fills /path/to/shadow_fills.jsonl --output /tmp/pm_lab.json
# 或者，在已有盘口导出目录上重放真实 Maker（只读文件，不连库）：
python -m app.commands.polymarket_maker_policy_lab --data /path/to/csv_export --cells BTC:5m,ETH:5m,BTC:15m,ETH:15m
```

`go: true` 的条件是同时满足：结算 PnL 的 LCB > 0、m60 的 LCB > 0、≥150 个市场、≥1000 股、留下的股数至少是 eligible 的 5%、优势为正且 as60/优势 ≤ 0.7、60 秒 mark 覆盖至少一半股数。缺一就 `go: false`，并在 `blocked_by` 里写原因。即使 `go: true`，也要人工再看 `keep_share` 和报价时间，然后另开一次改 `ADMISSION_SHADOW` 的 PR。这个脚本不会改环境变量。

如果 `book_stale_share` 本身就在 0.02 以上，先修录制，不要把 `MAX_BOOK_STALE_SHARE` 调大来换报价。数据质量门槛这次没有放宽。

## 9. 建议的上线顺序

1. 审这个 PR。不要为了“先有成交”把 `REENTRY_NEGATIVE_LCB` 设成 1。
2. 合并并部署后，确认真实报价仍为 0、影子仍在跑、权益不再被新的 maker 成交消耗。
3. 跑第 8 节的两条 SQL。把 `shadow_fills.jsonl` 交给 lab。
4. 只有某一变体 `go: true` 之后，再考虑 `ADMISSION_SHADOW`。在那之前，做市这一侧应保持影子记账、真实不挂。
