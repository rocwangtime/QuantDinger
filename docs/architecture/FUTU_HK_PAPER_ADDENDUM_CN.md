# 富途港股股票 SIMULATE 增量验收

本增量只开放港股**股票**模拟账户（`TrdMarket.HK`、`trd_env=SIMULATE`、`sim_acc_type=STOCK`），不开放港股期权/期货，也不开放 REAL。美股账户及已有策略不自动切换市场。

## Web 连接与策略账户

账户中心的富途表单选择“港股”后，先“探测账户”，只展示返回的港股股票 SIMULATE 账户。富途[交易账户列表文档](https://openapi.futunn.com/futu-api-doc/trade/get-acc-list.html)说明港股股票与期权模拟账户应按 `sim_acc_type` 区分。选择账户，手动输入完全一致的交易账户 ID，再点“连接”。连接成功时后台会复用已有的同一账户配置，或创建一条加密保存的配置供策略使用；无需另点“保存为策略账户”。此动作**不会启用自动交易**。重复连接不会再增加同一账户配置。旧版界面曾生成的重复配置不会自动删除，已有策略绑定也不会被悄悄改写。

美股或港股若有另一账户处于已启用、停止中或停止未确认状态，先核对并暂停原账户，才能连接新账户。服务器硬开关、账户手动启用、策略绑定的保存配置和最终下单门禁仍须全部匹配。进程重启后门禁默认暂停。

## 港股测试边界

测试标的暂用 `HKStock:00700`（腾讯控股），验收策略源见 [futu_hk_paper_roundtrip.py](../trading/futu_hk_paper_roundtrip.py)。2026-09-30 OpenD 只读快照显示其每手 100 股、价位约 HKD 427、价差档位 HKD 0.2；这些数字**不是永久参数**，部署前必须再次核对富途快照的每手数量、最小价位、模拟账户可买数量、既有持仓与挂单。策略仅尝试买一手、观察持仓后卖一手，不重试下单；已有同标的持仓或挂单时不要启动验收。限价单只能在当前报价上下 2% 内提交，新鲜行情、市场状态或每手数量不确定就拒绝下单。

港股正常连续交易时段为香港时间 09:30–12:00、13:00–16:00；节假日以交易日历为准（见富途[市场状态说明](https://openapi.futunn.com/futu-api-doc/qa/quote.html)）。系统同时检查港交所交易日历和 OpenD 返回的 `MORNING`/`AFTERNOON` 状态，午休、竞价时段及休市不下单。富途模拟账户的通用 `power` 字段可能为 0，不能据此推断没有港币现金；买入前使用富途按标的/价格返回的 `max_cash_buy` 校验，而不是把 `cash` 当成购买力。富途[最大可买可卖接口文档](https://openapi.futunn.com/futu-api-doc/trade/get-max-trd-qtys.html)也建议按具体标的确认可买数量。富途对该查询有限频要求，不要高频手动探测。

先在 Web 验证港股回测与策略编译，再绑定连接时复用/创建的港股配置；确认账号、标的、整手数量与策略状态后，由操作员单独启用自动交易。首次订单应有人值守，并在富途牛牛港股**模拟**账户里核对。任何订单状态不明、平台/富途数量不一致或 OpenD 断线，应立即暂停并人工核查，不进行第二次尝试。

买卖均最终成交且账户恢复空仓后，运行只读验收命令：

```bash
sudo docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml -f docker-compose.futu-paper.images.yml \
  exec -T backend python -m app.commands.futu_paper_acceptance --strategy-id <港股策略ID>
```

命令依策略市场限定为 `HKStock:00700` 一手 100 股，或原美股 `USStock:SPY` 一股；核对双方订单 ID、方向、累计成交、均价、平台成交行以及最终空仓。重启、断线重连和重复成交回报验证后再次执行；未通过不能宣称港股闭环已完成。
