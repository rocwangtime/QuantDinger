# QuantDinger 研发进展与云端 CLI 交接

交接日期：2026 年 10 月 10 日，Asia/Singapore。

本项目用于个人 AI 投研和自动化交易研究。当前已具备策略生成与回测、受控 Agent、富途美股和港股模拟执行、持续组合任务、可复现研究以及 Polymarket 纸面实验。下一阶段重点是把这些能力在同一部署版本中跑通，完成多个交易日的模拟观测和快速决策性能验收。

此前散落在开发分支和本地工作树中的相关功能，已在本次交接前提交、推送并合并到两个个人 fork 的 `main`。云端应从这两个仓库接手。代码合并完成与服务器部署完成是两种状态；本次合并没有更新服务器镜像或启用自动交易。

## 1 接手仓库与代码基线

| 项目 | 仓库 | 接手分支 | 功能合并基线 |
| --- | --- | --- | --- |
| 后端与部署 | `https://github.com/rocwangtime/QuantDinger.git` | `main` | `d0562d84912182817329ce69a55952bbaf327dbe` |
| Vue 前端 | `https://github.com/rocwangtime/QuantDinger-Vue.git` | `main` | `37343fa2e01f8e7c6e9511ba413dd3a39cf1502b` |

以上是功能合并的提交，交接文档的后续提交会继续推进后端 `main`。克隆时取最新 `main`，并确认上述提交为当前 HEAD 的祖先。

本次合并记录：

- [后端 PR 28](https://github.com/rocwangtime/QuantDinger/pull/28)：可复现研究、AI 评估、组合风险、虚拟多腿、Polymarket 纸面实验及富途港股订单历史修复。
- [后端 PR 29](https://github.com/rocwangtime/QuantDinger/pull/29)：持续 Agent 模拟组合、保护、研究循环、就绪检查和运行报告。
- [前端 PR 31](https://github.com/rocwangtime/QuantDinger-Vue/pull/31)：研究控件、Polymarket 工作区和个人侧栏。
- [前端 PR 32](https://github.com/rocwangtime/QuantDinger-Vue/pull/32)：Agent 模拟组合页面、研究预算、运行检查和报告导出。

前端原来位于另一份上游 checkout；此次仅将三笔 Agent 功能提交接到个人 fork 的最新界面，保留 Polymarket 入口、个人模式和交易意图翻译。后端 OpenAPI 已按合并后的代码重新生成。

本地 `backend_api_python/uv.lock` 仍是未跟踪文件，声明 Python >=3.13，未纳入本次交付。正式安装继续使用已跟踪的 `requirements.lock`，CI 基准为 Python 3.12；不要将该本地文件当作新的部署依赖契约。

## 2 产品方向与用户目标

用户明确提出的两个核心场景是：

1. **美股每日组合计划**：在指定股票池内，盘前读取已有持仓、购买力、行情及事件，先检查退出，再寻找买入机会；开盘后复核价格与资金，持续管理任务所属持仓。
2. **小米港股快速触发**：监听 `01810` 的实时行情，价格条件触发后调用一次短 AI 判断，在十几秒内给出有效决策或明确跳过；下单前再次核对行情、整手、持仓、资金与授权。

界面按个人工作台维护，已隐藏充值、积分、VIP 和邀请入口，主导航改为侧栏，移动端使用抽屉。后续新增功能应融入现有工作区。

系统保留两类 Agent 能力：内置模型的研究与工具流程，以及外部 Agent Gateway/MCP 的受控接口。持续交易任务在二者已有基础上增加持久状态、调度、组合、执行及复盘，不应另起一套券商或订单引擎。

## 3 当前已完成的研发

### 投研与策略开发

- Strategy API V2 的策略生成、保存、校验、回测和部署流程已具备；研究结果可衔接策略草稿及回测任务。
- AI 投研支持流式输出、工具进度、取消、标的上下文、对话历史恢复和新对话选择。
- 明确的单标的只读研究使用快速路由；策略创建、交易和持续任务仍执行完整结构化校验。
- 支持按请求或任务保存服务商、模型和思考档位。显式模型选择不会静默跨服务商降级。

### 富途美股与港股模拟执行

- 已接入 US/HK 股票 `SIMULATE` 的账户探测、加密凭证、报价与历史 K 线、限价订单、撤单、累计成交对账和重连。
- 账户连接、操作员启用、服务器总开关和 Agent `PAPER_AUTO` 策略是不同门禁；连接成功不会自动授予下单权限。
- 支持下单前持久绑定凭证与订单标识、不明确提交结果的恢复、重复累计成交去重，以及按本次策略运行验收。
- 原先订单历史硬编码 `US.` 导致港股记录不可见；现已按配置市场过滤，US/HK 回归覆盖已提交到主分支。

历史真实模拟环境验收：

| 日期 | 样例 | 结果与边界 |
| --- | --- | --- |
| 2026 年 10 月 5 日 | SPY 每边 1 股 | 买入 773.01 美元、卖出 773.08 美元，券商与平台对账通过，最终空仓；重启和重连通过。 |
| 2026 年 10 月 6 日 | 腾讯 `00700` 每边 100 股 | 买入 HKD 428.4、卖出 HKD 428.0，对账通过，最终空仓；两轮重复回报未增加成交。 |

这些记录证明对应版本的一买一卖执行链路。它们不等于每日 Agent 组合或小米快速触发已完成自动交易验收，也不构成持续策略收益证据。

### 持续 Agent 模拟组合

已合并的模板包括每日组合、单股价格触发，以及盘中组合复核。新工作区为 `/#/agent-tasks`；原来的账户任务入口位于 `/#/ai-monitor`。

- 持久任务、运行记录、预览、计划执行、暂停、取消、触发去重、冷却和决策过期拦截。
- 读取实际模拟账户状态，按预算、现金储备、集中度、未成交订单和港股整手计算可执行数量；卖出限制为任务有权管理的数量。
- 已被券商接受但尚未反映在现金或持仓快照中的订单会预占预算；不提前使用预计卖出回款。
- 任务持仓及基于实际累计成交的前瞻表现、等权持有基准、模型费用估算和报告导出。
- 独立程序保护处理持仓止损、当日亏损和回撤，不等待新 AI 判断；暂停保留已有持仓，撤单未确认时继续对账。
- 受限研究工具循环查询账户、行情、已完成日线快照和新闻，限制调用、工具请求、输出 token 和每日决策次数。
- 运行就绪检查、显式只读富途探测、任务版本与探测时效校验、Agent 组件心跳和重启代次拦截。

当前边界：

- 快速价格任务维持一次快照判断，不能用多轮工具研究替代。预览可使用较长截止时间，预览延迟不能直接证明实时任务达标。
- `AgentDecision` 仍只有两个共享执行槽位；普通与快速任务尚未隔离。保护使用独立线程池，但这没有解决快速决策排队问题。
- 盘中组合复核按约 30 秒观测，可能遗漏两次观测之间的波动。
- 绩效尚未完整计入券商费用、税费、分红和利息；模型费用也保留覆盖缺口。页面没有将这些数据宣称为完整净收益或胜率。
- 模型工具循环使用应用层 JSON 协议，并非服务商原生 function calling。
- 新增完整工作区仍需在目标服务器联调模型、OpenD 和多个交易日；此前本地合成数据验收不能替代运行验收。

### 可复现研究与 AI 评估

- 策略进化提交时冻结授权源码与参数定义；工作进程冻结行情、基本面、历史 universe 和交易规则，保存内容哈希数据包。
- 回放复用原请求与冻结数据，校验所有者、哈希、源码和执行库指纹，不重新读取替代行情。
- 累计研究试验与 holdout 使用记录，DSR、按日收益对齐的 CSCV PBO、成本压力和明确的 `insufficient_evidence` 状态。
- 基本面修订归档，区分系统获知时间与公开发布时间。迁移前的历史修订不能凭空重建。
- AI 入场模式为 `shadow`、`advisory`、`required`；退出不依赖 AI。required 无有效判断或审计写入失败时阻止新入场。
- shadow 报告按固定假设退出期限观察机会收益、拦截损失、漏掉盈利、延迟和模型用量；它不是实际执行净值。
- 部署可绑定匹配的研究证据；`REQUIRE_RESEARCH_EVIDENCE_FOR_LIVE` 默认仍为 false，需要明确配置才成为强制门禁。

固定参数窗口验证已经实现；逐窗口重新拟合的 walk-forward 仍属于未来扩展，不能把现有结果解释成滚动训练。

### 组合风险与虚拟多腿

- 对齐日收益、收缩协方差、波动贡献、经验 VaR/尾部损失、压力损失和 gross/net exposure。
- 可选账户入场门禁串行检查预计组合风险，并预占排队风险；模型过期、资产缺失、报价不足或估值币种不一致时拒绝。
- 风险分母是分配的策略本金，尚不是独立核验的账户 NAV；没有自动 FX 换算。
- 虚拟订单组支持逐腿持久状态、幂等、取消、超时、残余敞口、补偿退出重试和人工结案。
- 订单组限定在隔离的 signal-mode 虚拟账户；没有扩展为富途实盘、跨交易所实盘或原子多腿成交。

### Polymarket 实验室

- `/#/polymarket` 使用官方 Gamma/CLOB 公开数据扫描标准 YES/NO 市场与实际盘口深度。
- 纸面实验计算等量两腿的购买成本、费用、合并假设和净边际，并模拟延迟、部分成交、补偿及残余敞口。
- 持久任务、幂等、取消、所有者隔离、冻结证据下载和不联网回放。
- 2026 年 10 月 7 日新加坡服务器观察到 3 个市场、9 份样本，证据与回放核验通过；当时没有达标机会，正确拒绝入场。
- 目前没有钱包绑定、签名、真实余额、真实下单或链上合并；页面资金是每次实验独立重置的虚拟资金。

## 4 未来工作与验收顺序

以下是云端接手后的建议顺序，其中前四项直接服务于已提出的用户场景。

| 优先级 | 工作 | 可交付结果与验收标准 |
| --- | --- | --- |
| P0 | 发布当前合并版本 | 匹配前后端镜像，备份与迁移成功，所有应用进程使用同一后端版本，共享数据卷，健康与心跳正常；已有策略和凭证保留。 |
| P0 | 美股每日组合端到端 | 指定股票池盘前准备与决策，开盘复核，任务所属买卖及退出，次日持仓延续，撤单和对账；休市、夏令时、资金延迟更新及过期授权均有明确状态。 |
| P0 | 小米快速任务隔离与测速 | 独立快速容量或调度优先级；统计行情触发到有效决策/跳过的 P50、P95、超时率，覆盖长研究任务占满槽位；超时与旧运行回调不能下单。 |
| P0 | 多交易日模拟观测 | 每日检查授权与连接，积累至少覆盖多个完整交易日的计划、成交、估值、保护和模型成本，导出可核对报告；缺失数据明确保留为空。 |
| P1 | 新闻与事件数据质量 | 标注发布时间、系统获知时间、来源与新鲜度，区分缺失和无事件；对财报、重大新闻、供数失败和重复事件进行回放验证。 |
| P1 | AI 相对基线效果 | 固定股票池、成本与预算，比较不使用 AI、单次快照和受限研究循环；报告净成本、回撤、换手、漏掉机会、调用成本与延迟。 |
| P1 | 修复既有全量测试问题 | 复核 `test_spot_close_balance_guard.py` 的六个 worker 测试，更新已失效的 mock 接入点，保持现货卖出余额保护断言；完整测试恢复绿色。 |
| P1 | 风险模型维护与完整成本 | 显式更新协方差模型、覆盖相关仓位和估值币种；补齐券商费用与账单，保留成本覆盖状态，避免毛收益被展示为净收益。 |
| P2 | 更严格研究方法 | 逐窗重新拟合、跨复制源码的研究族管理、更完整 point-in-time 修订历史及更长期未见样本。 |
| P2 | Polymarket 真实执行 | 先确认账户与执行环境资格，再独立设计钱包、签名、资金预占、订单/链上对账、失败补偿和操作限额；公开行情可读不代表实盘已具备。 |
| P2 | 更复杂多腿与研究 Agent | 先验证现有虚拟组，再考虑真实券商能力与敞口恢复；TradingAgents 的反证研究或 MiroFish 情景实验应有固定预算和独立增益评估。 |

15 秒是快速任务的决策有效期与工程目标。此前小米预览约 4.6 秒、美股 AAPL/TSLA 组合预览约 38.7 秒，都是单次历史观测，不能当作 P95 保证。

## 5 部署与运行状态

目标是用户的**新加坡服务器**，不是旧广州实例。历史运维入口为 `ubuntu@43.159.52.170`，服务器目录为 `/srv/quantdinger/git/QuantDinger`，前端在同级 `QuantDinger-Vue`。云端 CLI 需要自己的授权访问方式；本地 Mac 的 SSH 私钥路径不能直接用于云端。

2026 年 10 月 7 日的记录表明研究和 Polymarket 版本以及个人界面已部署到新加坡。2026 年 10 月 10 日新增合并的完整 Agent 工作区，仍需重新构建、迁移与部署。本次没有在线读取服务器最新镜像、策略或门禁，因此运行状态以接手时检查为准。

历史美股验收结束时总开关关闭；随后港股验收结束时总开关保留开启、策略停止、账户自动交易暂停。不要将这些不同日期的记录混成当前状态。重启会使操作员启用失效，Agent PAPER_AUTO 授权最多 24 小时，不会自动续期。

旧部署文档 [FUTU_TENCENT_DEPLOY_CN.md](FUTU_TENCENT_DEPLOY_CN.md) 仍含广州地址及早期分支，作为架构和操作说明使用，执行时应采用本节目标与最新 `main`。

进程职责必须保持：API 接收命令；trading-worker 管理策略和订单；scheduler-worker 管理领域调度与 Agent 组件；Celery 执行有限后台任务。缓存 `redis` 与持久队列 `redis-jobs` 独立。

发布顺序：

1. 核对当前前后端提交、镜像引用、策略状态、未完成订单和所有启用门禁，保存回滚镜像与数据库备份。
2. 构建并固定两个 fork 的匹配镜像，保留原 `.env`、加密密钥、数据库卷、OpenD 登录状态和服务端私密文件。
3. 先运行 `python -m app.commands.migrate`，确认迁移成功，再更新 API、trading、scheduler、Celery worker/beat 及必要的 relay。
4. API 与 Celery 共用 `EVOLUTION_BUNDLE_DIR` 所在的数据卷。更新后检查 `/api/health`、`/api/health/ready`、`/api/health/workers`。
5. 验证页面预览、就绪检查、只读 SIMULATE 探测、模型选择和报告下载，再安排交易时段模拟执行。

富途部署使用 `docker-compose.yml`、`docker-compose.futu-paper.yml` 和 `docker-compose.futu-paper.images.yml`；由服务器本地 `FUTU_BACKEND_IMAGE_REF`、`FUTU_FRONTEND_IMAGE_REF` 指定镜像，实际操作还须保留原 profile 和端口覆盖。服务器的 PostgreSQL、Redis、OpenD 及 Web 端口配置以现有部署为准。

## 6 测试证据与复验方式

本次集成实际完成的验证：

- 研究分支与富途修复：173 项相关后端回归通过。
- 完整 Agent 后端集成：189 项单元/接口回归通过，另有 87 项独立 PostgreSQL 集成与发布门禁通过。
- 前端集成：413 项既有单元测试通过，改动文件 ESLint 和生产构建通过；构建保留已有大 chunk 提示。
- Ruff、后端结构及正式依赖锁检查通过；数据库迁移在独立 PostgreSQL 16 容器成功。CI 定义使用 PostgreSQL 18，云端复验应覆盖此版本。

这些数量对应不同范围，部分重叠，不能相加成全量测试数。GitHub 合并确认不等于 CI 全量通过。本次没有重新完成全量后端与线上券商验收。

历史 2026 年 10 月 7 日全量记录为 3640 passed、31 skipped、6 failed；六项失败在变更前版本也复现，原因是测试 patch 已移除的 `build_live_order_context`。本次保留这个已知问题为明确待办。

云端安装与检查示例：

```bash
git clone --branch main https://github.com/rocwangtime/QuantDinger.git
git clone --branch main https://github.com/rocwangtime/QuantDinger-Vue.git
cd QuantDinger/backend_api_python
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.lock -r requirements-dev.txt
python scripts/check_requirements_lock.py
ruff check app scripts tests
python scripts/backend_quality_check.py
python ../scripts/check_docs.py
python -m pytest tests/test_agent_automations.py tests/test_agent_performance.py \
  tests/test_agent_research.py tests/test_agent_run_acceptance.py \
  tests/test_futu_client_contract.py tests/test_research_execution_policies.py \
  tests/test_research_execution_api.py tests/test_polymarket.py -q
```

数据库测试仅使用新建的本地 disposable 数据库：`AUTOMATION_TEST_DATABASE_URL` 要求主机为 localhost/127.0.0.1、库名为 `qd_automation_test`。`QD_TEST_POSTGRES_DSN` 指向同一已迁移测试库。不要从运行部署的 `.env` 取得测试 DSN。

```bash
python -m pytest tests/test_agent_automations_db.py \
  tests/integration/test_research_order_groups.py \
  tests/integration/test_polymarket_jobs.py \
  tests/integration/test_execution_projection_atomic.py \
  tests/integration/test_grid_fill_accounting.py tests/release_gate -q
```

前端使用 Node 22 和仓库锁文件执行 `npm ci`、`npm run test:unit`、相关文件 ESLint 及 `npm run build`。HTTP 契约变更后执行 `python scripts/export_openapi.py` 并提交 `docs/api/openapi.yaml`。

## 7 代码入口与阅读顺序

后端路径均相对于 `backend_api_python`，前端路径相对于独立 Vue 仓库。

| 工作 | 主要入口 |
| --- | --- |
| 持续任务与运行 | `app/routes/agent_automations.py`、`app/services/automation/{domain,store,worker,runner}.py` |
| 保护和绩效 | `app/services/automation/{monitor,performance,evaluation}.py` |
| 研究与盘中事件 | `app/services/automation/{research,events}.py` |
| 就绪与报告 | `app/services/automation/{readiness,health,review}.py` |
| 交易授权 | `app/services/agent_trade_intents.py`、`app/services/futu_agent_execution.py` |
| 富途与对账 | `app/services/futu_trading/`、`app/services/pending_order_worker.py`、`app/services/execution_streams/` |
| 研究冻结与统计 | `app/services/strategy_evolution/{service,bundles,history,evaluator,statistics}.py` |
| AI 门禁与评估 | `app/services/ai_entry_policy.py`、`ai_decision_filter.py`、`ai_evaluation.py` |
| 风险与订单组 | `app/services/portfolio/`、`app/services/order_groups.py` |
| Polymarket | `app/routes/polymarket.py`、`app/services/polymarket/{client,engine,jobs}.py` |
| 前端 | `src/views/agent-tasks/index.vue`、`src/views/polymarket/`、`src/views/agent-task-center/`、`src/views/ai-analysis/components/CopilotWorkbench.vue` |

优先阅读：

1. [进程职责](PROCESS_ROLES_AND_TASKS.md) 与 [模块边界](MODULE_BOUNDARIES.md)。
2. [Agent 模拟组合使用说明](../agent/AGENT_PAPER_TASKS_CN.md)、[设计](AGENT_PAPER_DASHBOARD.md)、[运行验收](AGENT_RUN_ACCEPTANCE.md)。
3. [研究与执行使用指南](../trading/RESEARCH_EXECUTION_P0_P1.md)。
4. [港股模拟盘补充说明](FUTU_HK_PAPER_ADDENDUM_CN.md)。
5. [Polymarket 实验边界](../trading/POLYMARKET_PAPER_LAB.md)。

上游 `ROADMAP.md` 是通用开源规划，不代表本个人 fork 所有功能的实际交付状态；本交接的主分支与场景验收为接手依据。

## 8 可直接交给云端 CLI 的任务说明

```text
请接手 rocwangtime/QuantDinger 与 rocwangtime/QuantDinger-Vue 的最新 main，先阅读
后端 docs/architecture/DEVELOPMENT_HANDOFF_CN.md 和它引用的架构、Agent 及验收文档。

本项目是个人工作台，目标是美股自选池每日组合管理和港股小米实时触发决策。
研究冻结、AI 评估、风险与虚拟订单组、Polymarket 纸面实验以及完整 Agent 模拟组合
工作区都已合并；不要重复建设已经具备的模块。

先核验两个 main 的代码与测试，处理已知六项现货余额保护测试的失效 mock；
再核对新加坡服务器当前部署状态，准备匹配镜像、备份、迁移和更新。
接着完成美股每日组合端到端模拟验收，并隔离小米快速决策容量，测量真实 P50/P95
与超时率，覆盖长任务、旧回调、撤单、断线和重复成交。最后积累多交易日运行报告。

沿用现有进程、授权与订单引擎；富途范围保持 US/HK SIMULATE，Polymarket 当前
保持公开数据与纸面实验。操作员授权过期或暂停时不得自动续期或绕过门禁。
服务器密钥、钱包密钥和 .env 留在受控环境，不写进 Git 或日志。

每个工作包给出具体改动、对应测试、实际部署版本、验收证据与剩余边界。
把持续收益证据和流程正确性分别报告，缺失证据不能补造。
```
