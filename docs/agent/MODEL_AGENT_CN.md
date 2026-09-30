# 内置模型 Agent（DeepSeek / OpenAI）

内置模型 Agent 是 Agent Gateway 的受控运行时；它不是新的券商连接，也不能更改富途登录、账户、策略授权或实盘开关。已有的 MCP/HTTP Agent 接口仍可供外部 Agent 使用。

## 架构

`DeepSeek/OpenAI → 固定工具白名单 → Agent Token 权限和标的白名单 → TradeIntent → 人工交易策略 → 服务器开关和操作员授权 → 富途 SIMULATE 执行网关 → 审计/订单回报`

模型仅生成工具调用参数，不持有 OpenD 凭据，也不直接调用富途 SDK。后端验证每个参数，限制一次运行最多 4 个模型步骤、至多 1 个改变交易状态的工具调用；一个模型响应包含多个工具调用时拒绝执行。执行结果直接以服务端收据返回，不让后续模型文本掩盖券商结果。交易意图 ID 和幂等键用于防重；不确定结果只能核对，不能自动重报。

## 配置与启用

1. 在 Web「系统设置 → LLM」选择 `DeepSeek` 或 `OpenAI`，保存该服务商的 API Key 和模型。也可以在服务端设置 `DEEPSEEK_API_KEY` / `DEEPSEEK_MODEL` 或 `OPENAI_API_KEY` / `OPENAI_MODEL`；不要将密钥提交到 Git。
2. 在后端环境设置 `AGENT_MODEL_ENABLED=true`。可选 `AGENT_MODEL_PROVIDER=deepseek` 或 `openai`；为空时沿用 `LLM_PROVIDER`。重启后端。服务端默认 **关闭** 模型 Agent。
3. 创建受限的 Agent Token：观察层只需 `R`，计划/模拟盘执行层需要 `T`，交易令牌必须是 `paper_only`，并限定市场、标的、金额和调用速率。不要在浏览器地址栏或聊天里传令牌。
4. 模拟盘执行还需要人类管理员为**对应富途账户**短时启用 `PAPER_AUTO`，并完成服务器 `FUTU_PAPER_AUTOTRADE_ALLOWED` 与操作员授权。任一条件失效即不能下单。`REAL` 不可用。

`GET /api/agent/v1/model-agent/providers` 只返回已配置状态和模型名，不返回密钥。三个运行入口分别是：

| 入口 | Agent Token Scope | 可用工具 |
|---|---|---|
| `POST /model-agent/observe` | `R` | 查看交易策略授权、交易意图和带来源标记的富途港/美股行情 |
| `POST /model-agent/plan` | `T` | 观察 + 创建一条富途模拟交易意图，不下单 |
| `POST /model-agent/paper` | `T` | 计划 + 在所有人工/服务器/风控门禁通过后直接提交一笔富途模拟限价单 |

以上路径均以 `/api/agent/v1` 为前缀。请求 JSON 为 `{"goal":"...","provider":"deepseek"}`；`provider` 可省略，只允许已配置的 DeepSeek 或 OpenAI。模型名称只能由服务端配置，不能由调用者任意指定。**所有模型运行**均须提供唯一 `Idempotency-Key` 请求头，避免网络重试重复消耗模型额度或下单；同一个 key 不可配不同请求。

运行响应包含 `run_id`、服务商/模型、层级、文本答案和实际工具收据。`run_id` 是响应相关 ID，不是长期任务 ID。Agent 审计仅存目标文本长度与哈希、模型/工具元数据，不存原始目标、模型回答或密钥。若模拟单已经生成交易意图但券商执行失败，错误响应会带 `intent_id`；先查看/核对该意图，**不要换 key 重下单**。

## 当前边界

这是按请求触发的同步运行时，不是长期自主定时进程；策略调度继续由现有策略引擎负责。它不创建新策略源码、不更改风险限额、不登录/登出 OpenD、不操作实盘。暂不在 Web 页面直接运行：浏览器不保存 Agent Token；可使用受控 MCP/HTTP 客户端。生产启用前应在模拟盘验证服务商 API 可用性、账本一致、重启/断线/重复回报和人工暂停，并对服务商请求成本设置额度。
