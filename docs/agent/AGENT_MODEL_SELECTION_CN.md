# Agent 模型与思考深度

AI 投研输入框上方可选择服务商、模型和思考深度。配置随本次请求传递，不改系统默认模型；浏览器记住最近选择。持续研究任务另行保存自己的 `config.llm_selection`，后续调度不受聊天界面切换影响。

覆盖问答（JSON/SSE）、意图识别、专业报告（同步/异步）、策略/指标生成，以及持续研究任务。显式选择时关闭模型与跨服务商自动降级；配置删除或深度不支持时，在计费前返回 400。后台任务运行前重新校验已保存的选择。未传选择的旧客户端/旧任务保持系统默认行为。

## 配置

系统设置 → LLM 中配置服务商 Key、默认模型。`OPENAI_MODELS`、`DEEPSEEK_MODELS`、`VOLCENGINE_MODELS` 为逗号分隔的额外可选模型 ID；默认模型自动加入。列表代表管理员已配置，不代表已实时验证厂商额度/模型访问权限。无 Key 的服务商不会出现在选择器中；自定义本地兼容接口允许无 Key。仅有一个已配置模型时，列表只有一项。

`GET /api/ai/agent/models` 需要登录，只返回模型 ID、服务商及参数能力，不返回凭据/URL。调用接口传：

```json
{"llm_selection":{"provider":"volcengine","model":"deepseek-v4-1-flash-260910","reasoning_effort":"low"}}
```

## 参数依据（2026-10-03）

- [火山方舟思考参数](https://docs.volcengine.com/docs/ark/deep-thinking?lang=zh)：当前 DeepSeek V4.1 Flash 提供默认、关闭、low、high、max。关闭使用 `thinking.type=disabled`；其他显式档位启用 thinking 并传 `reasoning_effort`。不展示会映射到其他档位的 medium/xhigh。
- [OpenAI GPT-5.4](https://developers.openai.com/api/docs/models/gpt-5.4)、[参数兼容性](https://developers.openai.com/api/docs/guides/latest-model?model=gpt-5.4)：默认、none、low、medium、high、xhigh。Chat Completions 使用 `reasoning_effort` 与 `max_completion_tokens`，移除不兼容的 temperature。
- 未验证的模型/自定义推理接入点只提供厂商默认，不猜测参数。DeepSeek 官方与火山方舟是独立的凭据、计费和能力配置。

回答用量脚注保存实际服务商返回的模型、请求的思考档位及用量/估算价格。不展示或保存模型内部思考内容。

## 验证

`tests/test_agent_model_selection.py` 覆盖目录脱敏、无效参数提前拒绝、请求/线程隔离、SSE 与同步参数一致、无静默降级、用量记录及任务配置。前端测试验证每条 Copilot 调用链传递选择、任务往返保存和深度降级。此功能不改变交易权限或自动交易开关。
