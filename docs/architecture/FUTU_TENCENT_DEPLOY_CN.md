# 富途美股模拟盘：腾讯云广州轻量服务器部署准备

本方案只运行美股 `SIMULATE`，不启用实盘。广州机房可作为技术验证起点；是否能稳定连到富途服务，要以服务器上 OpenD 的实际登录和探测为准。富途官方说明 OpenD 可运行在 [Ubuntu 云服务器](https://openapi.futunn.com/futu-api-doc/en/opend/opend-intro.html)。

## 资源与访问方式

当前 2 核 / 2 GB / 40 GB 实例可用于配置和短暂联调，但同时运行 OpenD、PostgreSQL、两个 Redis、API、交易工作进程、回测任务及前端时，内存余量很小。正式进行持续模拟盘测试前建议升级至至少 4 GB 内存，并确认系统盘空间、备份及服务器到富途的网络连接。

第一阶段只通过 SSH 隧道访问 Web：Compose 保持前端绑定 `127.0.0.1:8888`，本地运行 `ssh -L 8888:127.0.0.1:8888 <服务器 SSH 别名>`，浏览器打开 `http://127.0.0.1:8888`。安全组仅开放 SSH 给自己的 IP；不要向公网开放 11111、11112、5432、6379、5000、8888。若后续使用域名对公网提供 Web，再处理 HTTPS、访问控制和中国内地网站 [ICP备案要求](https://intl.cloud.tencent.com/zh/solutions/icp-registration-support?lang=zh)。不需要仅因富途香港账户就预先购买香港服务器。

## 用户需要在服务器上完成的私密操作

1. 配置 SSH 公钥登录，并告知开发者 SSH 别名和用户名；不要发送私钥、密码或验证码。确认服务器上是否已有其他服务。
2. 安装 Docker Engine 与 Compose；如允许本项目占用整机，建议先将实例升级至 4 GB。
3. 按富途 [命令行 OpenD 指南](https://openapi.futunn.com/futu-api-doc/en/opend/opend-cmd.html)下载 Ubuntu 版，在服务器本地交互登录。登录密码及二次验证只在 OpenD 交互终端输入；不要写入 Git、项目 `.env`、服务启动命令、聊天或日志。OpenD 的 `ip` 配为 `127.0.0.1`，`api_port` 为 `11111`。确认登录后能够在重启时安全地恢复会话。
4. 在服务器本地创建项目根 `.env` 和 `backend_api_python/.env`（Git 已忽略）。更改 PostgreSQL、Redis、管理员密码，生成独立随机 `SECRET_KEY`，核对 `CREDENTIAL_ENCRYPTION_KEY` 的持久性和文件权限。不要使用仓库示例中的默认管理员或数据库密码。
5. Web 账户中心先“探测账户”，再选择返回的美股 `SIMULATE` `acc_id`。牛牛登录号不等于 OpenAPI 交易账户 ID；不要把登录密码或交易解锁密码填进 QuantDinger。

## 代码与服务启动

服务器应放置两个同级目录：`QuantDinger/`（本后端分支）和 `QuantDinger-Vue/`（带富途页面的前端分支）。在两者均已传到服务器、私密配置已本地保存、OpenD 已登录后，从 `QuantDinger/` 执行：

```bash
docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml --profile local-brokers \
  up -d --build postgres redis redis-jobs migration backend trading-worker celery-worker frontend opend-relay
docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml --profile local-brokers ps
```

创建富途凭证时填写 `host.docker.internal:11112`、`trade_env=demo`、`trade_market=US`、探测获得的模拟 `acc_id`。Web 页面连接只是诊断会话；自动策略使用单独保存的加密交易凭证。将 [`futu_us_paper_roundtrip.py`](../trading/futu_us_paper_roundtrip.py) 粘贴进策略 IDE，先验证并回测，再部署为模拟盘。它在一个 SPY 分钟线上只尝试买卖各一次、只下明确价格的限价单，并持久化运行状态；若订单未成交、被拒或行情中断，不会无限重试，需在富途和 Web 中人工检查并停止部署。第一笔务必有人值守，核对模拟账户和数量为 1 股。

验收以富途模拟账户订单与持仓为准：买卖闭环后比对订单 ID、累计成交数量、均价与最终持仓；重启交易工作进程、断开重连 OpenD、重放重复订单累计回报，平台成交数量不能增加。富途模拟盘不提供逐笔成交查询，所以 Web 的“推导成交”不是券商逐笔成交历史。
