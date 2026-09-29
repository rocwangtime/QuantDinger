# 富途美股模拟盘：腾讯云广州轻量服务器部署准备

本方案只运行美股 `SIMULATE`，不启用实盘。广州机房可作为技术验证起点；是否能稳定连到富途服务，要以服务器上 OpenD 的实际登录和探测为准。富途官方说明 OpenD 可运行在 [Ubuntu 云服务器](https://openapi.futunn.com/futu-api-doc/en/opend/opend-intro.html)。

## 资源与访问方式

当前 2 核 / 2 GB / 40 GB 实例足以先运行轻量模拟盘 MVP；持续运行 OpenD、PostgreSQL、两个 Redis、API、交易工作进程、回测任务及前端时，应监控内存和 swap，必要时升级至至少 4 GB 内存。不要仅因一次 GHCR 下载缓慢就升级配置，先确认网络链路和镜像仓库重试。2026-09-28 检查发现本机还有 `pdf2word` 服务，占用 80/443 和本机 5000；本项目改用本机 5001、8888，不接管现有站点。

第一阶段只通过 SSH 隧道访问 Web：Compose 保持前端绑定 `127.0.0.1:8888`，本地运行 `ssh -i ~/.ssh/tengxunyunRocGZ1.pem -N -L 8888:127.0.0.1:8888 ubuntu@106.55.138.232`，浏览器打开 `http://127.0.0.1:8888`。保持现有站点的安全组规则不变，本项目无需新增公网入站端口；SSH 尽量限制为自己的 IP，不要向公网开放 11111、11112、5432、6379、5001、8888。若后续使用域名对公网提供 Web，再处理 HTTPS、访问控制和中国内地网站 [ICP备案要求](https://intl.cloud.tencent.com/zh/solutions/icp-registration-support?lang=zh)。不需要仅因富途香港账户就预先购买香港服务器。

## 用户需要在服务器上完成的私密操作

1. SSH 密钥通过 `ubuntu` 用户登录（不是 `root`）；不要发送私钥、密码或验证码。已有的 `pdf2word` 和 DERP 服务应保持不变。
2. Docker Engine 和 Compose V2 已安装；2 GB 内存可先联调，但要观察内存、swap 和磁盘占用，持续运行时可考虑升级至 4 GB。
3. 官方 OpenD 10.11 命令行版本已安装至 `/opt/futu-opend`，由无 sudo 权限的 `futuopend` 用户持有。用户在自己的 SSH 终端执行 `sudo -u futuopend -H sh -c 'cd /opt/futu-opend && ./FutuOpenD -api_ip=127.0.0.1 -api_port=11111 -lang=chs'`，只在交互提示里输入牛牛号、密码及二次验证；是否使用 OpenD 自带的“记住密码”由用户决定。登录密码及二次验证不要写入 Git、项目 `.env`、服务启动命令、聊天或日志。当前 OpenD 以交互进程运行，`deploy/systemd/quantdinger-futu-opend.service` 尚未启用；整机重启后需要用户重新确认 OpenD 登录，不能假定会自动恢复。服务只监听 `127.0.0.1:11111`。富途 [命令行 OpenD 指南](https://openapi.futunn.com/futu-api-doc/en/opend/opend-cmd.html)。
4. 首次在服务器项目目录执行 `sh scripts/bootstrap_futu_paper_env.sh`，生成仅服务器本地保存的项目根 `.env` 和 `backend_api_python/.env`（Git 已忽略，权限 600）。脚本拒绝覆盖已有配置，生成独立的 PostgreSQL、Redis、管理员、签名及凭证加密密钥；不要将这些文件同步回开发机、提交到 Git、粘贴到聊天或放入日志。管理员初始密码仅由用户通过自己的 SSH 会话在服务器查看 `backend_api_python/.env`。
5. Web 账户中心先“探测账户”，再选择返回的美股 `SIMULATE` `acc_id`。牛牛登录号不等于 OpenAPI 交易账户 ID；不要把登录密码或交易解锁密码填进 QuantDinger。

## 代码与服务启动

服务器的 `/srv/quantdinger/git` 下放置两个同级目录：`QuantDinger/`（本后端分支）和 `QuantDinger-Vue/`（带富途页面的前端分支）。GitHub Actions 已在公开 fork 上构建并测试前后端镜像；服务器拉取已构建镜像，不在 2 GB 主机上运行 Node 或 Python 镜像构建。项目根 `.env` 只在服务器保存 `FUTU_BACKEND_IMAGE_REF`、`FUTU_FRONTEND_IMAGE_REF`、`FUTU_PAPER_AUTOTRADE_ALLOWED` 等部署配置。完成镜像下载或导入、数据库备份并确认无运行策略及待处理订单后，从 `QuantDinger/` 执行：

```bash
sudo docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml -f docker-compose.futu-paper.images.yml \
  up -d --no-build migration
# 确认迁移容器退出码为 0 后，再切换应用进程。
sudo docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml -f docker-compose.futu-paper.images.yml \
  up -d --no-build backend trading-worker celery-worker celery-beat frontend
sudo docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml -f docker-compose.futu-paper.images.yml ps
```

OpenD 登录并通过 `127.0.0.1:11111` 探测后，启动 `--profile local-brokers` 的 `opend-relay`。在此之前，Web 可先检查，但账户和自动交易不能连上富途。不要把模拟策略部署到无法确认 OpenD 连接和账户 ID 的服务。

创建富途凭证时填写 `host.docker.internal:11112`、`trade_env=demo`、`trade_market=US`、探测获得的模拟 `acc_id`。Web 页面连接只是诊断会话；自动策略使用单独保存的加密交易凭证。将 [`futu_us_paper_roundtrip.py`](../trading/futu_us_paper_roundtrip.py) 粘贴进策略 IDE，先验证并回测，再部署为模拟盘。它在一个 SPY 分钟线上只尝试买卖各一次、只下明确价格的限价单，并持久化运行状态；若订单未成交、被拒或行情中断，不会无限重试，需在富途和 Web 中人工检查并停止部署。第一笔务必有人值守，核对模拟账户和数量为 1 股。

### 操作员启停门禁

`FUTU_PAPER_AUTOTRADE_ALLOWED` 是服务器本地的总开关，默认 `false`；仅在完成安全版本部署和迁移后，在服务器项目根 `.env` 中显式设为 `true`。它不包含账户号或密码。总开关为 `false` 时，即使 Web 显示 OpenD 已连接，后端的最终下单调用也会被拒绝。总开关为 `true` 仍不足以下单：操作员必须在 Web 先探测、选择并手动输入完全一致的美股 SIMULATE `acc_id`，保存加密凭证、连接诊断会话，然后在独立的“自动交易”区域再输入该 `acc_id` 并点击启用。只有当前账户、当前保存凭证、新产生的待处理策略订单同时匹配时，下单门禁才放行；浏览器连接与 OpenD 登录本身不代表已启用。

如果同一账户曾保存多条凭证，启用时固定使用最新的一条，并在 Web 显示其凭证编号；策略部署也必须选择这个编号。旧凭证不会被自动删除，切换前会先核验旧凭证关联的未完成订单是否已停止。

点击“暂停自动交易”或“断开连接”会先禁用新下单，再等待进行中的提交返回，撤销本服务队列中尚未提交的订单，并仅撤销可识别为本服务所下的富途未完成模拟订单。随后再次查询富途以确认。若 OpenD 断线、订单身份不明或撤单未获确认，Web 显示“停止未确认”，门禁仍禁用，但**不能声称富途挂单已全部撤销**；操作员需要到牛牛模拟账户检查并重试。手工下的订单不会被服务撤销。交易 worker/整机重启后自动交易默认恢复为暂停/待核验，必须重新检查账户并手动启用。不要把 Web 断开当作 OpenD 退出登录；OpenD 登录仍由独立进程管理。

订单提交前会持久记录本服务的订单标识。如果 OpenD 超时使下单结果不明确，订单进入待对账状态，不会自动重复提交；后台会按标识查询并补记券商订单号与累计成交。在结果未查明前，暂停保持“停止未确认”，要求人工核对模拟账户。

## 当前 GitHub 镜像发布与手工导入兜底

后端分支为 `rocwangtime/QuantDinger` 的 `codex/futu-paper-mvp`，前端分支为用户 fork `rocwangtime/QuantDinger-Vue` 的 `codex/futu-paper-dashboard`，上游前端仓库仍是 `OpenByteInc/QuantDinger-Vue`。前后端是两个独立 Git 仓库。服务器 `.env`、OpenD 登录文件、交易密码和验证码绝不推送到 GitHub。

两个 fork 的 GitHub Actions 工作流已启用，并发布过通过测试的前后端镜像。使用工作流生成的对应 commit 镜像；能直接拉取时优先用 registry digest 固定版本。服务器 Git 拉取后，将完整镜像引用写入**仅服务器本地**的项目根 `.env`：`FUTU_BACKEND_IMAGE_REF`、`FUTU_FRONTEND_IMAGE_REF`。叠加 [`docker-compose.futu-paper.images.yml`](../../docker-compose.futu-paper.images.yml) 后，应用服务不在服务器上构建源码。该覆盖文件的 `pull_policy: missing` 允许预先拉取或手工导入镜像后离线切换，不会在每次 `up` 时强制重拉：

```bash
sudo docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml -f docker-compose.futu-paper.images.yml \
  pull migration backend trading-worker celery-worker celery-beat frontend
sudo docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml -f docker-compose.futu-paper.images.yml \
  up -d --no-build migration
# 仅在迁移成功后切换服务。
sudo docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml -f docker-compose.futu-paper.images.yml \
  up -d --no-build backend trading-worker celery-worker celery-beat frontend
```

若 GHCR 从服务器下载缓慢，可在已拉取同一 commit 镜像的开发机导出 `linux/amd64` 文件，上传至服务器 `/tmp`，先比较两端文件 SHA-256，再用 `zstd -dc /tmp/<文件名>.tar.zst | sudo docker load` 导入。此时 `.env` 的镜像引用须使用已导入的 commit tag（不是未导入的 registry digest），并确认镜像 revision 标签匹配预期提交；不得从未知来源导入文件。首次切换应保留已有数据库卷和服务器私密配置，先在模拟盘停单窗口操作，并逐项确认镜像来源、健康状态、账户环境和回滚版本。私有 GHCR 镜像需要服务器单独获得只读拉取权限；不要将访问令牌放进仓库或聊天。[GitHub Container Registry 文档](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)。

验收以富途模拟账户订单与持仓为准：买卖闭环后比对订单 ID、累计成交数量、均价与最终持仓；重启交易工作进程、断开重连 OpenD、重放重复订单累计回报，平台成交数量不能增加。富途模拟盘不提供逐笔成交查询，所以 Web 的“推导成交”不是券商逐笔成交历史。

策略买卖均完成、富途订单已显示最终状态后，运行只读核对命令（使用已部署的新后端镜像及实际策略 ID）：

```bash
sudo docker compose -f docker-compose.yml -f docker-compose.futu-paper.yml -f docker-compose.futu-paper.images.yml \
  exec -T backend python -m app.commands.futu_paper_acceptance --strategy-id <策略ID>
```

命令从服务器本地加密凭证解析 SIMULATE 账户，不接受或输出登录密码、交易密码、令牌或账户 ID；它要求恰好一笔 SPY 买入和一笔卖出，每笔 1 股，逐笔比对富途与平台的订单 ID、累计数量、均价和平台成交记录，并要求双方最终 SPY 持仓为零。任一证据缺失时返回失败，不能把“无记录”当作通过。完成交易进程重启、OpenD 断线重连与重复回报重放后须再次运行，确认仍然通过且成交行数不增加；还要在富途牛牛模拟账户界面人工核对订单。若账户原先持有 SPY，应先停止验收并另选干净的模拟账户，不能误把外部持仓计入策略闭环。
