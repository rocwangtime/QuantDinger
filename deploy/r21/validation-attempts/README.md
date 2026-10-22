# 未计为通过的验证尝试

- full-suite/：初次错误临时端口，部分集成测试硬编码5432；进程末尾 Pandas 原生代码崩溃，无完整总结。
- full-suite-final/：相同错误端口启动的无效运行；已作废。
- status-shape-first.log：一次初始化快照没有 cadence 样本，旧测试拿 populated 状态比空状态；已给测试显式第二条消息，保留断言。
- cold-simulation-timeout.log：原有两小时模拟在共享测试进程超过默认120s；单文件无改断言复跑6项42.14s、进程CPU42.67s通过。旧慢运行没有完整CPU计量，不能事后断言具体调度根因。单文件只给时间预算600s，没有跳过用例或降低断言。
- startup-boundary-before.log：新增反例先证明中途启动会错误拿市场开始作为失效基线；已限定只在真实rollover使用，startup保留单列诊断。

最终采用 accepted-suite/ 及 suite-summary.json。所有测试进程均与正式性能回放串行。

原始 pytest 日志和 unified-diff 夹具保留原始空白字节；最终源代码/报告的 git diff --check 排除这些 .log/.patch 证据格式。完整检查命令及结果在 ../quality.json。
