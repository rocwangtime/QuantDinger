# R4 修改前证据
/workspace/quantdinger/deploy/p35/clob_valueerror_20261022.md：108次HTTP200/ValueError，每300s一次。client.py:45–47的JSON-only类型检查拒绝/time的text/plain十位秒值；paper_account.py:70调用_json，latency.samples=0/fallback=true/one_way=150ms。是延迟探针解析回归，不是上游故障。
影响面：新增受限/time解析入口，纸面Latency.measure唯一调用点改用它；共享超时、禁止重定向、错误诊断与session恢复；其他端点继续仅JSON，/time上限64字节且值严格ASCII 9–13位数字。无新请求频率、无真实交易路径改动。
