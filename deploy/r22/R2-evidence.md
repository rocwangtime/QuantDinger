# R2 修改前证据
h0021/ROOT_CAUSE.md §1/3：M.1.stale 在16:07:33/08:02/08:33分别增加15.1k/21.4k/11.8k，无法区分来源；事件26 gap=7259.6ms，后续双踢却报6.967ms、rolling business=0。
c6acf50 ws_redundancy.py:176–206 只有全局stale；recording.py:242–244 metadata拒绝无计数；DoubleKicks:554–594只跟第二次中断自己的publication基线。主失败status没有统一的滚动小时计数。
影响面仅观察器诊断：按物理source保留有界过期计数，role从当前连接映射；每源book ingress lag与样本年龄；双踢取第一踢到第二踢所关联主故障及本次中断最大业务发布缺口。统一final且>1500ms、pending/unmeasured不是0；同business_event_id不重复计数。不会改盘口应用、连接或请求频率。
补充审计：子→父IPC须携带source，否则纸面Recorder仍会记unknown；新增可选来源字段，不更改事件内容。主故障诊断只保留64条，不能用它直接统计完整滚动一小时；另保留有界4096条一小时事件引用，按故障时刻归窗，同一final算法计算，并显式报告容量截断。
