"""Append final frozen-tree evidence; never turn failed replay evidence into PASS."""
from pathlib import Path
import json,statistics,subprocess
root=Path('/workspace/quantdinger/wt_r21');out=root/'deploy/r21'
read=lambda name:json.loads((out/name).read_text())
suite=read('suite-summary.json');matrix=read('comparison/results.json');capacity=read('capacity.json')['result']
silence=read('silence-comparison.json');memory=read('memory-check.json');handoff={phase:read(f'handoff-{phase}.json') for phase in ('before','after')}
assert len(matrix)==8 and all(x['exit']==0 and x['result']['valid'] for x in matrix)
assert capacity['valid'] and memory['valid'] and all(x['exit']==0 and x['result']['valid'] for x in silence)
assert all(row['valid'] for row in handoff.values())
assert suite['failed']==suite['errors']==0
assert all(x['exit']==0 for x in read('quality.json'))
lines=['\n## 最终冻结版本验证（本节为最终验收数据）\n',
 f"PG18.6 / Python3.12.15，全量{suite['test_files']}个文件：**{suite['passed']} passed / {suite['skipped']} skipped / 0 failed / 0 errors**。六组串行独立pytest进程，换场模块另进程，冷门长模拟另进程；r21/accepted-suite含完整文件清单、日志、结果，suite-summary含生产源SHA256。未改业务断言放宽验收；旧测试夹具补齐epoch/接收PONG契约，相关初次失败保留validation-attempts。生产代码冻结于3657dc1后再跑本节全部数据，性能运行与测试串行。\n",
 '### Q5 标准四核对比\n',
 '同一离线夹具、同一解释器、两条备均真实接收，--process-per-group --seconds 15 --confine-cpus 4 --backup-count 2；每个rate两次。1896/3792是原脚本主+单备的rate口径，两备实际总物理帧率分别2844/5688；不把主流量减半来声称覆盖峰值。25个基线生产模块逐文件对照e90ce02，只有ws_group_process的回放夹具/计时函数按清单等量覆盖；observer/merge/恢复逻辑均保持原版，见baseline-manifest与patch。\n',
 '|版本|rate|次|queue p99/max ms|kick→转发最大ms|kick→父应用最大ms|ready样本比|CPU核|valid/SHA|\n|---|---:|---:|---:|---:|---:|---:|---:|---|']
for row in matrix:
 r=row['result'];q=r['primary_queue_wait_ms'];parent=max((g.get('kick_to_parent_apply_ms',0) for g in r.get('failover_stage_traces',[])),default=0)
 assert r['final_book_sha256']==r['expected_book_sha256']
 lines.append(f"|{row['phase']}|{row['rate']}|{row['repeat']}|{q['p99']:.3f}/{q['max']:.3f}|{r['failover_gap_max_s']*1000:.3f}|{parent:.3f}|{r['ready_ratio']:.6f}|{r['measured_cpu_cores']:.3f}|true/一致|")
lines += ['\n以上failover为mark_kick→子进程下一次转发，父应用列另计IPC；不等于包含断前静默的业务merged_gap。全矩阵1008=0、队列/IPC未丢、重启0、意外主失败0；每次启动四组均两ready备。逐事件时序、完整SHA及queue的1s序列在comparison/*.json。\n',
 '### Q1/Q2 无码断开专项\n',
 '|版本|ready备|need_book|REST数|等待盘口ms|close→晋升ms|close→首次有效ms|合流业务gap ms|最终SHA|\n|---|---:|---|---:|---:|---:|---:|---:|---|']
for phase,r in handoff.items():
 lines.append(f"|{phase}|{r['ready_backups']}|{r['need_book_seen']}|{r['rest_requests']}|{r['need_book_wait_ms']:.3f}|{r['promotion_ms']:.3f}|{r['detection_to_first_valid_publication_ms']:.3f}|{r['event']['merged_gap_ms']:.3f}|一致|")
lines += ['\n专项主先静默1.15s、备继续增量，后无码关闭；REST延迟330ms，关闭后故意留250ms无新帧，验证空闲worker也会被确切关闭唤醒。前版可复现REST/need_book等待；后版不请求REST，静默期间已经由验证热备供数。400–530ms旧线上字段不是纯晋升耗时，本节才区分晋升/首发/IPC。未新增预测性关闭通知；正常主或旧代际通知不会唤醒并误晋升。\n',
 '### Q6/Q7 四秒及连续坏主\n',
 '|版本|场景|实际最大发布间隔ms|REST数|物理切换时刻（距场景开始s）|SHA/valid|\n|---|---|---:|---:|---|---|']
for row in silence:
 r=row['result'];switches=', '.join(f"{v['seconds']:.3f}" for v in r['switches'])
 lines.append(f"|{row['phase']}|{row['scenario']}|{r['max_publication_gap_ms']:.3f}|{r['rest_requests']}|{switches}|一致/true|")
lines += ['\n基线连续坏主仍走组冷却→PONG超时；新版新主立即具备独立字节保护，先由备合流、后正式切换，没有跨source冷却。真实全源无数据不能凭空保证1.5s：不伪造book、不以定时刷新旧书冒充发布；本次生产三个4s窗口的TCP备流实际均继续前进（前文已反证“merged_receipt_gap等于三路字节静默”）。\n',
 '安静市场反例：最后PONG年龄均匀0–10s、正常主副本落后0–800ms的固定种子1000场景，原谓词提前晋升638次，新版0次；只说明该限定合成输入族，不是实网误晋升率/置信区间。新物理判据保留4s，同时要求备数据持续至少1s、相邻≤1s，任何主字节重置；250ms备供数不切换连接。新增单条抢先报价、主字节恢复、稀疏不连续证据、两备并发一次决策测试。历史没有逐帧全副本数据，无法诚实给全市场实网误晋升概率；上线须对比spare先发后主恢复与真正断流事件。\n',
 'Q7对最终合流gap严格>1500ms判true；pending/无基线为null，不能算通过；独立physical_source_silence_alert_only不豁免合流违约。真实1554.872、1628.046、30657.5ms及1500ms边界均有行为测试。\n',
 '### Q1/Q3 四核容量与每备成本\n']
cpu=capacity['measured_cpu_cores'];copies=capacity['backup_book_cores_by_copy']
lines += [f"额外30s无踢、3792rate、两备、四核容量运行：valid=true，ready={capacity['ready_ratio']:.6f}，主queue p99={capacity['primary_queue_wait_ms']['p99']:.3f}ms，最终SHA一致，意外晋升0。总测得**{cpu:.3f}核**，四核余量**{4-cpu:.3f}核（{(4-cpu)/4*100:.1f}%）**。包含子进程+父应用/纸面history+回放生成/IPC；不把各线程CPU重复相加。该容量回放初始化{capacity['startup']['seconds']:.3f}s、总启动CPU {capacity['startup']['cpu_s']:.3f}核秒；详细启动阶段在capacity.json及comparison的startup，不混入稳态。\n",
 '|组/物理备|parse核|维护盘口核|供数判定核|\n|---|---:|---:|---:|']
for source,cost in copies.items():
 lines.append(f"|{source}|{cost.get('backup_parse',0):.5f}|{cost.get('backup_apply',0):.5f}|{cost.get('backup_fanin',0):.5f}|")
lines += ['\n每备实测成本覆盖各组流量权重，不能用最冷组平均值冒充BTC5m。补充微基准backup-cost-final.json的单/双解码末态SHA一致。两倍流量离线结果有上述余量；没有部署，不能宣称已经在真实21:30高峰、TLS/公网/线上SQL背景下验收通过。部署前后继续观察总CPU、PSI、分阶段线程CPU与queue p99，不能仅以平均CPU低就忽略调度尾部。\n',
 '### Q6 内存核对\n']
samples=memory['samples'];first,last=samples[1],samples[-1]
lines += [f"原生产07:41:24 RSS535,465,984B→08:18:50 609,374,208B→09:25:24 636,211,200B，增长约100.75MB；08:15之后显式history+aux只增加约5.1MB，单凭预算不能解释RSS。离线{memory['generations']}代、{memory['physical_sockets']}个真实observer reader/worker配合假socket，逐代供数、晋升、退休：每个采样退休manager活引用均0；20代→60代追踪Python活堆变化{memory['python_growth_after_warmup']}B，RSS {first['rss_bytes']/1e6:.3f}→{last['rss_bytes']/1e6:.3f}MB。完整曲线在memory-check.json。\n",
 '结论：该受控生命周期未发现无界Python对象/退休连接留存，盘口/checkpoints/dedup/continuity/历史均有容量或代际裁剪；生产RSS增长仍不能被现有材料严格归因，也不能排除native/TLS/allocator留存。故不添加无根因的gc/malloc_trim或改内存闸门。后续授权部署才可用各进程RSS与缓存计数/线程数长窗对照，必要时在复现环境分配栈采样。\n',
 '### Q4 换场与最终影响面\n',
 '新场gap=两个outcome各自首次有效发布相对eventStartTime的延迟取最大；跨市场间隔另列max(新场两边首发)-min(旧场两边各自最后有效发布)，采用双边保守口径。startup单列；无边界/首发仍为pending或unmeasured。旧07:40无足够首发原始数据，不能补造精确毫秒。ws_rollover_gaps、failure的baseline_kind与测试已落地，不改变发现/订阅选择。\n',
 '最终生产改动只涉及observer、WS warm book/merge/payload/子进程、换场指标；没有改模型、钱包、签名、下单、CLOB凭证、PAPER_AUTO、地区闸门。沿用r20两备预算，保留所有拨号/订阅/REST/心跳限制；不拆BTC5m token。全程不部署、不force push，只交付fix/pm-ws-r21。\n',
 '### 失败运行与风险保留\n',
 'validation-attempts保留初次测试契约失败、组合进程换场失败及修正后的完整复跑；pre-quiet-guard保留补充冷静保护前的八次有效性能运行。额外容量曾遇到wall receipt gap约1152s、monotonic测量窗口仅90.8s的时钟/执行暂停异常，valid=false（分类正确为business_gap=true），保存在capacity-clock-jump；不删除、不计通过，不把异常值作容量证明，也不声称已区分NTP跳时和宿主暂停。上表使用冻结版稳定窗口的重新测量。\n',
 '### 授权部署后应观察（本轮不部署）\n',
 '- 每个ws_hot_primary_failures事件的merged_gap_ms/final/business_gap、pending/unmeasured；任何已完成>1500ms均违约。双踢business_gaps_current_hour≤1，1008=0，1013单列。\n',
 '- 新主的source/physical_role/promoted_at、ws_liveness_silence.suppressed预期0、4s阈值与1s备持续证据，逐事件确认没有回落到PONG超时；ws_warm_backup_supply记录的是上次供数资格/发布，不代替当前ready或完整基线验收。\n',
 '- 主静默时热备合流的首次发布延迟、baseline_verified/need_book_assets、REST请求与回退原因；SHA不证明成交历史完整，成交丢失及物理源>20s只作独立告警。\n',
 '- 两备ready覆盖、补备期间零备秒、四核峰值CPU/PSI、各线程stage_cpu、每备维护成本、queue/IPC丢失及最长延迟；真实21:30双倍流量仍须实测。\n',
 '- 换场scheduled_start→双边首发、discovery_lateness、startup与unmeasured；冷门按delivery_latency，不按自然静默判失败。\n',
 '- 各进程RSS、cache charged bytes/entries、退休线程/连接、checkpoint资产与数量的长期趋势；受控内存测试不等价于已排除生产原生泄漏。\n',
 '代码提交保留原4个主提交及5个fixup历史：Q1 6ef7717，Q2 2779fc5，Q3 acdb663，Q4 e0744ba；Q5 36ea588/73d7a16；Q6 73f02a6/f3d0557/3657dc1及行为测试提交；Q7 60909f2。最终验证产物与本报告另提交，正常push至origin/fix/pm-ws-r21。\n']
p=root/'deploy/pm_ws_round21_20261022.md';s=p.read_text().split('\n## 最终冻结版本验证')[0]
p.write_text((s+'\n'.join(lines)).rstrip()+'\n')
print('final report written',p)
