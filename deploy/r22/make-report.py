"""Render the Chinese delivery report from frozen verification artifacts."""
from pathlib import Path
import hashlib
import json
import statistics
import subprocess
root=Path(__file__).resolve().parents[2]
out=root/'deploy/r22'
suite=json.loads((out/'suite-summary.json').read_text())
assert suite['failed']==suite['errors']==0
for rel,digest in suite['source_hashes'].items():assert hashlib.sha256((root/rel).read_bytes()).hexdigest()==digest,rel
matrix=json.loads((out/'comparison/results.json').read_text())
assert len(matrix)==8 and all(x['exit']==0 and x['result']['valid'] and x['result']['codes_1008']==0 for x in matrix)
for rate in (1896,3792):
 rows=[x['result'] for x in matrix if x['rate']==rate]
 assert len({r['final_book_sha256'] for r in rows})==1
 assert all(r['final_book_sha256']==r['expected_book_sha256'] for r in rows)
lag=json.loads((out/'lag-comparison/results.json').read_text())
assert all(x['exit']==0 and x['result']['valid'] for x in lag)
by={x['phase']:x['result'] for x in lag}
assert by['before']['max_publication_gap_ms']>1500 and by['after']['max_publication_gap_ms']<=1500
assert by['before']['primary_min_bytes_per_second']>=300000
assert by['after']['reason']=='upstream_lag' and by['pressure']['reason']=='ingress_pressure'
assert by['pressure']['classification']=='ingress_pressure' and not by['quiet']['switches']
profiles={phase:json.loads((out/f'warm-profile-final-{phase}.txt').read_text().splitlines()[0]) for phase in ('before','after')}
assert len({x['sha256'] for p in profiles.values() for x in p['results']})==1
profile_mean={phase:statistics.mean(x['cpu_seconds'] for x in p['results']) for phase,p in profiles.items()}
profile_drop=(1-profile_mean['after']/profile_mean['before'])*100
mt=[]
for x in matrix:
 r=x['result'];q=r['primary_queue_wait_ms']
 parent=max((t.get('kick_to_parent_apply_ms',0) for t in r['failover_stage_traces']),default=0)
 mt.append(f"|{x['phase']}|{x['rate']}|{x['repeat']}|{q['p99']:.3f}/{q['max']:.3f}|{r['failover_gap_max_s']*1000:.3f}|{parent:.3f}|{r['ready_ratio']:.6f}|{r['measured_cpu_cores']:.3f}|true/一致|")
ct=[]
for rate in (1896,3792):
 values={phase:[x['result'] for x in matrix if x['phase']==phase and x['rate']==rate] for phase in ('before','after')}
 cpu={phase:statistics.mean(r['measured_cpu_cores'] for r in rs) for phase,rs in values.items()}
 btc={phase:statistics.mean(sum(r['cpu_breakdown']['by_group']['up_down:BTC:5m'].get(k,0) for k in ('backup_parse','backup_apply')) for r in rs) for phase,rs in values.items()}
 ct.append(f"|{rate}|{cpu['before']:.3f}→{cpu['after']:.3f}|{(1-cpu['after']/cpu['before'])*100:.1f}%|{btc['before']:.4f}→{btc['after']:.4f}|{(1-btc['after']/btc['before'])*100:.1f}%|")
capacity=json.loads((out/'capacity-fanin-3792.json').read_text())
cap=capacity['result']
assert capacity['exit']==0 and cap['valid'] and cap['codes_1008']==0
assert cap['final_book_sha256']==cap['expected_book_sha256']
cap_events=[e for group in cap['primary_failure_events'].values() for e in group['events']]
assert len(cap_events)==4 and all(e['merged_gap_final'] and e['merged_gap_ms']<=1500 for e in cap_events)
cap_gap=max(e['merged_gap_ms'] for e in cap_events)
lt=[]
for phase in ('before','after','pressure'):
 r=by[phase];dk=r['double_kicks'];dkmax=max((e.get('merged_gap_ms') or 0 for e in dk['events']),default=0)
 lt.append(f"|{phase}|{r['max_publication_gap_ms']:.3f}|{format(r['primary_min_bytes_per_second']/1000,'.1f') if r['primary_min_bytes_per_second'] else '—'}|{r.get('reason') or 'transport'}|{dkmax:.3f}|{r['rest_requests']}|{r['measured_cpu_cores']:.4f}|一致/true|")
commits=subprocess.check_output(['git','log','--format=%h %s','c6acf50..HEAD'],cwd=root,text=True).strip()
text=f'''# Polymarket WS 第22轮（2026-10-22）

工作区 `/workspace/quantdinger/wt_r22`，分支 `fix/pm-ws-r22`，基线 `c6acf50`（PR #89 / r21 生产镜像）。开始时工作树干净。原始生产证据根 `/workspace/quantdinger/deploy/p35`；本文验证文件相对根为 `wt_r22/deploy/r22/`。

只修改公开观察器、WS指标、纸面 /time 探针及离线验证。未部署、未启用 PAPER_AUTO，未改模型、交易闸门、钱包、签名、真实订单或 CLOB 凭证；未使用代理或绕过地区限制。不拆 BTC5m token。连接预算、订阅、PING 和 REST 频率均未改变，已对照 `deploy/pm_ws_official_limits_20261017.md`。

## R1：以新鲜度识别“活着但滞后”，水印随有效合流提交

修改前证据见 `R1-evidence.md`、生产 `h0021/ROOT_CAUSE.md`、`mon_2350_0013.txt`、`ev.json`。生产事件26的业务 gap=7259.584ms；主仍311–637KB/s、pong 3.49–9.52s，而备约0.19s。本地 queue_wait 4.5–47ms，不能归因于本地积压。原 observer.py:1661–1664、1763–1772 的字节静默判据对这种故障不可见。`production-evidence.json`保留原始行号与事件摘录。

`observer.py:_feed_freshness/_freshness_decision`按资产保存真实接收时间与交易所时间。ready 备必须有本机最近500ms内收到、且交易所时间比主更新的实际盘口证据，才允许供数。热门资产合流发布超过250ms、主 pong>1.5s且备<0.5s，或主交易所滞后>1.5s持续1s，均可启用已校验备供数；后者同时要求主备相对 ingress lag 差>1s，以排除公共时钟偏差。持续2s并再次核验队列、TCP pending、ready 与共同水印后，标记 `upstream_lag`，走既有晋升路径。冷清/全源安静不会仅因没发布而晋升。供数在主恢复后退出，保留短暂500ms连续供数窗口，避免永久放大备合流成本。

正式回放还揭示既存水印竞态：`lag-debug.json`中共享 merge.latest=500、主checkpoint=501，备已650且有共同历史，校验仍失败。原 `_remember_books`在代际检查与合流前推进 raw 水印，关闭可使这一帧最终不发布。修复让交接 checkpoint 只随实际接受的 merged publication 推进，raw book_marks仍保留给快照hold。确定性关闭竞态测试在原版失败（`checkpoint-baseline.log`），修复后通过且0 REST；没有跳过或放宽 baseline 校验。

影响面：热门 liveness warm 备供数、主 worker 的交接水印；原逐档时间 fence、重复交易语义、订阅/队列代际、完整盘口及共同水印闸门继续生效。冷门只新增 ingress lag 诊断。新增行为测试覆盖持续字节、RTT/相对滞后、无备新证据、未校验盘口、本地积压、恢复退出及关闭竞态。

## R2：统一业务缺口口径与来源归属

证据见 `R2-evidence.md`：生产 stale 在故障窗口分别增长15.1k/21.4k/11.8k，却没有来源；首踢7259.6ms，双踢只计第二次6.967ms。新增 FeedMerge 和 Recorder 的 `stale_by_source`；IPC显式携带source，使父进程纸面Recorder也能归属。每条feed报告 `exchange_lag_seconds` 与 `sample_age_seconds`，role/source另列。

DoubleKicks引用两踢关联的主故障事件并报告最大业务发布缺口，不再只报第二踢。主故障滚动小时计数按故障发生时刻归窗，按 business_event_id 去重；64条诊断尾部与最多4096条滚动引用分开。所有业务违约均要求 final 且 merged_gap_ms>1500；pending/unmeasured保持未知。容量截断显式报告，不能当作完整计数。物理静默>20s和成交历史缺失继续独立告警，冷门继续按交付延迟。

影响面仅诊断与IPC的可选来源字段，不更改事件内容或外部请求。source计数有界，超出保留上限的来源汇总；滚动引用有上限。测试验证真实7259.6/6.967反例、1500ms边界、跨小时、去重、pending、100条事件计数及子→父来源传递。

## R3：减少私有盘口无用解析与指纹

修改前证据见 `R3-evidence.md`：`h2225/cpu.txt`最高323.62% CPU、PSI avg10=36.89%；`top.txt`和`h1622/`是生产背景。没有从top伪造逐函数占比。

先 profile 真实 `loads + decoded_envelope + WarmBook.receive + checkpoints`：20,000帧，20%双资产140档book，80%八行delta；profile中递归 envelope 扫描、逐行 canonical JSON/blake2b 是可省成本。私有 ShadowBooks 已经不做副本去重，却仍通过 price_rows 计算指纹。改为直接取资产/side/price键；已解析事件通过 feed_envelope提取合法公开载荷的时间/资产/hash。公开FeedMerge的去重与指纹保持。独立完整盘口、逐档时序、hash checkpoint、共同水印和直接交接全部保留，不惰性丢增量、不依赖REST。

最终同脚本微基准三次平均 CPU：{profile_mean['before']:.6f}s→{profile_mean['after']:.6f}s，下降{profile_drop:.1f}%，六次末态SHA一致。原始 profile 和三次计时见 `warm-profile-final-before.txt/after.txt`。这只度量备的解码/应用，不冒充整机收益。

|rate|总CPU核，前→后（两次均值）|下降|BTC5m两备 parse+apply核，前→后|下降|
|---:|---:|---:|---:|---:|
{chr(10).join(ct)}

额外40秒两倍流量容量变体：`--rate 3792 --confine-cpus 4 --backup-count 2`，`QD_REPLAY_KICK_MODE=stall:5`，四组各一次5s主静默，持续触发备供数。CPU **{cap['measured_cpu_cores']:.3f}核**，ready={cap['ready_ratio']:.6f}，queue p99/max={cap['primary_queue_wait_ms']['p99']:.3f}/{cap['primary_queue_wait_ms']['max']:.3f}ms；kick→首次备转发最大{cap['failover_gap_max_s']*1000:.3f}ms，四个实际故障业务gap最大{cap_gap:.3f}ms，全部final，SHA一致、valid=true、1008=0。该变体验证高流量持续fan-in成本，静默触发与RTT因果回放分开，见 `capacity-fanin-3792.json`。

影响面是所有私有 WS ShadowBooks（含热门 warm 备及迁移影子）的无用指纹移除，以及 liveness 已解析 envelope 提取；公共合流/交易去重不变。完整测试及四核矩阵覆盖正常、换代、乱序、删除和R1路径。滞后专项另计总进程CPU与 backup_fanin 阶段，包含R1持续补数和补备成本，未与全量测试并行。

## R4：纸面 /time 专用数值解析

证据 `p35/clob_valueerror_20261022.md`：HTTP200/text/plain十位数字每300s被JSON-only拒绝，108次错误，延迟模型退回固定150ms。新增 `PublicClient.server_time()`，沿用连接池恢复、超时与禁止重定向；仅该入口接受text/plain或JSON数值，最多64字节，去空白后严格ASCII 9–13位数字。其他接口仍JSON-only。`paper_account.Latency.measure`改调用此方法，频率不变。

影响面是纸面延迟探针及公开HTTP公共读取的等价拆分。测试含纯文本、JSON数字、HTML（含伪装text/plain）、JSON字符串/指数、小数位数/体积异常、其他接口仍拒绝text/plain，以及探针成功退出fallback/失败保留fallback。没有联网实测或改真实延迟模型参数。

## 最终冻结验证

Python3.12.15，临时 PostgreSQL18.0/UTF-8（`/tmp`、仅127.0.0.1，测试结束已停止）。416个测试文件：**{suite['passed']} passed / {suite['skipped']} skipped / 0 failed / 0 errors**。六组串行pytest、换场模块单独一组、冷门长模拟单独进程，文件清单和完整输出见 `accepted-suite/`，代码SHA256见 `suite-summary.json`。Ruff全后端及compileall通过，源码差异检查通过。

### 60秒滞后、连续无码断开及对照

使用真实生产reader/worker和假公开transport，无网络/SQL/凭证。50Hz、每帧约9KB padding，主在退休前持续至少300KB/s；主数据年龄从0增至约20s，pong RTT样本从0增至20s，备维持当前。末7s只交付旧行，建模生产 stale-fence 激增；原始生产没有逐帧备流，不能声称逐字节复原。物理关闭在完整帧之间注入；关闭中的水印竞态由独立确定性测试覆盖。预防性晋升后先补齐两备，再于T+60s关闭当前主，T+63s关闭接替者。新备不会在已有备接管前抢先创建。

|版本/场景|最大有效发布间隔ms|旧主最小KB/s|旧主失败原因|DK最大业务gap ms|REST数|观察器CPU核|SHA/valid|
|---|---:|---:|---|---:|---:|---:|---|
{chr(10).join(lt)}

修复版主在约{by['after']['switches'][0]['seconds']:.3f}s被预防性替换，业务缺口≤1500ms。`lag-comparison/`保存各失败事件、切换、双踢、校验前水印和CPU阶段。安静对照3s无新行情、0晋升；没有发布间隔样本，不把自然静默写成业务违约。压力对照对旧主worker实际施加SCHED_IDLE、仅夹具中每帧增加10ms thread CPU工作，并在同四核运行4个正常优先级CPU竞争进程，接管后停止竞争，分类/失败原因均为 ingress_pressure，不能冒充 upstream_lag；这不是生产CFS配额/TLS场景。

### 标准四核前后矩阵

同一脚本/解释器，`--process-per-group --seconds 15 --backup-count 2 --confine-cpus 4`，rate=1896/3792各两次。每组两备实际接收，物理总帧率为2844/5688；不把两备流量误写成原一备rate。27个基线生产模块逐文件与c6acf50核验，见 `source-manifest.json`，基线未覆盖任何生产修复。性能回放与PG全套串行。

|版本|rate|次|queue p99/max ms|kick→子转发最大ms|kick→父应用最大ms|ready_ratio|总CPU核|valid/SHA|
|---|---:|---:|---:|---:|---:|---:|---:|---|
{chr(10).join(mt)}

8次全为1008=0、valid=true、同rate前后及oracle SHA一致。queue_wait为实际FIFO/处理中最老帧；上表failover_gap是kick→下一转发，父应用另计IPC，不是包含断前静默的业务gap。业务验收使用专项实际有效发布间隔和 primary_failure 的 merged_gap_ms。CPU总值不重复相加各阶段；分阶段是归因辅助。

## 风险、失败运行与上线后观察（本轮不部署）

`validation-attempts/`保留临时PG生命周期、未初始化schema、SQL_ASCII、迁移角色污染、状态指标样本未填充，以及夹具补备/关闭竞态失败记录。它们没有计入最终通过数据；raw水印竞态促成了R1修复，最终源码之后重跑整套与正式回放。

离线合成流不含公网TLS、线上SQL或真实21:30高峰。R1在滞后时会增加备合流；已测该路径CPU，但不能承诺真实生产PSI下降幅度。所有副本真的无新数据时，不能凭空保证1.5s；不伪造旧盘口发布。备缺快照/hash/共同水印或代际失效仍保守走既有恢复。历史成交完整性不能由盘口SHA证明。滚动/来源计数均有边界及截断诊断。

授权部署后逐事件观察：merged_gap_ms/final/business_gap及pending/unmeasured；主故障与DoubleKicks的rolling_hour业务计数、DK窗口最大gap；1008须0、1013单列；各源交易所滞后与样本年龄、过期丢弃及逻辑/物理role；upstream_lag对照queue_wait/TCP pending/备更新，防止把本地CPU压力误判；两备ready与基线验证/need_book/REST；CPU/PSI、backup_parse/apply/fanin与队列尾部；纸面 /time samples/fallback/RTT恢复。冷门看delivery latency，物理>20s和成交丢失仅独立告警。

## 提交

{commits if commits else '四项独立提交将在全部验证完成后创建；最终交付时本节写入实际提交。'}
'''
(root/'deploy/pm_ws_round22_20261022.md').write_text(text)
Path('/workspace/quantdinger/deploy/pm_ws_round22_20261022.md').write_text(text)
print('report written; all artifact assertions passed')
