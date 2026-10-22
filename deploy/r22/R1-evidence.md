# R1 修改前证据与影响面
基线 c6acf50。生产原始根 /workspace/quantdinger/deploy/p35/h0021：ROOT_CAUSE.md §1–3，ev.json，mon_2350_0013.txt。
16:07:33/16:08:34 UTC exchange→receipt=7412/20116ms；主311–637KB/s、byte_silence≤120ms、pong 3.49–9.52s，备pong 187–190ms。本地queue_wait 4.5–47ms，scheduler≤10.8ms。业务事件26 gap=7259.6ms。
observer.py:1661–1664/1763–1772 仅依据字节静默，所有新鲜度故障均不可见。必须同时记录每源盘口时间及本机接收时钟，与ready备的新盘口正面比较；250ms无发布仅允许已校验备供数，绝不单独晋升。连续2s的相对滞后且本地队列健康才归upstream_lag；本地积压归ingress_pressure。
影响面：热门组warm备供数与现有晋升入口；不增连接、订阅、REST或心跳。已对照official_limits_20261017：不改任何外部频率。主要风险是备合流CPU及误判本地压力；用原盘口共同水印/代际闸门、接收时钟、持续证据和本地积压否决保护。冷门/全源静默无新证据不触发。

正式回放追加根因（修改前取证）：lag-debug.json/pre_kicks[1]中feed-4的peer checkpoint=501ay/an（17:11:44.396），共享merge.latest仍是500（17:11:44.376）；备feed-2已更新650且含peer共同历史，却baseline=null。前一踢前所有备与共享盘口、checkpoint均相等。c6acf50也在第二踢出现此不一致（validation-attempts/下保留）。代码observer.py原:3658–3663在Pipeline代际检查/merge之前调用_remember_books；关闭可在两者之间使inbox失效，raw水印已前进而共享盘口没有前进。不是hash被篡改，也不能跳过校验。
因此R1还须将共享交接checkpoint的推进与Pipeline接受的合流事件绑定；raw book_marks仍保留给既有快照hold解析，私有WarmBook继续独立维护。影响面是主worker交接水印，不能放宽baseline或REST闸门；新增关闭竞态反例，重跑完整PG18及全部正式回放。
