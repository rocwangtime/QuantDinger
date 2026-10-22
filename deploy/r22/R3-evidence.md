# R3 修改前证据与profile
生产 h2225/cpu.txt：docker最高323.62%，PSI cpu some avg10最高36.89%；top.txt显示两个热门子进程81.8%。h1622/check.txt/obs.log.gz/tail.txt保留另一个采样窗。线上只有stage累计值，不宣称能从top精确拆函数占比。
离线真实WarmBook.receive+loads+decoded_envelope，20,000帧(20%140档双边book、80%八行delta)，warm-profile-before.txt保留三次未profile计时及cProfile：总CPU1.221/1.253/1.302s，parse .534/.549/.572s，apply .671/.689/.714s；profile累计price_rows .989s（fingerprint .530s），decoded_envelope 1.567s（递归_has_envelope_key .949s）。私有warm ladder不做副本dedup，却走逐行canonical JSON+blake2b；decoded_envelope为了与regex兼容还遍历每档，与生产完整盘口应用无关。
拟仅让WarmBook私有盘口应用直接读取level key；liveness路径用已解析事件抽取tips，保留完整独立apply、hash checkpoint、共同水印、逐档时序与代际闸门。不会惰性丢增量或依赖REST；不共享可变盘口。影响只私有盘口维护和已解析envelope提取；公共ParsedFrames/FeedMerge仍保留dedup指纹。四核矩阵与滞后回放均包含R1额外供数成本。

代码定位：c6acf50 ws_rotation.py:95通过price_rows计算私有盘口不需要的指纹；ws_payloads.py:337 decoded_envelope递归检查深度；profile逐函数行号保留在warm-profile-before.txt。最终对比另含3792rate/40秒/四核持续fan-in容量变体，覆盖R1合流负载。
