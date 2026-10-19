import json, time, gzip, threading, urllib.request, sys, os
import websocket
URL='wss://ws-subscriptions-clob.polymarket.com/ws/market'
SERIES=[('btc','15m',900),('btc','5m',300),('eth','15m',900),('eth','5m',300)]
DUR=float(sys.argv[1]) if len(sys.argv)>1 else 2100
out=gzip.open(''+os.environ.get('REC_OUT','/tmp/fixC/frames.jsonl.gz')+'','wt')
lock=threading.Lock(); stop=time.time()+DUR
def tokens(asset,iv,period):
    now=int(time.time()); res=[]
    for start in (now//period*period, now//period*period+period):
        try:
            d=json.loads(urllib.request.urlopen(urllib.request.Request(f'https://gamma-api.polymarket.com/markets?slug={asset}-updown-{iv}-{start}',headers={'User-Agent':'curl/8.5.0'}),timeout=10).read())
            if d: res+=json.loads(d[0]['clobTokenIds'])
        except Exception as e: print('gamma',e,flush=True)
    return res
def run(asset,iv,period,key):
    while time.time()<stop:
        cur=tokens(asset,iv,period); subs={'a':set(cur)}; last={'t':time.time()}
        def on_open(ws):
            ws.send(json.dumps(dict(assets_ids=cur,type='market')))
            def hb():
                while ws.keep_running and time.time()<stop:
                    time.sleep(10)
                    if time.time()-last['t']>30: break
                    try: ws.send('PING')
                    except Exception: return
                    new=tokens(asset,iv,period)
                    add=[t for t in new if t not in subs['a']]
                    if add:
                        ws.send(json.dumps(dict(assets_ids=add,operation='subscribe'))); subs['a'].update(add)
                        with lock: out.write(json.dumps(dict(t=time.time(),k=key,sub=add))+'\n')
                ws.close()
            threading.Thread(target=hb,daemon=True).start()
        def on_msg(ws,raw):
            t=time.time(); last['t']=t
            with lock: out.write(json.dumps(dict(t=t,k=key,r=raw))+'\n')
        def on_close(ws,c,r):
            with lock: out.write(json.dumps(dict(t=time.time(),k=key,close=c,reason=str(r)))+'\n')
        ws=websocket.WebSocketApp(URL,on_open=on_open,on_message=on_msg,on_close=on_close)
        ws.run_forever()
        time.sleep(2)
ths=[threading.Thread(target=run,args=(a,i,p,f'{a.upper()}:{i}')) for a,i,p in SERIES]
for t in ths: t.start()
for t in ths: t.join()
out.close(); print('DONE',flush=True)
