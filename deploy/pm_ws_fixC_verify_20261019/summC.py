import json,sys
for l in open('/tmp/fixC/gateC.jsonl'):
    d=json.loads(l); r=d['r']
    pass
    if r.get('mode')=='process-per-group':
        g=r['primary_queue_age_by_group']; b5=g.get('up_down:BTC:5m',{})
        # BTC5m backup ready ratio not given per group -> computed separately if samples present
        print(f"{d['label']:22s} pf={d['primary_first']} pl={d['parent_load']} valid={r['valid']} pq_p99={r['primary_queue_wait_ms']['p99']:.1f} pq_max={r['primary_queue_wait_ms']['max']:.0f} btc5_p99={b5.get('p99') or 0:.1f} btc5_series_max={b5.get('series_max_ms') or 0:.0f} bq_p99={r['backup_queue_wait_ms']['p99']:.0f} ready={r['ready_ratio']:.3f} gaps={[round(x,3) for x in r['failover_gaps_s']]} 1008={r['codes_1008']} restarts={r['restarts']} obs_pk={d['obs_peak10s_prod_cores']:.2f} bg={d['bg_at_peak_prod_cores']:.2f} tot={d['total_peak10s_prod_cores']:.2f} stale={r['stale_skipped']} sha_ok={r['final_book_sha256']==r['expected_book_sha256']}")
    else:
        print(f"{d['label']:22s} SP valid={r.get('valid')} pq_p99={r['primary_queue_wait_ms']['p99']:.1f} pq_max={r['primary_queue_wait_ms']['max']:.0f} ready={r.get('ready_ratio')} peak_bq={r.get('peak_backup_queue_ms')} obs_pk={d['obs_peak10s_prod_cores']:.2f} bg={d['bg_at_peak_prod_cores']:.2f} tot={d['total_peak10s_prod_cores']:.2f}")
