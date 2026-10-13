"""Persisted US/HK fast-path timings; incomplete stages never count as zero."""
import math
import time

from app.services.automation import store

STAGES = ('trigger_detected', 'context_built', 'model_call', 'decision', 'submitted')


def enabled(row, run):
    return row['config']['kind'] == 'price_trigger' and row['config'].get('market') in {'HKStock', 'USStock'} and not run['preview']


def mark(run, stage):
    timing = run.setdefault('_latency', {'timestamps': {}, 'elapsed_ms': {}})
    now = time.time()
    timing['timestamps'][stage] = now
    if stage == 'trigger_detected':
        run['_latency_origin'] = time.monotonic()
    origin = run.get('_latency_origin')
    timing['elapsed_ms'][stage] = round((time.monotonic() - origin) * 1000, 3) if origin is not None else round(
        (now - timing['timestamps']['trigger_detected']) * 1000, 3)


def persist(run):
    if run.get('_latency'):
        store.query("""UPDATE qd_agent_automation_runs SET result=jsonb_set(
            COALESCE(result,'{}'::jsonb),'{fast_latency}',%s::jsonb) WHERE id=%s RETURNING id""",
            (store.dumps(run['_latency']), run['id']), one=True)


def summarize(rows):
    samples = {stage: [] for stage in STAGES[1:]}
    samples['total'] = []
    for row in rows:
        elapsed = ((row.get('result') or {}).get('fast_latency') or {}).get('elapsed_ms', {})
        for previous, stage in zip(STAGES, STAGES[1:]):
            if stage in elapsed and previous in elapsed:
                samples[stage].append(max(0, elapsed[stage] - elapsed[previous]))
        if 'finished' in elapsed:
            samples['total'].append(elapsed['finished'])
        elif row.get('finished_at') and ((row.get('result') or {}).get('fast_latency') or {}).get('timestamps', {}).get('trigger_detected'):
            origin = row['result']['fast_latency']['timestamps']['trigger_detected']
            samples['total'].append(max(0, (row['finished_at'].timestamp() - origin)*1000))
    def percentile(values, p):
        return round(sorted(values)[max(0, math.ceil(len(values)*p)-1)], 3) if values else None
    return {'sampled_runs': len(rows), 'expired_runs': sum(r.get('status') == 'expired' for r in rows),
            'expired_rate': sum(r.get('status') == 'expired' for r in rows)/len(rows) if rows else None,
            'stages_ms': {key: {'count': len(values), 'p50': percentile(values, .5), 'p95': percentile(values, .95)}
                          for key, values in samples.items()}}


def read(row):
    rows = store.query("""SELECT result,status,finished_at FROM qd_agent_automation_runs
        WHERE task_id=%s AND user_id=%s AND preview=FALSE ORDER BY id DESC LIMIT 1000""",
        (row['id'], row['user_id']))
    return {**summarize(rows), 'window': 'latest_1000_non_preview_runs', 'percentile_method': 'nearest_rank'}
