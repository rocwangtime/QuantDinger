"""Descriptive decision-ledger counts, never inferred trading accuracy."""
from collections import Counter


def summarize(runs):
    states, actions, causes = Counter(), Counter(), Counter()
    decisions = previews = protective = 0
    for run in runs:
        states[run['status']] += 1
        result = run.get('result') or {}
        if result.get('source') == 'deterministic_protection':
            protective += 1
            continue
        if 'items' not in result:
            continue
        decisions += 1
        previews += int(bool(run.get('preview')))
        actions.update(item['action'] for item in result['items'])
        event = result.get('event') or (run.get('evidence') or {}).get('event') or {}
        causes[event.get('type', 'manual_preview' if run.get('preview') else 'scheduled_or_price_trigger')] += 1
    return {'window': 'latest_30_runs', 'runs': len(runs), 'valid_model_decisions': decisions,
            'preview_decisions': previews, 'protective_runs': protective,
            'statuses': dict(states), 'proposed_actions': dict(actions), 'decision_causes': dict(causes)}
