"""Latency HTTP contract retains authentication and task ownership checks."""
from flask import Flask

from app.routes import agent_automations as routes
from app.services.automation import latency


def test_latency_endpoint_uses_session_owner_and_rejects_other_task(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(routes.blp, url_prefix='/api/agent-automations')
    seen = []
    def task(user_id, task_id):
        seen.append((user_id, task_id))
        if user_id != 42 or task_id != 7:
            raise ValueError('Task not found')
        return {'id': task_id, 'user_id': user_id}
    monkeypatch.setattr(routes.store, 'task', task)
    monkeypatch.setattr(latency, 'read', lambda row: {'sampled_runs': 3, 'stages_ms': {}})
    client = app.test_client()
    assert client.get('/api/agent-automations/7/latency').status_code == 401
    monkeypatch.setattr('app.utils.auth.verify_token', lambda _: {'user_id': 42, '_verified_user_role': 'user'})
    headers = {'Authorization': 'Bearer fixture-only'}
    response = client.get('/api/agent-automations/7/latency', headers=headers)
    assert response.status_code == 200 and response.get_json()['data']['sampled_runs'] == 3
    assert client.get('/api/agent-automations/8/latency', headers=headers).status_code == 400
    monkeypatch.setattr('app.utils.auth.verify_token', lambda _: {'user_id': 43, '_verified_user_role': 'user'})
    assert client.get('/api/agent-automations/7/latency', headers=headers).status_code == 400
    assert seen == [(42, 7), (42, 8), (43, 7)]
