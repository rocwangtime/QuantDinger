"""Validated, request-local Agent model choices. Never accept client credentials/URLs."""
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import os
import re

from flask import g, has_request_context, jsonify, request

_selection = ContextVar('agent_llm_selection', default=None)


def current_selection():
    if has_request_context():
        return dict(getattr(g, 'agent_llm_selection', None) or {})
    return dict(_selection.get() or {})


def reasoning_options(provider, model):
    """Conservative capability table: unknown models retain provider defaults.

    Sources checked 2026-10-03: developers.openai.com/api/docs/models/gpt-5.4
    and docs.volcengine.com/docs/ark/deep-thinking?lang=zh (Chat API).
    """
    options = ['default']
    if provider == 'volcengine' and model in {
        'deepseek-v4-1-flash-260910', 'deepseek-v4-pro-ga-260813',
        'deepseek-v4-flash-ga-260731',
    }:
        return options + ['none', 'low', 'high', 'max']
    if provider == 'openai':
        if re.fullmatch(r'gpt-5\.[24](?:-\d{4}-\d{2}-\d{2})?', model):
            return options + ['none', 'low', 'medium', 'high', 'xhigh']
        if re.fullmatch(r'(?:o3(?:-mini)?|o4-mini)(?:-\d{4}-\d{2}-\d{2})?', model):
            return options + ['low', 'medium', 'high']
    return options


def model_catalog():
    from app.services.llm import LLMProvider, LLMService
    from app.utils.config_loader import load_addon_config

    service = LLMService(selection={})
    config = load_addon_config()
    items = []
    for provider in LLMProvider:
        # LiteLLM's historical is_configured=True is not evidence of a credential.
        if provider.value == 'litellm' or not service.is_configured(provider):
            continue
        models = config.get(provider.value, {}).get('models') or os.getenv(f'{provider.value.upper()}_MODELS', '')
        if isinstance(models, str):
            models = models.split(',')
        if not isinstance(models, list):
            models = []
        default = service.get_default_model(provider)
        for model in dict.fromkeys([default, *[m.strip() for m in models if isinstance(m, str)]]):
            if model and len(model) <= 200:
                items.append({'provider': provider.value, 'model': model,
                              'reasoning_options': reasoning_options(provider.value, model)})
    return {'items': items, 'default': {'provider': service.provider.value,
                                       'model': service.get_default_model(), 'reasoning_effort': 'default'}}


def validate_selection(raw):
    if raw is None or raw == {}:
        return {}
    if not isinstance(raw, dict) or set(raw) - {'provider', 'model', 'reasoning_effort'}:
        raise ValueError('Invalid Agent model selection')
    provider, model = raw.get('provider'), raw.get('model')
    effort = raw.get('reasoning_effort', 'default')
    choice = next((item for item in model_catalog()['items']
                   if item['provider'] == provider and item['model'] == model), None)
    if not choice:
        raise ValueError('所选模型未配置或已移除，请刷新模型列表 / Selected model is not configured')
    if effort not in choice['reasoning_options']:
        raise ValueError('所选模型不支持该思考深度 / Unsupported reasoning effort')
    return {'provider': provider, 'model': model, 'reasoning_effort': effort}


@contextmanager
def selection_scope(raw):
    token = _selection.set(validate_selection(raw))
    try:
        yield
    finally:
        _selection.reset(token)


def agent_model_selection(fn):
    """Run after authentication and before billing, including SSE initialization."""
    @wraps(fn)
    def wrapped(*args, **kwargs):
        data = request.get_json(silent=True) or {}
        raw = data.get('llm_selection') if isinstance(data, dict) else None
        if request.method == 'GET' and request.args.get('llm_provider'):
            raw = {'provider': request.args.get('llm_provider'), 'model': request.args.get('llm_model'),
                   'reasoning_effort': request.args.get('reasoning_effort', 'default')}
        try:
            g.agent_llm_selection = validate_selection(raw)
        except ValueError:
            return jsonify({'code': 0, 'msg': '模型未配置、已移除或不支持所选思考深度，请刷新模型列表 / Invalid model selection or reasoning effort', 'data': None}), 400
        return fn(*args, **kwargs)
    return wrapped


def apply_reasoning(payload, provider, model, effort):
    if effort not in reasoning_options(provider, model):
        raise ValueError('Unsupported reasoning effort')
    if effort != 'default':
        if provider == 'volcengine':
            payload['thinking'] = {'type': 'disabled' if effort == 'none' else 'enabled'}
            if effort != 'none':
                payload['reasoning_effort'] = effort
        elif provider == 'openai':
            payload['reasoning_effort'] = effort
    if provider == 'openai' and (model.startswith('gpt-5') or re.match(r'^o[134](?:-|$)', model)):
        payload['max_completion_tokens'] = payload.pop('max_tokens')
        # Omit sampling knobs for reasoning models (also safe for default/none).
        payload.pop('temperature', None)
    return payload
