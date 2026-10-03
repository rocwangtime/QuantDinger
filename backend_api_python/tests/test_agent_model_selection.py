from concurrent.futures import ThreadPoolExecutor
import json

from flask import Flask, jsonify, stream_with_context
import pytest

from app.services import llm_selection as choices
from app.services.llm import LLMAPIError, LLMProvider, LLMService
from app.services.llm_cost import build_usage_display

ARK = {'provider': 'volcengine', 'model': 'deepseek-v4-1-flash-260910', 'reasoning_effort': 'high'}
GPT = {'provider': 'openai', 'model': 'gpt-5.4', 'reasoning_effort': 'medium'}


@pytest.fixture
def catalog(monkeypatch):
    monkeypatch.setattr(choices, 'model_catalog', lambda: {'items': [
        {**item, 'reasoning_options': choices.reasoning_options(item['provider'], item['model'])}
        for item in (ARK, GPT)
    ]})


@pytest.mark.parametrize('raw', [[], 'gpt-5.4', {**ARK, 'api_key': 'forbidden'}, {**ARK, 'base_url': 'http://other'},
                                      {**ARK, 'model': 'unconfigured'}, {**ARK, 'reasoning_effort': 'medium'}])
def test_reject_unknown_routes_and_unsupported_effort(catalog, raw):
    with pytest.raises(ValueError):
        choices.validate_selection(raw)


def test_catalog_contains_only_configured_routes_and_no_secrets(monkeypatch):
    monkeypatch.setattr(LLMService, 'is_configured', lambda self, p=None: p in {LLMProvider.VOLCENGINE, LLMProvider.LITELLM})
    monkeypatch.setattr(LLMService, 'get_default_model', lambda self, p=None: ARK['model'])
    monkeypatch.setattr('app.utils.config_loader.load_addon_config', lambda: {'volcengine': {'models': 'ep-second,ep-second'}})
    catalog = choices.model_catalog()
    assert [(i['provider'], i['model']) for i in catalog['items']] == [('volcengine', ARK['model']), ('volcengine', 'ep-second')]
    assert catalog['items'][1]['reasoning_options'] == ['default']
    assert all(set(item) == {'provider', 'model', 'reasoning_options'} for item in catalog['items'])


def test_request_validation_happens_before_work_and_survives_stream(catalog):
    app = Flask(__name__)
    reached = []

    @app.post('/call')
    @choices.agent_model_selection
    def call():
        reached.append(True)
        @stream_with_context
        def output():
            yield json.dumps(LLMService().selection)
        return app.response_class(output())

    with app.test_client() as client:
        response = client.post('/call', json={'llm_selection': {**ARK, 'reasoning_effort': 'medium'}})
        assert response.status_code == 400 and not reached
        response = client.post('/call', json={'llm_selection': ARK})
        assert json.loads(response.data) == ARK
        response = client.post('/call', json={})
        assert json.loads(response.data) == {}


def test_worker_scopes_are_isolated_and_reset_on_error(catalog):
    def worker(item):
        with choices.selection_scope(item):
            service = LLMService()
        assert not choices.current_selection()
        return service.provider.value, service.get_code_generation_model(), service.selection
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(worker, [ARK, GPT] * 10))
    assert all(result == (item['provider'], item['model'], item) for result, item in zip(results, [ARK, GPT] * 10))
    with pytest.raises(RuntimeError), choices.selection_scope(ARK):
        raise RuntimeError('test')
    assert choices.current_selection() == {}


@pytest.mark.parametrize('effort', ['none', 'low', 'high', 'max'])
@pytest.mark.parametrize('stream', [False, True])
def test_ark_sends_real_reasoning_payload_in_both_paths(monkeypatch, effort, stream):
    selection = {**ARK, 'reasoning_effort': effort}
    service = LLMService(selection=selection)
    sent = []

    class Response:
        status_code = 200
        headers = {}
        def json(self):
            return {'model': ARK['model'], 'choices': [{'message': {'content': 'ok'}, 'finish_reason': 'stop'}],
                    'usage': {'prompt_tokens': 3, 'completion_tokens': 2}}
        def iter_lines(self, **kwargs):
            yield b'data: {"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}'
            yield b''
            yield b'data: {"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2}}'
            yield b''
            yield b'data: [DONE]'
            yield b''
        def close(self):
            pass

    def post(url, **kwargs):
        sent.append(kwargs['json_payload'])
        return Response()
    monkeypatch.setattr(service, '_llm_post', post)
    monkeypatch.setattr(service, 'get_api_key', lambda *args: 'test')
    if stream:
        assert ''.join(service.stream_llm_api([], model='wrong-model')) == 'ok'
    else:
        assert service.call_llm_api([], model='wrong-model') == 'ok'
    assert sent[0]['model'] == ARK['model']
    assert sent[0]['thinking']['type'] == ('disabled' if effort == 'none' else 'enabled')
    assert sent[0].get('reasoning_effort') == (None if effort == 'none' else effort)
    usage = build_usage_display(provider=service.last_provider, model=service.last_model, usage=service.last_usage,
                                estimated_input_tokens=0, estimated_output_tokens=0)
    assert usage['reasoning_effort'] == effort


def test_openai_payload_and_no_silent_fallback(monkeypatch):
    payload = choices.apply_reasoning({'temperature': .3, 'max_tokens': 400}, 'openai', 'gpt-5.4', 'medium')
    assert payload == {'max_completion_tokens': 400, 'reasoning_effort': 'medium'}
    service = LLMService(selection=GPT)
    monkeypatch.setattr(service, 'get_api_key', lambda *args: 'test')
    seen = []
    def fail(*args, **kwargs):
        seen.append(args[1])
        raise LLMAPIError('denied', status_code=403)
    monkeypatch.setattr(service, '_call_openai_compatible', fail)
    monkeypatch.setattr(service, '_try_alternative_providers', lambda *args, **kwargs: pytest.fail('provider switched'))
    with pytest.raises(LLMAPIError):
        service.call_llm_api([], model='different')
    assert seen == ['gpt-5.4']


def test_fast_analysis_instances_do_not_share_model_or_usage(catalog):
    from app.services.fast_analysis import get_fast_analysis_service
    with choices.selection_scope(ARK):
        one = get_fast_analysis_service()
    with choices.selection_scope(GPT):
        two = get_fast_analysis_service()
    assert one is not two and one.llm_service is not two.llm_service
    assert one.llm_service.selection == ARK and two.llm_service.selection == GPT


def test_research_configuration_pins_model(catalog):
    from app.services.research_workflow import normalize_research_config
    assert normalize_research_config({'llm_selection': ARK})['llm_selection'] == ARK
    with pytest.raises(ValueError):
        normalize_research_config({'llm_selection': {**ARK, 'model': 'removed'}})
