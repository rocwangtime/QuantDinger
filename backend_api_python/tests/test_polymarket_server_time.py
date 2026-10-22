import json
from types import SimpleNamespace
import pytest
from app.services.polymarket.client import PublicClient, CLOB
from app.services.polymarket.paper_account import Latency
from app.services.polymarket.account_model import DEFAULTS


def client(body, content_type):
    requests=[]
    response=SimpleNamespace(content=body,headers={'Content-Type':content_type},status_code=200,
        raise_for_status=lambda:None,json=lambda:json.loads(body))
    def request(*a,**kw):requests.append((a,kw));return response
    return PublicClient(SimpleNamespace(request=request,close=lambda:None)),requests


@pytest.mark.parametrize('body,kind',[(b'1792679463','text/plain; charset=UTF-8'),(b'1792679463','application/json'),(b'1792679463000','text/plain')])
def test_numeric_time_is_accepted_and_probe_measures(body,kind):
    c,requests=client(body,kind)
    assert c.server_time()==int(body)
    latency=Latency(DEFAULTS);latency.measure(c,1000)
    assert latency.status(1000)['fallback'] is False
    assert requests[0]==(('GET',CLOB+'/time'),dict(timeout=(3,8),allow_redirects=False))


@pytest.mark.parametrize('body,kind',[(b'<html>blocked</html>','text/html'),(b'<html>blocked</html>','text/plain'),(b'1.792679463e9','application/json'),(b'"1792679463"','application/json'),(b'123','text/plain'),(b'1'*65,'text/plain')])
def test_invalid_time_cannot_supply_latency(body,kind):
    c,_=client(body,kind)
    with pytest.raises(ValueError,match='polymarket.invalidApiResponse'):c.server_time()
    latency=Latency(DEFAULTS);latency.measure(c,1000)
    assert latency.status(1000)['fallback'] is True


def test_other_endpoints_remain_json_only():
    c,_=client(b'1792679463','text/plain')
    with pytest.raises(ValueError,match='polymarket.invalidApiResponse'):
        c._json('GET',CLOB+'/book')
