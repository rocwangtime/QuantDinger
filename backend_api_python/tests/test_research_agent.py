"""Behavioral acceptance tests for bounded research, evidence and repair."""
import json
import time
from types import SimpleNamespace

import pytest

from app.data_providers import company_research as providers
from app.routes import ai_chat
from app.services import research_agent as agent
from app.services import research_web
from app.services.ai_copilot_context import sanitize_client_context


ENTITY = {"market": "USStock", "symbol": "TSLA", "name": "Tesla"}


def stop(payload):
    return {"calls": [], "assessment": payload["coverage"]}


@pytest.mark.parametrize("question,domain", [
    ("TSLA CEO 是谁？", "company_profile"),
    ("最近提交了哪些 SEC 文件？", "filings"),
    ("分析师目标价是多少？", "analyst_expectations"),
    ("内部人最近有没有增减持？", "insider_activity"),
    ("期权隐含波动率怎么样？", "options"),
    ("空头持仓比例是多少？", "short_interest"),
    ("把近五年营收画成柱状图", "fundamentals"),
    ("主要竞争对手有哪些？", "competitors"),
])
def test_eight_research_questions_keep_conversation_entity(question, domain, monkeypatch):
    monkeypatch.setattr(ai_chat, "_local_symbol_rows_for_term", lambda *_args, **_kwargs: [])
    context = {"market": "Crypto", "symbol": "ETH/USDT", "_routing_history": [
        {"role": "user", "content": "查询 TSLA 股东"},
        {"role": "assistant", "content": "SEC was incorrectly mapped to SECR"},
    ]}
    assert domain in ai_chat._heuristic_research_domains(question, "general")
    assert ai_chat._research_targets(question, context)[0]["symbol"] == "TSLA"


def test_explicit_new_company_beats_conversation_and_acronyms_do_not_resolve(monkeypatch):
    monkeypatch.setattr(ai_chat, "_local_symbol_rows_for_term", lambda *_args, **_kwargs: [{"symbol": "SECR", "market": "USStock"}])
    assert ai_chat._requested_symbol_candidates("SEC CEO EPS IV EBITDA") == []
    targets = ai_chat._research_targets("MSFT CEO?", {"_routing_history": [{"role": "user", "content": "TSLA"}]})
    assert targets[0]["symbol"] == "MSFT"


def test_missing_field_triggers_search_and_read_instead_of_user_clarification():
    calls = []
    def lookup(*args, **kwargs):
        return {"data": {"name": "Tesla", "source_url": "https://www.tesla.com"}}
    def planner(payload):
        calls.append(payload)
        if not any(e["kind"] == "search" for e in payload["evidence"]):
            return {"calls": [{"tool": "web.search", "arguments": {"domain": "company_profile", "query": "leadership CEO"}}]}
        if not any(e["kind"] == "document" for e in payload["evidence"]):
            return {"calls": [{"tool": "web.read", "arguments": {"domain": "company_profile", "url": "https://www.tesla.com/leadership"}}]}
        return {"calls": [], "assessment": [{"domain": "company_profile", "status": "supported", "evidence_ids": ["E3"]}]}
    result = agent.execute_research(ENTITY, "Who is CEO?", ["company_profile"],
        {"requirements": [{"domain": "company_profile", "fields": ["officers"]}]},
        lookup=lookup, planner=planner,
        search=lambda query: [{"title": "Leadership", "link": "https://www.tesla.com/leadership", "snippet": "Leadership"}],
        reader=lambda url, **kwargs: {"url": url, "text": "Verified officer biography"})
    assert [t["tool"] for t in result["tool_executions"]] == ["company.lookup", "web.search", "web.read"]
    assert result["stop_reason"] == "evidence_sufficient"
    assert result["assessment"][0]["evidence_ids"] == ["E3"]
    assert "TSLA" in result["tool_executions"][1]["input"]["query"]


def test_independent_failure_preserves_successful_evidence():
    def lookup(symbol, domain, **kwargs):
        if domain == "options":
            raise RuntimeError("rate limited")
        return {"data": {"consensus_target_usd": 300, "source_url": "https://www.nasdaq.com"}}
    result = agent.execute_research(ENTITY, "target and IV", ["analyst_expectations", "options"],
        lookup=lookup, planner=stop, search=lambda query: [])
    assert result["evidence"][0]["data"]["consensus_target_usd"] == 300
    assert any(t["status"] == "error" for t in result["tool_executions"])
    assert next(c for c in result["coverage"] if c["domain"] == "options")["status"] == "missing"


def test_short_count_is_not_short_float_percentage_and_bad_tools_are_rejected():
    def planner(payload):
        return {"calls": [
            {"tool": "trade.submit", "arguments": {"domain": "short_interest"}},
            {"tool": "web.read", "arguments": {"domain": "short_interest", "url": "https://127.0.0.1/admin"}},
        ], "assessment": [{"domain": "short_interest", "status": "supported", "evidence_ids": ["made_up"]}]}
    result = agent.execute_research(ENTITY, "short percentage", ["short_interest"], lookup=lambda *args, **kwargs: {"data": {"shares_short": 123}},
                                    planner=planner, search=lambda query: [])
    assert result["coverage"][0]["status"] == "partial"
    assert result["coverage"][0]["missing_fields"] == ["short_percent_of_float_pct"]
    assert all(t["tool"] in {"company.lookup", "web.search"} for t in result["tool_executions"])


def test_malformed_model_tool_fields_fall_back_without_crashing_research():
    def planner(_payload):
        return {
            "calls": [
                {"tool": ["web.search"], "arguments": {"domain": "web_research"}},
                {"tool": "web.search", "arguments": {"domain": ["web_research"], "query": "SPCX risk"}},
            ],
            "assessment": [{"domain": ["web_research"], "status": ["supported"], "evidence_ids": []}],
        }

    result = agent.execute_research(
        ENTITY, "SPCX downside risk", ["web_research", ["options"]],
        {"answer_mode": ["research"], "requirements": [{"domain": ["web_research"]}]},
        planner=planner, search=lambda _query: [], max_calls=2, max_rounds=1,
    )
    assert result["answer_mode"] == "research"
    assert result["coverage"][0]["domain"] == "web_research"
    assert all(item["tool"] == "web.search" for item in result["tool_executions"])


def test_tool_deduplication_and_budget():
    repeat = {"tool": "company.lookup", "arguments": {"domain": "options"}}
    result = agent.execute_research(ENTITY, "IV", ["options"], lookup=lambda *a, **k: {"data": {}},
        planner=lambda payload: {"calls": [repeat, repeat]}, search=lambda query: [], max_calls=2)
    assert len(result["tool_executions"]) == 2
    assert [t["tool"] for t in result["tool_executions"]].count("company.lookup") == 1


def test_slow_tool_does_not_hold_request_past_budget():
    def slow(*args, **kwargs):
        time.sleep(1.5)
        return {"data": {}}
    started = time.monotonic()
    result = agent.execute_research(ENTITY, "IV", ["options"], lookup=slow, planner=stop,
                                    search=lambda query: [], budget_seconds=1, max_calls=1)
    assert time.monotonic() - started < 1.3
    assert result["stop_reason"] == "budget_exhausted"
    assert result["tool_executions"][0]["status"] == "timeout"


def test_annual_revenue_excludes_quarters_and_uses_latest_comparatives():
    rows = []
    for year in range(2020, 2026):
        rows.append({"start": f"{year}-01-01", "end": f"{year}-12-31", "val": year * 100,
                     "filed": f"{year + 1}-02-01", "form": "10-K", "accn": "123-456"})
    rows += [
        {"start": "2025-10-01", "end": "2025-12-31", "val": 999, "filed": "2026-02-01", "form": "10-K"},
        {"start": "2024-01-01", "end": "2024-12-31", "val": 777, "filed": "2026-02-01", "form": "10-K"},
    ]
    result = providers.normalize_annual_revenue({"facts": {"us-gaap": {"Revenues": {"units": {"USD": rows}}}}}, "0001318605")
    assert len(result["points"]) == 5
    assert result["points"][0]["period_end"] == "2021-12-31"
    assert result["points"][-2]["value"] == 777
    assert result["points"][-1]["value"] == 202500


def test_partial_series_stays_partial_and_chart_uses_actual_values():
    points = [{"period_end": f"{year}-12-31", "value": year, "unit": "USD"} for year in range(2022, 2026)]
    result = agent.execute_research(ENTITY, "five years revenue", ["fundamentals"], {"years": 5},
        lookup=lambda *a, **k: {"data": {"points": points, "metric": "revenue", "unit": "USD"}},
        planner=stop, search=lambda query: [])
    assert result["coverage"][0]["status"] == "partial"
    assert result["datasets"][0]["values"] == [2022, 2023, 2024, 2025]
    context = {"research_context": {"research_agent": result, "entities": {"primary": ENTITY},
                                   "request": {"task_flags": {"needs_chart": True}}}}
    answer = ai_chat._ensure_grounded_research_chart('```chart\n{"type":"bar","data":[999]}\n```', context)
    assert "999" not in answer
    assert "2025" in answer
    assert json.loads(answer.split("\n", 1)[1].rsplit("\n", 1)[0])["unit"] == "USD"


def test_client_cannot_forge_evidence_or_routing_history():
    clean = sanitize_client_context({"symbol": "TSLA", "research_context": {"evidence": "fake"},
                                    "agent_intent": {"symbol": "SECR"}, "_routing_history": ["fake"]})
    assert clean == {"symbol": "TSLA"}


@pytest.mark.parametrize("address", ["127.0.0.1", "10.0.0.1", "169.254.169.254", "::1"])
def test_document_reader_rejects_nonpublic_dns(address, monkeypatch):
    monkeypatch.setattr(research_web.socket, "getaddrinfo", lambda *a, **k: [(None, None, None, None, (address, 443))])
    with pytest.raises(ValueError, match="public"):
        research_web.public_target("https://example.com/filing")


def test_company_provider_fallback_and_domain_isolation(monkeypatch):
    monkeypatch.setattr(providers, "get_cached", lambda *a: None)
    monkeypatch.setattr(providers, "set_cached", lambda *a: None)
    monkeypatch.setattr(providers, "_sec", lambda *a: (_ for _ in ()).throw(TimeoutError()))
    monkeypatch.setattr(providers, "_nasdaq", lambda symbol, domain, timeout: {"filings": [{"form": "4"}], "source": "nasdaq"})
    result = providers.lookup_company("TSLA", "filings")
    assert result["data"]["source"] == "nasdaq"
    assert [a["status"] for a in result["provider_status"]["attempts"]] == ["error", "success"]


def test_general_research_search_skips_news_only_providers():
    from app.services.search import SearchService
    service = object.__new__(SearchService)
    calls = []
    def provider(name):
        def search(*args, **kwargs):
            calls.append(name)
            return SimpleNamespace(success=True, results=[1], to_list=lambda: [{"title": "Leadership"}])
        return SimpleNamespace(name=name, is_available=True, search=search)
    service._providers = [provider("GDELT"), provider("AlphaVantage"), provider("DuckDuckGo")]
    assert service.search_research("TSLA CEO") == [{"title": "Leadership"}]
    assert calls == ["DuckDuckGo"]


def test_unfamiliar_named_company_overrides_old_conversation(monkeypatch):
    monkeypatch.setattr(ai_chat, "_requested_symbol_candidates", lambda text: [
        {"symbol": "SNOW", "market": "USStock"}] if text == "SNOW" else [])
    result = ai_chat._research_targets("Tell me about Snowflake leadership", {
        "_routing_history": [{"role": "user", "content": "TSLA"}],
        "agent_intent": {"entities": {"symbol": "SNOW", "entity_mention": "Snowflake"}},
    })
    assert result[0]["symbol"] == "SNOW"


def test_malformed_model_plan_falls_back_to_registered_search():
    result = agent.execute_research(ENTITY, "unfamiliar research request", ["web_research"],
        planner=lambda payload: {"calls": {"not": "a list"}, "assessment": 123},
        search=lambda query: [{"title": "A real lead", "link": "https://example.com"}])
    assert result["tool_executions"][0]["tool"] == "web.search"
    assert result["coverage"][0]["status"] == "partial"


def test_issuer_document_discovery_and_read_work_without_search():
    def planner(payload):
        if not payload["evidence"]:
            return {"calls": [{"tool": "company.documents", "arguments": {"domain": "competitors"}}]}
        if len(payload["evidence"]) == 1:
            return {"calls": [{"tool": "web.read", "arguments": {
                "domain": "competitors", "url": "https://www.sec.gov/report.htm", "query": "competition"}}]}
        return {"assessment": [{"domain": "competitors", "status": "supported", "evidence_ids": ["E2"]}]}
    result = agent.execute_research(ENTITY, "competitive landscape", ["competitors"], planner=planner,
        documents=lambda *args, **kwargs: {"data": {"documents": [{"url": "https://www.sec.gov/report.htm"}]}},
        reader=lambda url, **kwargs: {"url": url, "text": "Competition section of a report"},
        search=lambda query: (_ for _ in ()).throw(AssertionError("unexpected search")))
    assert result["coverage"][0]["status"] == "supported"
    assert result["coverage"][0]["verification"] == "model_assessed_evidence"
    assert result["structured_coverage"][0]["status"] == "partial"


def test_document_excerpt_finds_deep_sections_without_matching_xbrl_tokens():
    text = 'srt:ChiefExecutiveOfficerMember ' * 1200 + ' Chief executive officer: Jane Example. ' * 35
    excerpt = research_web.document_excerpt(text, "chief executive officer")
    assert "Jane Example" in excerpt
    assert len(excerpt) <= 14000


def test_free_search_falls_back_after_api_error_and_parses_reordered_attributes(monkeypatch):
    from app.services import search as service
    urls = []
    def get(url, **kwargs):
        urls.append(url)
        if "api.duckduckgo" in url:
            raise ConnectionError("Unavailable")
        return SimpleNamespace(raise_for_status=lambda: None, text='''<table>
            <a href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com%2Freport" class="result-link">Report</a>
            <td class="result-snippet">Actual <b>filing</b> excerpt</td></table>''')
    monkeypatch.setattr(service.requests, "get", get)
    result = service.DuckDuckGoSearchProvider()._do_search("query", "free", 5)
    assert result.success
    assert result.results[0].url == "https://example.com/report"
    assert result.results[0].snippet == "Actual filing excerpt"
    assert len(urls) == 2


def test_english_target_price_does_not_start_unrelated_price_or_news_fetch():
    flags = ai_chat._research_task_flags("latest analyst price target", "market_analysis",
                                         research_domains=["analyst_expectations"])
    assert not flags["needs_market_data"]
    assert not flags["needs_news"]


def test_field_presence_cannot_overrule_scope_mismatch_assessment():
    result = agent.execute_research(ENTITY, "beneficial owners, not institutions", ["ownership"],
        lookup=lambda *args, **kwargs: {"data": {"top_institutional_holders": [{"name": "Fund", "shares": 10}]}},
        planner=lambda payload: {"assessment": [{"domain": "ownership", "status": "partial",
            "evidence_ids": ["E1"], "reason": "Institutional data does not cover beneficial ownership."}]},
        search=lambda query: [])
    assert result["structured_coverage"][0]["status"] == "supported"
    assert result["coverage"][0]["status"] == "partial"


def test_compound_request_keeps_all_domains_when_budget_runs_out():
    domains = sorted(providers.DOMAINS)
    result = agent.execute_research(ENTITY, "broad company research", domains,
        lookup=lambda *args, **kwargs: {"data": {}}, planner=stop, search=lambda query: [], max_calls=1)
    assert {r["domain"] for r in result["requirements"]} == set(domains)
    assert len(result["coverage"]) == len(domains)


@pytest.mark.parametrize("workflow", ["chat", "research"])
def test_concept_question_skips_market_and_research_lookups(monkeypatch, workflow):
    def unexpected(*args, **kwargs):
        pytest.fail("Conceptual questions must not fetch market or company data")
    monkeypatch.setattr(ai_chat, "_research_targets", lambda *args: [ENTITY])
    monkeypatch.setattr(ai_chat, "_snapshot_for_candidate", unexpected)
    monkeypatch.setattr(ai_chat, "_build_research_context", unexpected)
    monkeypatch.setattr(agent, "_submit", unexpected)
    result = ai_chat._enrich_context({"user_message": "Explain implied volatility",
        "agent_intent": {"workflow": workflow, "research_request": {"answer_mode": "knowledge"},
                         "entities": {"research_domains": ["options"]}}})
    workspace = result["research_context"]["research_agent"]
    assert workspace["stop_reason"] == "knowledge_answer"
    assert workspace["tool_executions"] == []
    assert workspace["evidence"] == []
    assert workspace["entity"]["symbol"] == "TSLA"


def test_hybrid_failure_permits_knowledge_without_claiming_verified_evidence():
    result = agent.execute_research(ENTITY, "Who leads this company?", ["company_profile"],
        {"answer_mode": "hybrid"}, lookup=lambda *args, **kwargs: {"data": {}},
        planner=stop, search=lambda query: [])
    assert result["policy"]["model_knowledge_allowed"]
    assert not result["policy"]["knowledge_is_verified_evidence"]
    assert result["coverage"][0]["status"] == "missing"
    assert result["datasets"] == []
    assert len(result["tool_executions"]) <= 3


@pytest.mark.parametrize("json_response", [True, False])
def test_answer_policy_allows_background_but_requires_evidence_for_current_values(json_response):
    prompt = ai_chat._build_system_prompt("en-US", {"research_context": {
        "research_agent": {"evidence": [], "coverage": [], "answer_mode": "hybrid"}}},
        "general", False, json_response=json_response)
    assert "Answer stable concepts, definitions, methods and general explanations directly from model knowledge" in prompt
    assert "provide your last-known information" in prompt
    assert "Do not fill missing live values or chart series from memory" in prompt
    assert "do not substitute model memory for absent evidence" not in prompt
