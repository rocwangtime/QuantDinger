import pytest
from app.services.indicator_code_quality import _param_read_names, _has_strategy_annotations


@pytest.mark.timeout(2)
def test_long_whitespace_does_not_cause_quadratic_backtracking():
    assert _param_read_names('params' + ' ' * 200000 + 'x') == set()
    assert not _has_strategy_annotations(' ' * 200000 + 'x')
    assert not _has_strategy_annotations('\n' * 200000 + 'x')


def test_valid_param_reads_and_strategy_annotations_are_preserved():
    assert _param_read_names('n = params . get ("window", 10)') == {'window'}
    assert _has_strategy_annotations('  # @strategy direction long')
    assert _has_strategy_annotations('  # timeframe: 1D')
    assert _has_strategy_annotations('  timeframe: 1D')
