"""Hive pipeline checks that need no Hadoop: the HiveQL text and the availability probe.

The real behaviour (parsing, queries, rounding) is covered by
``pytest tests/test_contract.py --engine hive`` when HiveServer2 is running.
"""

import re
from pathlib import Path

import pytest

from pipelines import get_engine
from pipelines.hive import hiveql

SPEC = Path(__file__).resolve().parent.parent / "spec" / "parsing_spec.md"


def _spec_regexes() -> tuple[str, str]:
    """The line regex (spec 2.1) and request regex (spec 4), read from the frozen spec."""
    blocks = re.findall(r"```regex\n(.*?)\n```", SPEC.read_text(encoding="utf-8"), re.DOTALL)
    assert len(blocks) == 2, "expected the line regex and the request regex in the spec"
    return blocks[0], blocks[1]


def _hive_unescape(literal: str) -> str:
    """What HiveServer2 does to a quoted literal: ``\\\\`` -> ``\\`` and ``\\'`` -> ``'``."""
    assert literal[0] == literal[-1] == "'"
    return re.sub(r"\\(.)", r"\1", literal[1:-1])


def test_regexes_are_the_ones_in_the_frozen_spec() -> None:
    line_re, request_re = _spec_regexes()
    assert hiveql.LINE_REGEX == line_re
    assert hiveql.REQUEST_REGEX == request_re


def test_literal_survives_hive_unescaping() -> None:
    for text in (
        hiveql.LINE_REGEX,
        hiveql.REQUEST_REGEX,
        hiveql.LINE_PREFIX_REGEX,
        r"a\Sb\\c",
        "it's",
        "\\'",
    ):
        assert _hive_unescape(hiveql.literal(text)) == text


def test_regexes_in_statements_are_the_spec_regexes_plus_unix_lines_flag() -> None:
    sql = hiveql.parse_batch(1, 1)
    assert hiveql.literal(hiveql.JAVA_FLAGS + hiveql.LINE_REGEX) in sql
    assert hiveql.literal(hiveql.JAVA_FLAGS + hiveql.REQUEST_REGEX) in sql
    # all eleven groups of the line regex are available to Python-free parsing
    for group in (1, 2, 3, 4, 5, 6, 7, 9, 10, 11):
        assert f"regexp_extract(line, {hiveql._LINE_RE}, {group})" in sql


def test_parse_statement_keeps_every_line_and_checks_the_month() -> None:
    sql = hiveql.parse_batch(7, 3)
    assert "PARTITION (run_id = 7, batch_id = 3)" in sql
    assert "WHERE ok" in sql and "WHERE NOT ok" in sql  # parsed and malformed, nothing dropped
    for month in hiveql.MONTHS:
        assert f"'{month}'" in sql
    assert "mon IN (" in sql  # case-sensitive month check is part of the malformed rule


def test_error_rate_uses_integer_half_up_arithmetic() -> None:
    sql = hiveql.q3(1)
    assert "* 20000 +" in sql and "DIV (2 *" in sql and "/ 10000.0" in sql
    assert "ROUND(" not in sql.upper()
    assert "BETWEEN 400 AND 599" in sql


def test_q2_orders_by_count_then_path_and_limits_20() -> None:
    sql = hiveql.q2(1)
    assert "ORDER BY request_count DESC, resource_path ASC" in sql and sql.endswith("LIMIT 20")
    assert "COUNT(DISTINCT host)" in sql


def test_statements_are_scoped_to_the_run() -> None:
    for sql in (hiveql.q1(42), hiveql.q2(42), hiveql.q3(42)):
        assert "run_id = 42" in sql
    assert all("run_id = 42" in s for s in hiveql.drop_run_partitions(42))


def test_registered_and_unavailable_without_hiveserver(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIVE_HOST", "127.0.0.1")
    monkeypatch.setenv("HIVE_PORT", "1")  # nothing listens here
    engine = get_engine("hive")
    assert engine.name == "hive"
    assert engine.is_available() is False  # type: ignore[attr-defined]
