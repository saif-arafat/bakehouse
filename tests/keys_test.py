"""Tests for lakeora_core.keys.

The expected hashes are built with hashlib the way dbt_utils.generate_surrogate_key
builds them: md5 of the string parts joined by "-", nulls as a placeholder.
"""

import datetime
import hashlib

import pytest

from lakeora_core.keys import date_key, entity_key, surrogate_key, version_key

NULL = "_dbt_utils_surrogate_key_null_"


def _md5(*parts: str) -> str:
    return hashlib.md5("-".join(parts).encode()).hexdigest()


def _key(spark, rows, schema, *columns):
    return [r.sk for r in spark.createDataFrame(rows, schema).select(surrogate_key(*columns).alias("sk")).collect()]


def test_matches_dbt_for_id_and_date(spark):
    keys = _key(spark, [(1, datetime.date(2026, 10, 19))], "id bigint, d date", "id", "d")
    assert keys == [_md5("1", "2026-10-19")]


def test_single_column(spark):
    assert _key(spark, [("abc",)], "s string", "s") == [_md5("abc")]


def test_null_uses_placeholder(spark):
    keys = _key(spark, [(1, None)], "id bigint, d date", "id", "d")
    assert keys == [_md5("1", NULL)]


def test_null_differs_from_missing_part(spark):
    """concat_ws would drop a bare null; the placeholder keeps (1, null) apart from (1)."""
    keys = _key(spark, [(1, None)], "id bigint, d date", "id", "d")
    assert keys != [_md5("1")]


def test_column_order_matters(spark):
    rows, schema = [(1, 2)], "a bigint, b bigint"
    assert _key(spark, rows, schema, "a", "b") != _key(spark, rows, schema, "b", "a")


def test_needs_a_column():
    with pytest.raises(ValueError, match="at least one column"):
        surrogate_key()


# ---- dimension keys -------------------------------------------------------


def _one(spark, rows, schema, expr):
    return [r.k for r in spark.createDataFrame(rows, schema).select(expr.alias("k")).collect()]


def test_date_key(spark):
    rows = [(datetime.date(2026, 10, 19),), (datetime.date(2000, 1, 1),), (None,)]
    assert _one(spark, rows, "d date", date_key("d")) == [20261019, 20000101, None]


def test_entity_key_packs_id_and_created_date(spark):
    keys = _one(spark, [(2000123, datetime.date(2026, 10, 19))], "id bigint, d date", entity_key("id", "d"))
    assert keys == [200012320261019]


def test_entity_key_sorts_like_the_natural_key(spark):
    """Order-preserving: sorting by the key is sorting by (id, created_date)."""
    natural = [(1, datetime.date(2026, 1, 2)), (1, datetime.date(2026, 10, 19)), (2, datetime.date(1999, 12, 31))]
    keys = _one(spark, natural, "id bigint, d date", entity_key("id", "d"))
    assert keys == sorted(keys) and len(set(keys)) == 3


def test_version_key(spark):
    keys = _one(spark, [(200012320261019, 1), (200012320261019, 12)], "e bigint, v int", version_key("e", "v"))
    assert keys == [200012320261019001, 200012320261019012]


@pytest.mark.parametrize("version", [0, 1000])
def test_version_key_rejects_out_of_range(spark, version):
    with pytest.raises(Exception, match="version in 1..999"):
        _one(spark, [(1, version)], "e bigint, v int", version_key("e", "v"))


def test_entity_key_overflow_fails(spark):
    """ANSI mode turns a too-large id into an error instead of a wrapped key."""
    with pytest.raises(Exception, match="(?i)overflow"):
        _one(spark, [(10**12, datetime.date(2026, 1, 1))], "id bigint, d date", entity_key("id", "d"))
