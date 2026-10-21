"""Tests for lakeora_core.dimensions and lakeora_core.seeds."""

import datetime as dt

import pytest
import pyspark.sql.functions as F
import pyspark.sql.types as T

from lakeora_core.dimensions import lookup_as_of, unknown_member
from lakeora_core.seeds import read_seed
from lakeora_core.table_schema import Column, TableSchema
from lakeora.bronze.seeds.models import countries, us_states

dim = TableSchema(
    table="dim_thing",
    columns=[
        Column("thing_key", T.LongType()),
        Column("thing_name", T.StringType()),
        Column("thing_size", T.IntegerType()),
    ],
    primary_key="thing_key",
)


def test_unknown_member_fills_given_columns_and_nulls_the_rest(spark):
    row = unknown_member(spark, dim, thing_key=-1, thing_name="Unknown").collect()
    assert [r.asDict() for r in row] == [{"thing_key": -1, "thing_name": "Unknown", "thing_size": None}]


def test_unknown_member_is_typed_like_the_schema(spark):
    assert unknown_member(spark, dim, thing_key=-1).schema == dim.struct_type


def test_unknown_member_rejects_unknown_columns(spark):
    with pytest.raises(ValueError, match="no column"):
        unknown_member(spark, dim, nope=1)


def test_read_seed_us_states(spark):
    df = read_seed(spark, us_states, "us_states.csv")
    assert df.count() == 98
    assert dict(df.dtypes)["is_contiguous_us"] == "boolean"
    # empty cells come through as null, like dbt seeds
    assert df.filter("census_region IS NULL").count() > 0


def test_read_seed_countries(spark):
    df = read_seed(spark, countries, "countries.csv")
    assert df.columns == countries.column_names
    assert {r.country_name for r in df.filter("country_from_erp IN ('US', 'USA')").collect()} == {"United States"}


# ---- lookup_as_of ---------------------------------------------------------

D = dt.date


def _lookup(spark, facts):
    """Customer 1 has two versions split on 2026-03-01; customer 2 a single open-ended one."""
    dim = spark.createDataFrame(
        [(-1, -1, -1, D(1900, 1, 1), None), (11, 10, 1, D(1900, 1, 1), D(2026, 3, 1)),
         (12, 10, 1, D(2026, 3, 1), None), (21, 20, 2, D(1900, 1, 1), None)],
        "key bigint, durable bigint, id bigint, valid_from date, valid_to date",
    )
    f = spark.createDataFrame(facts, "sale int, customer_id bigint, at date")
    out = lookup_as_of(f, dim, ("customer_id", "id"), F.col("at"), {"key": "customer_key", "durable": "customer_durable_key"},
                       "valid_from", "valid_to")
    return {r.sale: (r.customer_key, r.customer_durable_key) for r in out.collect()}, out.columns


def test_lookup_picks_the_version_valid_at_the_date(spark):
    got, _ = _lookup(spark, [(1, 1, D(2026, 2, 28)), (2, 1, D(2026, 3, 1)), (3, 2, D(2030, 1, 1))])
    # valid_to is exclusive: the change day belongs to the new version
    assert got == {1: (11, 10), 2: (12, 10), 3: (21, 20)}


def test_lookup_unmatched_gets_the_unknown_member(spark):
    got, _ = _lookup(spark, [(1, 99, D(2026, 1, 1)), (2, None, D(2026, 1, 1))])
    assert got == {1: (-1, -1), 2: (-1, -1)}


def test_lookup_keeps_one_row_per_fact_and_only_adds_the_keys(spark):
    got, columns = _lookup(spark, [(1, 1, D(2026, 2, 1)), (2, 1, D(2026, 4, 1))])
    assert len(got) == 2
    assert columns == ["sale", "customer_id", "at", "customer_key", "customer_durable_key"]
