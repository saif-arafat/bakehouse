"""Helpers for the gold dimensions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

    from lakeora_core.table_schema import TableSchema


def unknown_member(spark: SparkSession, schema: TableSchema, **values) -> DataFrame:
    """One row typed like the schema: the given column values, null everywhere else.

    Union it onto a dimension so facts with no matching member can point at it, e.g.
    unknown_member(spark, dim_supplier, supplier_key=-1, supplier_name="Unknown").
    """
    unknown = set(values) - set(schema.column_names)
    if unknown:
        raise ValueError(f"{schema.full_name} has no column(s) {sorted(unknown)}")
    row = tuple(values.get(c) for c in schema.column_names)
    return spark.createDataFrame([row], schema.struct_type)


def lookup_as_of(
    facts: DataFrame,
    dim: DataFrame,
    on: tuple[str, str],
    at: Column,
    keys: dict[str, str],
    valid_from: str,
    valid_to: str,
    unknown: int = -1,
) -> DataFrame:
    """Add the keys of the dimension row whose validity window holds `at`, for the fact's id.

    on: (fact id column, dimension id column). The window is [valid_from, valid_to), with a null
    valid_to meaning open-ended. keys maps dimension key column -> output column; a fact with no
    matching row gets `unknown` (the dimension's unknown member).
    """
    import pyspark.sql.functions as F

    fact_id, dim_id = on
    d = dim.select(
        F.col(dim_id).alias("_dim_id"),
        F.col(valid_from).alias("_from"),
        F.col(valid_to).alias("_to"),
        *[F.col(k).alias(f"_key_{k}") for k in keys],
    )
    matched = (facts[fact_id] == d["_dim_id"]) & (at >= d["_from"]) & (d["_to"].isNull() | (at < d["_to"]))
    out = facts.join(d, matched, "left")
    for k, name in keys.items():
        out = out.withColumn(name, F.coalesce(F.col(f"_key_{k}"), F.lit(unknown)))
    return out.drop("_dim_id", "_from", "_to", *[f"_key_{k}" for k in keys])
