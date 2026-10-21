"""
Key helpers shared by the pipeline tables.

    from lakeora_core.keys import surrogate_key, entity_key

    df.select(surrogate_key("franchiseid", "created_date").alias("franchise_sk"))  # silver
    df.select(entity_key("franchise_id", "franchise_created_date").alias("franchise_key"))  # gold
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as F
import pyspark.sql.types as T

if TYPE_CHECKING:
    from pyspark.sql import Column

# placeholder dbt_utils uses for nulls, so a null part still changes the hash
_NULL_PLACEHOLDER = "_dbt_utils_surrogate_key_null_"


def surrogate_key(*columns: str) -> Column:
    """md5 hash of the columns, the same as dbt_utils.generate_surrogate_key.

    Each column is cast to string, nulls become a placeholder, and the parts are
    joined with "-" before hashing, so keys match the ones the dbt build produced.
    """
    if not columns:
        raise ValueError("surrogate_key needs at least one column")
    return F.md5(
        F.concat_ws(
            "-",
            *[F.coalesce(F.col(c).cast(T.StringType()), F.lit(_NULL_PLACEHOLDER)) for c in columns],
        )
    )


# ---- dimension keys -------------------------------------------------------
# Gold dimensions follow https://docs.databricks.com/aws/en/ldp/best-practices/dimensional-modeling:
# no hash keys (they're random, which defeats liquid clustering); a surrogate, where an ERP id gets
# reused, is derived deterministically from the natural key and sorts the same way it does, so it
# survives a full refresh and clusters well. Arithmetic overflow raises under ANSI mode.

_DATE_DIGITS = 100_000_000  # room for a yyyymmdd date
_VERSION_DIGITS = 1_000  # room for versions 1..999


def _col(c: str | Column) -> Column:
    return F.col(c) if isinstance(c, str) else c


def date_key(date: str | Column) -> Column:
    """The date as a yyyymmdd int, e.g. 2026-10-19 -> 20261019. Null stays null."""
    d = _col(date)
    return (F.year(d) * 10_000 + F.month(d) * 100 + F.dayofmonth(d)).cast(T.IntegerType())


def entity_key(id: str | Column, created_date: str | Column) -> Column:
    """bigint key for an ERP id that can be reused: id followed by the yyyymmdd it was created.

    Orders by (id, created_date), e.g. (2000123, 2026-10-19) -> 200012320261019.
    """
    return _col(id).cast(T.LongType()) * _DATE_DIGITS + date_key(created_date)


def version_key(entity: str | Column, version: str | Column) -> Column:
    """bigint key for version 1..999 of an entity_key; fails the query on any other version."""
    v = _col(version)
    return F.when(
        v.between(1, _VERSION_DIGITS - 1),
        _col(entity) * _VERSION_DIGITS + v.cast(T.LongType()),
    ).otherwise(F.raise_error(F.concat(F.lit("version_key needs a version in 1..999, got "), v.cast("string"))))
