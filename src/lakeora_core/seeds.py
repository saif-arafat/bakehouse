"""Read the CSV seeds under lakeora/seeds (outside the pipeline glob) the way dbt seeds them."""

from __future__ import annotations

import csv
from importlib.resources import files
from typing import TYPE_CHECKING

import pyspark.sql.types as T

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from lakeora_core.table_schema import TableSchema


def read_seed(spark: SparkSession, schema: TableSchema, filename: str) -> DataFrame:
    """The seed's rows conformed to the schema; empty cells are null, like dbt."""
    with (files("lakeora.seeds") / filename).open(newline="") as f:
        rows = [tuple(r[c] or None for c in schema.column_names) for r in csv.DictReader(f)]
    raw = T.StructType([T.StructField(c, T.StringType()) for c in schema.column_names])
    return schema.conform(spark.createDataFrame(rows, raw))
