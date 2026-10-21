"""Shared logic for the conformed dimensions; no pipeline code, so it's safe to import."""

import pyspark.sql.functions as F

from lakeora.bronze.seeds.models import countries


def with_country_name(spark, df, erp_country: str):
    """Add country_name: the countries seed's standard name for the ERP spelling, else the spelling itself."""
    seed = spark.table(countries.full_name).select(
        F.col("country_from_erp").alias("_erp"),
        F.col("country_name").alias("_standard"),
    )
    return (
        df.join(seed, df[erp_country] == seed["_erp"], "left")
        .withColumn("country_name", F.coalesce("_standard", erp_country))
        .drop("_erp", "_standard")
    )
