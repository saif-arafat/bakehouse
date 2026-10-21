import datetime

import pyspark.sql.functions as F
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.gold.conformed.models import dim_supplier
from lakeora.silver.supply.models import supplier
from lakeora_core.dimensions import unknown_member
from lakeora_core.keys import entity_key


@dp.materialized_view(name=dim_supplier.full_name, comment=dim_supplier.comment, schema=dim_supplier.ddl())
@dp.expect_or_fail("supplier_id_not_null", "supplier_id IS NOT NULL")
def load_gold_conformed_dim_supplier():
    df = spark.table(supplier.full_name).select(
        entity_key("supplier_id", "supplier_created_date").alias("supplier_key"),
        "supplier_id",
        "supplier_created_date",
        "supplier_valid_from",
        "supplier_valid_to",
        "supplier_name",
        F.col("material_name").alias("supplier_material_name"),
        "supplier_size",
        F.col("is_approved").alias("is_supplier_approved"),
        "supplier_city",
        "supplier_district",
        "supplier_continent",
        "supplier_location",
        F.current_timestamp().alias("md_loaded_at_ts"),
    )
    unknown = unknown_member(
        spark,
        dim_supplier,
        supplier_key=-1,
        supplier_id=-1,
        supplier_valid_from=datetime.date(1900, 1, 1),
        supplier_name="Unknown",
    )
    return unknown.unionByName(dim_supplier.conform(df)).withColumn("md_loaded_at_ts", F.current_timestamp())
