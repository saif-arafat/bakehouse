import datetime

import pyspark.sql.functions as F
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.gold.conformed.common import with_country_name
from lakeora.gold.conformed.models import dim_franchise
from lakeora.silver.branch.models import franchise
from lakeora.silver.supply.models import supplier
from lakeora_core.dimensions import unknown_member
from lakeora_core.keys import entity_key


@dp.materialized_view(
    name=dim_franchise.full_name,
    comment=dim_franchise.comment,
    schema=dim_franchise.ddl(),
)
@dp.expect_or_fail("franchise_id_not_null", "franchise_id IS NOT NULL")
def load_gold_conformed_dim_franchise():
    # the ERP is current state, so a franchise's supplierID means whoever holds that id now
    current_suppliers = (
        spark.table(supplier.full_name)
        .where("supplier_valid_to IS NULL")
        .select(
            F.col("supplier_id").alias("_supplier_id"),
            entity_key("supplier_id", "supplier_created_date").alias("supplier_key"),
            "supplier_name",
            F.col("material_name").alias("supplier_material_name"),
            "supplier_continent",
            "supplier_city",
            F.col("is_approved").alias("is_supplier_approved"),
        )
    )
    franchises = spark.table(franchise.full_name).withColumn(
        "franchise_key", entity_key("franchise_id", "franchise_created_date")
    )
    df = (
        with_country_name(spark, franchises, "franchise_country")
        .join(
            current_suppliers,
            F.col("supplier_id") == current_suppliers._supplier_id,
            "left",
        )
        .withColumn("supplier_key", F.coalesce("supplier_key", F.lit(-1)))
        .withColumn("md_loaded_at_ts", F.current_timestamp())
    )
    unknown = unknown_member(
        spark,
        dim_franchise,
        franchise_key=-1,
        franchise_id=-1,
        franchise_valid_from=datetime.date(1900, 1, 1),
        franchise_name="Unknown",
        supplier_key=-1,
    )
    return unknown.unionByName(dim_franchise.conform(df)).withColumn("md_loaded_at_ts", F.current_timestamp())
