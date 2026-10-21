import pyspark.sql.functions as F
import pyspark.sql.types as T
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import Window

from lakeora.bronze.erp.models import brz_erp__supplier
from lakeora.silver.supply.models import supplier
from lakeora_core.keys import surrogate_key


@dp.materialized_view(name=supplier.full_name, comment=supplier.comment, schema=supplier.ddl())
@dp.expect_or_fail("supplier_id_not_null", "supplier_id IS NOT NULL")
def load_silver_supply_supplier():
    by_id = Window.partitionBy("supplierid").orderBy("created_date")
    df = spark.table(brz_erp__supplier.full_name).select(
        # the ERP can reuse an id for a new supplier, so the business key is id + creation date
        surrogate_key("supplierid", "created_date").alias("supplier_sk"),
        F.col("supplierid").alias("supplier_id"),
        F.col("created_date").alias("supplier_created_date"),
        # the id's first supplier owns it from 1900-01-01, a supplier reusing the id from its created_date
        F.when(F.row_number().over(by_id) == 1, F.lit("1900-01-01").cast(T.DateType()))
        .otherwise(F.col("created_date"))
        .alias("supplier_valid_from"),
        F.lead("created_date").over(by_id).alias("supplier_valid_to"),
        F.col("name").alias("supplier_name"),
        F.col("ingredient").alias("material_name"),
        F.col("continent").alias("supplier_continent"),
        F.col("city").alias("supplier_city"),
        F.col("district").alias("supplier_district"),
        F.col("size").alias("supplier_size"),
        # EWKT string, SRID 4326 = WGS 84
        F.expr("st_asewkt(st_point(longitude, latitude, 4326))").alias("supplier_location"),
        F.coalesce(F.upper(F.trim("approved")) == "Y", F.lit(False)).alias("is_approved"),
        F.current_timestamp().alias("md_loaded_at_ts"),
    )
    return supplier.conform(df)
