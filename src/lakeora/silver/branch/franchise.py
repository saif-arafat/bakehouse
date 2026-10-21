import pyspark.sql.functions as F
import pyspark.sql.types as T
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import Window

from lakeora.bronze.erp.models import brz_erp__franchise
from lakeora.silver.branch.models import franchise
from lakeora_core.keys import surrogate_key


@dp.materialized_view(name=franchise.full_name, comment=franchise.comment, schema=franchise.ddl())
@dp.expect_or_fail("franchise_id_not_null", "franchise_id IS NOT NULL")
def load_silver_branch_franchise():

    by_id = Window.partitionBy("franchiseid").orderBy("created_date")
    df = spark.table(brz_erp__franchise.full_name).select(
        surrogate_key("franchiseid", "created_date").alias("franchise_sk"),
        F.col("franchiseid").alias("franchise_id"),
        F.col("created_date").alias("franchise_created_date"),
        # for records with same id the first one is the start of history and takes the 1900-01-01
        # otherwise its the created_date of the record
        F.when(F.row_number().over(by_id) == 1, F.lit("1900-01-01").cast(T.DateType()))
        .otherwise(F.col("created_date"))
        .alias("franchise_valid_from"),
        # lead to get the next record's created_date, which is the valid_to of the current record
        F.lead("created_date").over(by_id).alias("franchise_valid_to"),
        F.col("supplierid").alias("supplier_id"),
        F.col("name").alias("franchise_name"),
        F.col("city").alias("franchise_city"),
        F.col("district").alias("franchise_district"),
        F.col("zipcode").alias("franchise_zipcode"),
        F.col("country").alias("franchise_country"),
        F.col("size").alias("franchise_size"),
        # EWKT string, SRID 4326 = WGS 84
        F.expr("st_asewkt(st_point(longitude, latitude, 4326))").alias("franchise_location"),
        F.current_timestamp().alias("md_loaded_at_ts"),
    )
    return franchise.conform(df)
