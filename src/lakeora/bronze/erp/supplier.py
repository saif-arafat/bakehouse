import pyspark.sql.functions as F
import pyspark.sql.types as T
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.bronze.erp.models import brz_erp__supplier


@dp.table(
    name=brz_erp__supplier.full_name,
    comment=brz_erp__supplier.comment,
    schema=brz_erp__supplier.ddl(),
)
def load_bronze_erp_supplier():
    df = (
        spark.readStream.table("samples.bakehouse.sales_suppliers")
        .withColumn("md_ingested_ts", F.current_timestamp())
        .withColumn("created_date", F.lit("2026-10-19").cast(T.DateType()))
    )
    return brz_erp__supplier.conform(df)
