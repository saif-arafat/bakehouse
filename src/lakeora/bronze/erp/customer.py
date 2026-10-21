import pyspark.sql.functions as F
import pyspark.sql.types as T
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.bronze.erp.models import brz_erp__customer


@dp.table(
    name=brz_erp__customer.full_name,
    comment=brz_erp__customer.comment,
    schema=brz_erp__customer.ddl(),
)
def load_bronze_erp_customer():
    df = (
        spark.readStream.table("samples.bakehouse.sales_customers")
        .withColumn("md_ingested_ts", F.current_timestamp())
        .withColumn("created_date", F.lit("2026-10-19").cast(T.DateType()))
    )
    return brz_erp__customer.conform(df)
