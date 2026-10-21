import pyspark.sql.functions as F
import pyspark.sql.types as T
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.bronze.erp.models import brz_erp__franchise


@dp.table(
    name=brz_erp__franchise.full_name,
    comment=brz_erp__franchise.comment,
    schema=brz_erp__franchise.ddl(),
)
def load_bronze_erp_franchise():
    df = (
        spark.readStream.table("samples.bakehouse.sales_franchises")
        .withColumn("md_ingested_ts", F.current_timestamp())
        .withColumn("created_date", F.lit("2026-10-19").cast(T.DateType()))
    )
    return brz_erp__franchise.conform(df)
