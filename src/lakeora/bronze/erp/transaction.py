import pyspark.sql.functions as F
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.bronze.erp.models import brz_erp__transaction


@dp.table(
    name=brz_erp__transaction.full_name,
    comment=brz_erp__transaction.comment,
    schema=brz_erp__transaction.ddl(),
)
def load_bronze_erp_transaction():
    df = spark.readStream.table("samples.bakehouse.sales_transactions").withColumn(
        "md_ingested_ts", F.current_timestamp()
    )
    return brz_erp__transaction.conform(df)
