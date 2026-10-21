import pyspark.sql.functions as F
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.bronze.web.models import brz_web__review


@dp.table(
    name=brz_web__review.full_name,
    comment=brz_web__review.comment,
    schema=brz_web__review.ddl(),
)
def load_bronze_web_review():
    df = spark.readStream.table("samples.bakehouse.media_customer_reviews").withColumn(
        "md_ingested_ts", F.current_timestamp()
    )
    return brz_web__review.conform(df)
