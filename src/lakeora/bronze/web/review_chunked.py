import pyspark.sql.functions as F
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.bronze.web.models import brz_web__review_chunked


@dp.table(
    name=brz_web__review_chunked.full_name,
    comment=brz_web__review_chunked.comment,
    schema=brz_web__review_chunked.ddl(),
)
def load_bronze_web_review_chunked():
    df = spark.readStream.table("samples.bakehouse.media_gold_reviews_chunked").withColumn(
        "md_ingested_ts", F.current_timestamp()
    )
    return brz_web__review_chunked.conform(df)
