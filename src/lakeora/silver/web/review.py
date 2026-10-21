import pyspark.sql.functions as F
import pyspark.sql.types as T
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.bronze.web.models import brz_web__review
from lakeora.silver.web.models import review
from lakeora_core.keys import surrogate_key


@dp.table(name=review.full_name, comment=review.comment, schema=review.ddl())
@dp.expect_or_fail("review_id_not_null", "review_id IS NOT NULL")
def load_silver_web_review():
    df = spark.readStream.table(brz_web__review.full_name).select(
        surrogate_key("new_id").alias("review_sk"),
        F.col("new_id").alias("review_id"),
        # an id can belong to different franchises over time push to dim_franchise
        F.col("franchiseid").cast(T.LongType()).alias("franchise_id"),
        F.col("review").alias("review_text"),
        # VARIANT: {"response": {"star_rating": {"value": 4.5}}}
        F.expr("""ai_extract(review, '{
                "star_rating": {
                    "type": "number",
                    "description": "Star rating out of 5 explicitly stated in the review, e.g. 4.5. Null if no rating is stated."
                }
            }'):response.star_rating.value::decimal(2, 1)""").alias("review_star_rating"),
        "review_date",
        F.current_timestamp().alias("md_loaded_at_ts"),
    )
    return review.conform(df)
