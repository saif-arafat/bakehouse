import pyspark.sql.functions as F
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.gold.conformed.models import dim_franchise
from lakeora.gold.feedback.models import fct_review
from lakeora.silver.web.models import review
from lakeora_core.dimensions import lookup_as_of
from lakeora_core.keys import date_key


@dp.table(name=fct_review.full_name, comment=fct_review.comment, schema=fct_review.ddl())
@dp.expect_or_fail("review_id_not_null", "review_id IS NOT NULL")
@dp.expect("franchise_resolved", "franchise_key <> -1")
def load_gold_feedback_fct_review():
    review_day = F.to_date("review_date")
    reviews = lookup_as_of(
        spark.readStream.table(review.full_name),
        spark.table(dim_franchise.full_name),
        on=("franchise_id", "franchise_id"),
        at=review_day,
        keys={"franchise_key": "franchise_key"},
        valid_from="franchise_valid_from",
        valid_to="franchise_valid_to",
    )
    df = reviews.select(
        "review_id",
        date_key(review_day).alias("date_key"),
        "franchise_key",
        F.col("review_date").alias("review_datetime"),
        "review_text",
        F.col("review_star_rating").alias("star_rating"),
        F.col("review_star_rating").isNotNull().alias("has_star_rating"),
        F.size(F.split(F.trim("review_text"), r"\s+")).alias("review_word_count"),
        F.length("review_text").alias("review_char_count"),
        F.current_timestamp().alias("md_loaded_at_ts"),
    )
    return fct_review.conform(df)
