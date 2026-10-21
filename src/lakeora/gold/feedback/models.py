"""Table definitions for gold.feedback, kept free of pipeline code so other pipeline files can import them without registering a dataset twice."""

import pyspark.sql.types as T

from lakeora.gold.conformed.models import dim_date, dim_franchise
from lakeora_core.table_schema import Column, ForeignKey, TableSchema

fct_review = TableSchema(
    catalog="gold",
    schema="feedback",
    table="fct_review",
    comment=(
        "Review fact, one row per web review of a franchise, with an AI-extracted star rating and text metrics. "
        "Streamed from silver; the franchise key is resolved when a review is processed."
    ),
    tags={"domain": "feedback", "model_type": "fact"},
    columns=[
        Column("review_id", T.IntegerType(), comment="Natural key of the review."),
        Column("date_key", T.IntegerType(), comment="The review date as yyyymmdd."),
        Column(
            "franchise_key",
            T.LongType(),
            comment=(
                "The franchise being reviewed: the dim_franchise row whose validity window holds the review date "
                "for the review's franchise_id. -1 (unknown member) when none matches."
            ),
        ),
        Column("review_datetime", T.TimestampType()),
        Column("review_text", T.StringType(), comment="Free-text review body."),
        Column(
            "star_rating",
            T.DecimalType(2, 1),
            comment="Star rating out of 5 stated in the review, extracted with ai_extract. Null when no rating is stated.",
        ),
        Column(
            "has_star_rating",
            T.BooleanType(),
            comment="True when star_rating could be extracted; use it to filter before averaging ratings.",
        ),
        Column(
            "review_word_count",
            T.IntegerType(),
            comment="Number of whitespace-separated words in review_text.",
        ),
        Column("review_char_count", T.IntegerType()),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="review_id",
    foreign_keys=[
        ForeignKey.to(dim_date, "date_key", name="fk_fct_review_date"),
        ForeignKey.to(dim_franchise, "franchise_key", name="fk_fct_review_franchise"),
    ],
)
