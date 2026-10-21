"""Table definitions for silver.web, kept free of pipeline code so other pipeline files can import them without registering a dataset twice."""

import pyspark.sql.types as T

from lakeora_core.table_schema import Column, TableSchema

review = TableSchema(
    catalog="silver",
    schema="web",
    table="review",
    comment="One row per web review of a franchise, with the star rating extracted from the free text by ai_extract.",
    tags={"domain": "feedback", "source_system": "web"},
    columns=[
        Column("review_sk", T.StringType(), comment="Surrogate key, hash of review_id."),
        Column(
            "review_id",
            T.IntegerType(),
            nullable=False,
            comment="Natural key of the review (new_id in the source).",
        ),
        Column(
            "franchise_id",
            T.LongType(),
            comment=(
                "The reviewed franchise's ERP id (franchiseID in the web source). Not a key on its own (ids can be "
                "reused), so fct_review resolves it to franchise_sk using the review date."
            ),
        ),
        Column("review_text", T.StringType(), comment="Free-text review body."),
        Column(
            "review_star_rating",
            T.DecimalType(2, 1),
            comment=(
                "Star rating out of 5 that the review explicitly states, extracted with ai_extract. "
                "Null when the review states no rating."
            ),
        ),
        Column("review_date", T.TimestampType()),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="review_sk",
)
