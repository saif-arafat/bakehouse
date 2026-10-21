"""Table definitions for bronze.web, kept free of pipeline code so other pipeline files can import them without registering a dataset twice."""

import pyspark.sql.types as T

from lakeora_core.table_schema import Column, TableSchema

brz_web__review = TableSchema(
    catalog="bronze",
    schema="web",
    table="review",
    comment=(
        "Raw copy of customer reviews scraped from the web (samples.bakehouse.media_customer_reviews). "
        "One row per review."
    ),
    tags={"domain": "feedback", "source_system": "web"},
    columns=[
        Column("review", T.StringType(), comment="Free-text review body."),
        Column("franchiseid", T.LongType(), comment="The franchise the review is about."),
        Column("review_date", T.TimestampType()),
        Column("new_id", T.LongType(), comment="Natural key of the review."),
        Column("md_ingested_ts", T.TimestampType()),
    ],
    primary_key="new_id",
)


brz_web__review_chunked = TableSchema(
    catalog="bronze",
    schema="web",
    table="review_chunked",
    comment=(
        "Raw copy of web reviews split into text chunks (samples.bakehouse.media_gold_reviews_chunked). "
        "One row per chunk, intended for vector search / RAG."
    ),
    tags={"domain": "feedback", "source_system": "web"},
    columns=[
        Column("franchiseid", T.LongType()),
        Column("review_date", T.TimestampType()),
        Column(
            "chunked_text",
            T.StringType(),
            comment="The text of this chunk of the review.",
        ),
        Column(
            "chunk_id",
            T.StringType(),
            comment=(
                "Identifier of the chunk text. Not unique: the source repeats the same chunk under "
                "several franchiseID / review_date combinations."
            ),
        ),
        Column(
            "review_uri",
            T.StringType(),
            comment="URI of the original review the chunk came from.",
        ),
        Column("md_ingested_ts", T.TimestampType()),
    ],
)
