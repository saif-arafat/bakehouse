"""Table definitions for silver.product, kept free of pipeline code so other pipeline files can import them without registering a dataset twice."""

import pyspark.sql.types as T

from lakeora_core.table_schema import Column, TableSchema

product = TableSchema(
    catalog="silver",
    schema="product",
    table="product",
    comment=(
        "One row per product. The ERP has no product master, so products are derived from the "
        "distinct product names seen in transactions."
    ),
    tags={"domain": "product", "source_system": "erp"},
    columns=[
        Column("product_sk", T.StringType(), comment="Surrogate key, hash of product_name."),
        Column(
            "product_name",
            T.StringType(),
            nullable=False,
            comment="Product name as it appears on transactions; the natural key.",
        ),
        Column(
            "product_unit_price",
            T.DecimalType(18, 6),
            comment="Highest unit price ever charged for the product.",
        ),
        Column(
            "product_release_date",
            T.TimestampType(),
            comment="Timestamp of the product's first transaction (a first-sold date, not a launch date).",
        ),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="product_sk",
)
