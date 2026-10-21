"""Table definitions for silver.branch, kept free of pipeline code so other pipeline files can import them without registering a dataset twice."""

import pyspark.sql.types as T

from lakeora_core.table_schema import Column, TableSchema

franchise = TableSchema(
    catalog="silver",
    schema="branch",
    table="franchise",
    comment=(
        "One row per bakery franchise (branch) from the ERP (franchise_id + created date, since the ERP can "
        "reuse an id), carrying the id of its single ingredient supplier. Location is stored as an EWKT point (SRID 4326)."
    ),
    tags={"domain": "branch", "source_system": "erp"},
    columns=[
        Column(
            "franchise_sk",
            T.StringType(),
            comment="Surrogate key, hash of franchise_id and franchise_created_date.",
        ),
        Column(
            "franchise_id",
            T.LongType(),
            nullable=False,
            comment="Natural key from the ERP (franchiseID). Not unique on its own once an id is reused.",
        ),
        Column(
            "franchise_created_date",
            T.DateType(),
            comment="When the ERP created the franchise; with franchise_id it forms the business key.",
        ),
        Column(
            "franchise_valid_from",
            T.DateType(),
            comment=(
                "First event date this row owns its franchise_id. The id's first row opens at 1900-01-01 so events "
                "dated before the created date still resolve; later rows start at their franchise_created_date."
            ),
        ),
        Column(
            "franchise_valid_to",
            T.DateType(),
            comment="Exclusive end, the date the next franchise took over this franchise_id. Null while current.",
        ),
        Column(
            "supplier_id",
            T.LongType(),
            comment=(
                "The franchise's ingredient supplier, as its ERP id. Resolved to supplier_sk in dim_franchise "
                "against the supplier currently holding the id."
            ),
        ),
        Column("franchise_name", T.StringType()),
        Column("franchise_city", T.StringType()),
        Column("franchise_district", T.StringType()),
        Column("franchise_zipcode", T.StringType()),
        Column(
            "franchise_country",
            T.StringType(),
            comment="Country as spelled in the ERP; standardised against the countries seed in gold.",
        ),
        Column("franchise_size", T.StringType()),
        Column(
            "franchise_location",
            T.StringType(),
            comment="Franchise coordinates as an EWKT point string: SRID=4326;POINT(lon lat).",
        ),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="franchise_sk",
)
