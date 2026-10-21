"""Table definitions for silver.supply, kept free of pipeline code so other pipeline files can import them without registering a dataset twice."""

import pyspark.sql.types as T

from lakeora_core.table_schema import Column, TableSchema

supplier = TableSchema(
    catalog="silver",
    schema="supply",
    table="supplier",
    comment=(
        "One row per ingredient supplier from the ERP (supplier_id + created date, since the ERP can reuse an id), "
        "cleaned and typed. Location is stored as an EWKT point (SRID 4326)."
    ),
    tags={"domain": "supply", "source_system": "erp"},
    columns=[
        Column(
            "supplier_sk",
            T.StringType(),
            comment="Surrogate key, hash of supplier_id and supplier_created_date.",
        ),
        Column(
            "supplier_id",
            T.LongType(),
            nullable=False,
            comment="Natural key from the ERP (supplierID). Not unique on its own once an id is reused.",
        ),
        Column(
            "supplier_created_date",
            T.DateType(),
            comment="When the ERP created the supplier; with supplier_id it forms the business key.",
        ),
        Column(
            "supplier_valid_from",
            T.DateType(),
            comment=(
                "First date this row owns its supplier_id. The id's first row opens at 1900-01-01 so references "
                "dated before the created date still resolve; later rows start at their supplier_created_date."
            ),
        ),
        Column(
            "supplier_valid_to",
            T.DateType(),
            comment="Exclusive end, the date the next supplier took over this supplier_id. Null while current.",
        ),
        Column("supplier_name", T.StringType()),
        Column(
            "material_name",
            T.StringType(),
            comment="The ingredient this supplier provides.",
        ),
        Column("supplier_continent", T.StringType()),
        Column("supplier_city", T.StringType()),
        Column("supplier_district", T.StringType()),
        Column("supplier_size", T.StringType()),
        Column(
            "supplier_location",
            T.StringType(),
            comment="Supplier coordinates as an EWKT point string: SRID=4326;POINT(lon lat).",
        ),
        Column(
            "is_approved",
            T.BooleanType(),
            comment="True when the ERP approved flag is 'Y'; anything else (including null) is false.",
        ),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="supplier_sk",
)
