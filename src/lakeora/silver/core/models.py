"""Table definitions for silver.core, kept free of pipeline code so other pipeline files can import them without registering a dataset twice."""

import pyspark.sql.types as T

from lakeora_core.table_schema import Column, TableSchema

state = TableSchema(
    catalog="silver",
    schema="core",
    table="state",
    comment=(
        "Reference list of US states, districts and territories built from the us_states seed. "
        "One row per ERP spelling of a state (a state can appear twice: as its code and as its full name)."
    ),
    tags={"domain": "core", "source_system": "seed"},
    columns=[
        Column("state_sk", T.StringType(), comment="Surrogate key, hash of state_from_erp."),
        Column(
            "state_from_erp",
            T.StringType(),
            nullable=False,
            comment="The state exactly as it appears in the ERP; the join key to customer data.",
        ),
        Column(
            "state_raw_format",
            T.StringType(),
            comment="Whether state_from_erp is an abbreviation or a full name.",
        ),
        Column("state_state_code", T.StringType(), comment="Official two-letter USPS code."),
        Column("state_state_name", T.StringType()),
        Column("state_type", T.StringType(), comment="State, District or Territory."),
        Column(
            "state_fips_code",
            T.StringType(),
            comment="Two-digit FIPS code, kept as a string to preserve the leading zero.",
        ),
        Column("state_census_region", T.StringType()),
        Column("state_census_division", T.StringType()),
        Column("state_capital", T.StringType()),
        Column(
            "state_is_contiguous_us",
            T.BooleanType(),
            comment="True for the 48 contiguous states plus DC.",
        ),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="state_sk",
)
