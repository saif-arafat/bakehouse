"""Table definitions for bronze.seeds, kept free of pipeline code so other pipeline files can import them without registering a dataset twice."""

import pyspark.sql.types as T

from lakeora_core.table_schema import Column, TableSchema

us_states = TableSchema(
    catalog="bronze",
    schema="seeds",
    table="us_states",
    comment="USPS state, district and territory codes with full names.",
    columns=[
        Column(
            "state_from_erp",
            T.StringType(),
            comment="The state code as it appears in the ERP system.",
        ),
        Column(
            "raw_format",
            T.StringType(),
            comment="The format of the state code (e.g., abbreviation, full name).",
        ),
        Column("state_code", T.StringType(), comment="The official USPS state code."),
        Column("state_name", T.StringType(), comment="The full name of the state."),
        Column(
            "type",
            T.StringType(),
            comment="The type of region (e.g., State, District, Territory).",
        ),
        Column("fips_code", T.StringType(), comment="The FIPS code for the state."),
        Column(
            "census_region",
            T.StringType(),
            comment="The census region the state belongs to.",
        ),
        Column(
            "census_division",
            T.StringType(),
            comment="The census division the state belongs to.",
        ),
        Column("capital", T.StringType(), comment="The capital city of the state."),
        Column(
            "is_contiguous_us",
            T.BooleanType(),
            comment="Indicates if the state is part of the contiguous United States (true/false).",
        ),
    ],
    primary_key="state_from_erp",
)


countries = TableSchema(
    catalog="bronze",
    schema="seeds",
    table="countries",
    comment="Maps each ERP country spelling to a standard country name, ISO codes and UN M49 region.",
    columns=[
        Column(
            "country_from_erp",
            T.StringType(),
            comment="The country as it appears in the ERP system (e.g., US, USA).",
        ),
        Column("country_name", T.StringType(), comment="The standard English country name."),
        Column("iso2_code", T.StringType(), comment="ISO 3166-1 alpha-2 country code."),
        Column("iso3_code", T.StringType(), comment="ISO 3166-1 alpha-3 country code."),
        Column("continent", T.StringType(), comment="The continent the country belongs to."),
        Column(
            "subregion",
            T.StringType(),
            comment="The UN M49 sub-region the country belongs to.",
        ),
    ],
    primary_key="country_from_erp",
)
