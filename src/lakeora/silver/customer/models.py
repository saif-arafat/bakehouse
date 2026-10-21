"""Table definitions for silver.customer, kept free of pipeline code so other pipeline files can import them without registering a dataset twice."""

import pyspark.sql.types as T

from lakeora_core.table_schema import Column, TableSchema

silver_customer = TableSchema(
    catalog="silver",
    schema="customer",
    table="customer",
    comment=(
        "SCD2: one row per version of an ERP customer (customer_id + created date, since the ERP can reuse an id), "
        "built from the bronze customer_history SCD2 table, with cleaned phone number and the state resolved to its full name."
    ),
    tags={"domain": "customer", "source_system": "erp", "contains_pii": "true"},
    columns=[
        Column(
            "customer_sk",
            T.StringType(),
            comment=(
                "Surrogate key of the version, hash of customer_id, customer_created_date and the "
                "history version's __START_AT."
            ),
        ),
        Column(
            "customer_durable_sk",
            T.StringType(),
            nullable=False,
            comment="Stable key of the customer across versions, hash of customer_id and customer_created_date.",
        ),
        Column(
            "customer_id",
            T.LongType(),
            nullable=False,
            comment=(
                "Natural key from the ERP (customerID). Not unique on its own once an id is reused "
                "or a customer changes."
            ),
        ),
        Column(
            "customer_created_date",
            T.DateType(),
            comment="When the ERP created the customer; with customer_id it forms the business key.",
        ),
        Column(
            "customer_valid_from",
            T.DateType(),
            comment=(
                "First sale date this version owns its customer_id. The id's first version opens at 1900-01-01 so "
                "sales dated before the created date still resolve; a customer reusing the id starts at its "
                "customer_created_date; later versions of a customer start the day the history saw the change."
            ),
        ),
        Column(
            "customer_valid_to",
            T.DateType(),
            comment=(
                "Exclusive end, the date the next version (a change, or a new customer taking over the id) starts. "
                "Null for the id's latest version."
            ),
        ),
        Column(
            "customer_is_current",
            T.BooleanType(),
            comment="True for the customer's latest version.",
        ),
        Column(
            "customer_full_name",
            T.StringType(),
            comment="First and last name joined with a space.",
            tags={"pii": "name"},
        ),
        Column("customer_first_name", T.StringType(), tags={"pii": "name"}),
        Column("customer_last_name", T.StringType(), tags={"pii": "name"}),
        Column("customer_email", T.StringType(), tags={"pii": "email"}),
        Column(
            "customer_phone_number",
            T.LongType(),
            comment="Phone number as digits only, with any extension removed.",
            tags={"pii": "phone"},
        ),
        Column(
            "raw_phone_number",
            T.StringType(),
            comment="Phone number exactly as entered in the ERP, kept for auditing the cleaning.",
            tags={"pii": "phone"},
        ),
        Column(
            "customer_address",
            T.StringType(),
            comment="Street address.",
            tags={"pii": "address"},
        ),
        Column("customer_city", T.StringType()),
        Column(
            "customer_state",
            T.StringType(),
            comment="Full state name, resolved from the ERP code or name via the us_states seed. Null when unmatched.",
        ),
        Column(
            "customer_country",
            T.StringType(),
            comment="Country as spelled in the ERP; standardised against the countries seed in gold.",
        ),
        Column("customer_continent", T.StringType()),
        Column("customer_postal_code", T.IntegerType()),
        Column(
            "customer_gender",
            T.StringType(),
            comment="Lower-cased, trimmed gender from the ERP.",
        ),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="customer_sk",
)
