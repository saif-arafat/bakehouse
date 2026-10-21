"""Table definitions for bronze.erp, kept free of pipeline code so other pipeline files can import them without registering a dataset twice."""

import pyspark.sql.types as T

from lakeora_core.table_schema import Column, TableSchema

brz_erp__customer = TableSchema(
    catalog="bronze",
    schema="erp",
    table="customer",
    comment="Raw copy of ERP customers (samples.bakehouse.sales_customers). One row per customer.",
    tags={"domain": "customer", "source_system": "erp", "contains_pii": "true"},
    columns=[
        Column(
            "customerid",
            T.LongType(),
            comment="Natural key of the customer in the ERP.",
        ),
        Column("first_name", T.StringType(), tags={"pii": "name"}),
        Column("last_name", T.StringType(), tags={"pii": "name"}),
        Column("email_address", T.StringType(), tags={"pii": "email"}),
        Column(
            "phone_number",
            T.StringType(),
            comment='Free-text phone number, may contain an extension (e.g. "x123").',
            tags={"pii": "phone"},
        ),
        Column(
            "address",
            T.StringType(),
            comment="Street address.",
            tags={"pii": "address"},
        ),
        Column("city", T.StringType()),
        Column(
            "state",
            T.StringType(),
            comment="State as entered in the ERP; a mix of USPS codes and full names (see the us_states seed).",
        ),
        Column("country", T.StringType()),
        Column("continent", T.StringType()),
        Column("postal_zip_code", T.LongType()),
        Column("gender", T.StringType()),
        Column(
            "created_date",
            T.DateType(),
            comment="Date the customer was created in the ERP. Assumed to be 2026-10-19 for every customer.",
        ),
        Column("md_ingested_ts", T.TimestampType()),
    ],
    primary_key="customerid",
)


brz_erp__customer_history = TableSchema(
    catalog="bronze",
    schema="erp",
    table="customer_history",
    comment=(
        "SCD2 history of ERP customers. The ERP only holds current state, so each update compares the tracked "
        "columns to the latest version and closes/opens a version when any of them changed."
    ),
    tags={"domain": "customer", "source_system": "erp", "contains_pii": "true"},
    columns=[
        *brz_erp__customer.columns,
        Column(
            "__START_AT",
            T.TimestampType(),
            comment="md_ingested_ts of the bronze row that opened this version.",
        ),
        Column(
            "__END_AT",
            T.TimestampType(),
            comment="__START_AT of the next version. Null for the current version.",
        ),
    ],
    primary_key=["customerid", "created_date", "__START_AT"],
)


brz_erp__franchise = TableSchema(
    catalog="bronze",
    schema="erp",
    table="franchise",
    comment="Raw copy of ERP franchises (samples.bakehouse.sales_franchises). One row per franchise.",
    tags={"domain": "branch", "source_system": "erp"},
    columns=[
        Column(
            "franchiseid",
            T.LongType(),
            comment="Natural key of the franchise in the ERP.",
        ),
        Column("name", T.StringType()),
        Column("city", T.StringType()),
        Column("district", T.StringType()),
        Column("zipcode", T.StringType()),
        Column("country", T.StringType()),
        Column("size", T.StringType()),
        Column("longitude", T.DoubleType()),
        Column("latitude", T.DoubleType()),
        Column("supplierid", T.LongType(), comment="The franchise's ingredient supplier."),
        Column(
            "created_date",
            T.DateType(),
            comment="Date the franchise was created in the ERP. Assumed to be as 2026-10-19 for every franchise.",
        ),
        Column("md_ingested_ts", T.TimestampType()),
    ],
    primary_key="franchiseid",
)


brz_erp__supplier = TableSchema(
    catalog="bronze",
    schema="erp",
    table="supplier",
    comment="Raw copy of ERP suppliers (samples.bakehouse.sales_suppliers). One row per supplier.",
    tags={"domain": "supply", "source_system": "erp"},
    columns=[
        Column(
            "supplierid",
            T.LongType(),
            comment="Natural key of the supplier in the ERP.",
        ),
        Column("name", T.StringType()),
        Column(
            "ingredient",
            T.StringType(),
            comment="The ingredient the supplier provides.",
        ),
        Column("continent", T.StringType()),
        Column("city", T.StringType()),
        Column("district", T.StringType()),
        Column("size", T.StringType()),
        Column("longitude", T.DoubleType()),
        Column("latitude", T.DoubleType()),
        Column("approved", T.StringType(), comment="Approval flag as a Y/N string."),
        Column(
            "created_date",
            T.DateType(),
            comment="Date the supplier was created in the ERP. Assumed to be 2026-10-19 for every supplier.",
        ),
        Column("md_ingested_ts", T.TimestampType()),
    ],
    primary_key="supplierid",
)


brz_erp__transaction = TableSchema(
    catalog="bronze",
    schema="erp",
    table="transaction",
    comment=(
        "Raw copy of ERP sales transactions (samples.bakehouse.sales_transactions). One row per transaction. "
        "Incremental append: only rows newer than the latest dateTime already loaded are added."
    ),
    tags={"domain": "sales", "source_system": "erp", "contains_pii": "true"},
    columns=[
        Column(
            "transactionid",
            T.LongType(),
            comment="Natural key of the transaction in the ERP.",
        ),
        Column("customerid", T.LongType()),
        Column("franchiseid", T.LongType()),
        Column(
            "datetime",
            T.TimestampType(),
            comment="When the transaction happened; also the incremental watermark.",
        ),
        Column("product", T.StringType()),
        Column("quantity", T.LongType()),
        Column("unitprice", T.LongType()),
        Column("totalprice", T.LongType()),
        Column("paymentmethod", T.StringType()),
        Column(
            "cardnumber",
            T.LongType(),
            comment="Payment card number.",
            tags={"pii": "payment_card"},
        ),
        Column("md_ingested_ts", T.TimestampType()),
    ],
    primary_key="transactionid",
)
