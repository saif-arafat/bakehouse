"""Table definitions for gold.conformed, kept free of pipeline code so other pipeline files can import them without registering a dataset twice.

Conformed dimensions shared by every fact and mart in gold. Keys follow
https://docs.databricks.com/aws/en/ldp/best-practices/dimensional-modeling: natural keys where the source's
is stable, otherwise an order-preserving key derived from it (lakeora_core.keys), never a hash.
"""

import pyspark.sql.types as T

from lakeora_core.table_schema import Column, ForeignKey, TableSchema

dim_geography = TableSchema(
    catalog="gold",
    schema="conformed",
    table="dim_geography",
    comment=(
        "Geography dimension at country grain, one row per standard country name. "
        "Country is the only level shared cleanly across sources (customer city/state values are synthetic "
        "and suppliers carry no country). Countries missing from the countries seed still appear, with null codes."
    ),
    tags={"domain": "geography", "model_type": "dimension"},
    columns=[
        Column(
            "country_name",
            T.StringType(),
            comment=(
                "Standard English country name from the countries seed (falls back to the ERP spelling); "
                "the natural key."
            ),
        ),
        Column("country_iso2_code", T.StringType(), comment="ISO 3166-1 alpha-2 code."),
        Column("country_iso3_code", T.StringType(), comment="ISO 3166-1 alpha-3 code."),
        Column("country_subregion", T.StringType(), comment="UN M49 sub-region."),
        Column("country_continent", T.StringType()),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="country_name",
)


dim_supplier = TableSchema(
    catalog="gold",
    schema="conformed",
    table="dim_supplier",
    comment=(
        "One row per ingredient supplier (supplier_id + created date). Includes an unknown member (supplier_key -1)."
    ),
    tags={"domain": "supply", "model_type": "dimension"},
    columns=[
        Column(
            "supplier_key",
            T.LongType(),
            comment=(
                "supplier_id followed by supplier_created_date as yyyymmdd, so it sorts like the business key "
                "and survives a rebuild; -1 for the unknown member."
            ),
        ),
        Column(
            "supplier_id",
            T.LongType(),
            nullable=False,
            comment="Natural key from the ERP (supplierID); -1 for the unknown member. Not unique once an id is reused.",
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
                "dated before the created date still resolve."
            ),
        ),
        Column(
            "supplier_valid_to",
            T.DateType(),
            comment="Exclusive end, the date the next supplier took over this supplier_id. Null while current.",
        ),
        Column("supplier_name", T.StringType()),
        Column(
            "supplier_material_name",
            T.StringType(),
            comment="The ingredient the supplier provides.",
        ),
        Column("supplier_size", T.StringType()),
        Column("is_supplier_approved", T.BooleanType()),
        Column("supplier_city", T.StringType()),
        Column("supplier_district", T.StringType()),
        Column("supplier_continent", T.StringType()),
        Column(
            "supplier_location",
            T.StringType(),
            comment="Supplier coordinates as an EWKT point string: SRID=4326;POINT(lon lat).",
        ),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="supplier_key",
)


dim_customer = TableSchema(
    catalog="gold",
    schema="conformed",
    table="dim_customer",
    comment=(
        "Customer dimension, SCD2: one row per version of an ERP customer (customer_id + created date), "
        "enriched with US state attributes and linked to dim_geography at country grain. Facts pick the "
        "version whose customer_valid_from/to window holds the sale date; customer_durable_key groups a "
        "customer's versions and customer_is_current flags the latest. Includes an unknown member (customer_key -1)."
    ),
    tags={"domain": "customer", "model_type": "dimension", "contains_pii": "true"},
    columns=[
        Column(
            "customer_key",
            T.LongType(),
            comment=(
                "Key of the version: customer_durable_key followed by the 3-digit version number, so it sorts "
                "like (customer_id, customer_created_date, version) and survives a rebuild; -1 for the unknown member."
            ),
        ),
        Column(
            "customer_durable_key",
            T.LongType(),
            nullable=False,
            comment=(
                "Stable key of the customer across versions: customer_id followed by customer_created_date as "
                "yyyymmdd; -1 for the unknown member. Group or count distinct on this to treat a customer as one."
            ),
        ),
        Column(
            "country_name",
            T.StringType(),
            comment="The customer's country, the key into dim_geography.",
        ),
        Column(
            "customer_id",
            T.LongType(),
            nullable=False,
            comment=(
                "Natural key from the ERP (customerID); -1 for the unknown member. Not unique once an id is reused "
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
                "sales dated before the created date still resolve."
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
            comment="True for the customer's latest version (and the unknown member).",
        ),
        Column("customer_full_name", T.StringType(), tags={"pii": "name"}),
        Column("customer_first_name", T.StringType(), tags={"pii": "name"}),
        Column("customer_last_name", T.StringType(), tags={"pii": "name"}),
        Column("customer_email", T.StringType(), tags={"pii": "email"}),
        Column(
            "customer_phone_number",
            T.LongType(),
            comment="Phone number as digits only, with any extension removed.",
            tags={"pii": "phone"},
        ),
        Column("customer_gender", T.StringType()),
        Column(
            "customer_address",
            T.StringType(),
            comment="Street address.",
            tags={"pii": "address"},
        ),
        Column("customer_city", T.StringType()),
        Column("customer_postal_code", T.IntegerType()),
        Column(
            "customer_state",
            T.StringType(),
            comment="Full state name. Null when the ERP value did not match the us_states seed.",
        ),
        Column("customer_state_code", T.StringType(), comment="Two-letter USPS state code."),
        Column(
            "customer_state_type",
            T.StringType(),
            comment="State, District or Territory.",
        ),
        Column("customer_census_region", T.StringType()),
        Column("customer_census_division", T.StringType()),
        Column("customer_is_contiguous_us", T.BooleanType()),
        Column(
            "customer_country",
            T.StringType(),
            comment="Country as spelled in the ERP; use country_name / dim_geography for the standard name.",
        ),
        Column("customer_continent", T.StringType()),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="customer_key",
    foreign_keys=[ForeignKey.to(dim_geography, "country_name", name="fk_dim_customer_geography")],
)


dim_date = TableSchema(
    catalog="gold",
    schema="conformed",
    table="dim_date",
    comment=(
        "Calendar dimension, one row per day from 2000-01-01 to 2059-12-31. "
        "Weeks follow ISO 8601 (start Monday, week 1 contains the first Thursday)."
    ),
    tags={"domain": "calendar", "model_type": "dimension"},
    columns=[
        Column(
            "date_key",
            T.IntegerType(),
            comment="Date as a yyyyMMdd integer, e.g. 20261017; the natural, order-preserving join key from facts.",
        ),
        Column("date_date", T.DateType(), nullable=False, comment="The calendar date."),
        Column("date_year", T.IntegerType()),
        Column("year_start_date", T.DateType()),
        Column("year_end_date", T.DateType()),
        Column("date_day_of_year", T.IntegerType()),
        Column("is_leap_year", T.BooleanType()),
        Column("date_quarter", T.IntegerType()),
        Column("quarter_name", T.StringType(), comment="e.g. Q4"),
        Column("year_quarter", T.StringType(), comment="e.g. 2026-Q4"),
        Column("quarter_start_date", T.DateType()),
        Column("quarter_end_date", T.DateType()),
        Column("date_month", T.IntegerType()),
        Column("month_name", T.StringType()),
        Column("month_name_short", T.StringType()),
        Column("year_month", T.StringType(), comment="e.g. 2026-10"),
        Column("month_start_date", T.DateType()),
        Column("month_end_date", T.DateType()),
        Column("days_in_month", T.IntegerType()),
        Column("is_month_start", T.BooleanType()),
        Column("is_month_end", T.BooleanType()),
        Column("iso_week_of_year", T.IntegerType()),
        Column(
            "iso_year",
            T.IntegerType(),
            comment="ISO week-numbering year; differs from date_year for some days around 1 January.",
        ),
        Column("iso_year_week", T.StringType(), comment="e.g. 2026-W42"),
        Column("week_start_date", T.DateType(), comment="Monday of the ISO week."),
        Column("week_end_date", T.DateType(), comment="Sunday of the ISO week."),
        Column("date_day_of_month", T.IntegerType()),
        Column(
            "date_day_of_week",
            T.IntegerType(),
            comment="1 = Sunday ... 7 = Saturday (Spark convention). See iso_day_of_week for Monday-based.",
        ),
        Column("iso_day_of_week", T.IntegerType(), comment="1 = Monday ... 7 = Sunday."),
        Column("day_name", T.StringType()),
        Column("day_name_short", T.StringType()),
        Column("is_weekend", T.BooleanType(), comment="Saturday or Sunday."),
        Column("is_weekday", T.BooleanType()),
        Column("prior_day_date", T.DateType()),
        Column(
            "same_day_prior_year_date",
            T.DateType(),
            comment="Same day one year earlier; 29 February maps to 28 February.",
        ),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="date_key",
)


dim_franchise = TableSchema(
    catalog="gold",
    schema="conformed",
    table="dim_franchise",
    comment=(
        "Franchise dimension, one row per franchise (franchise_id + created date). The franchise's single "
        "supplier is denormalised onto it so consumers don't need a dim_supplier join. Facts pick the row whose "
        "franchise_valid_from/to window holds the event date. Includes an unknown member (franchise_key -1)."
    ),
    tags={"domain": "branch", "model_type": "dimension"},
    columns=[
        Column(
            "franchise_key",
            T.LongType(),
            comment=(
                "franchise_id followed by franchise_created_date as yyyymmdd, so it sorts like the business key "
                "and survives a rebuild; -1 for the unknown member."
            ),
        ),
        Column(
            "country_name",
            T.StringType(),
            comment="The franchise's country, the key into dim_geography.",
        ),
        Column(
            "franchise_id",
            T.LongType(),
            nullable=False,
            comment="Natural key from the ERP (franchiseID); -1 for the unknown member. Not unique once an id is reused.",
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
                "dated before the created date still resolve."
            ),
        ),
        Column(
            "franchise_valid_to",
            T.DateType(),
            comment="Exclusive end, the date the next franchise took over this franchise_id. Null while current.",
        ),
        Column("franchise_name", T.StringType()),
        Column("franchise_size", T.StringType()),
        Column("franchise_city", T.StringType()),
        Column("franchise_district", T.StringType()),
        Column("franchise_zipcode", T.StringType()),
        Column(
            "franchise_country",
            T.StringType(),
            comment="Country as spelled in the ERP; use country_name / dim_geography for the standard name.",
        ),
        Column(
            "franchise_location",
            T.StringType(),
            comment="Franchise coordinates as an EWKT point string: SRID=4326;POINT(lon lat).",
        ),
        Column(
            "supplier_key",
            T.LongType(),
            comment=(
                "The franchise's ingredient supplier: the supplier currently holding the franchise's supplier_id. "
                "-1 (unknown member) when none does."
            ),
        ),
        Column(
            "supplier_id",
            T.LongType(),
            comment="The supplier's ERP id as recorded on the franchise, kept even when no supplier matches.",
        ),
        Column("supplier_name", T.StringType()),
        Column(
            "supplier_material_name",
            T.StringType(),
            comment="The ingredient the supplier provides.",
        ),
        Column("supplier_continent", T.StringType()),
        Column("supplier_city", T.StringType()),
        Column("is_supplier_approved", T.BooleanType()),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="franchise_key",
    foreign_keys=[
        ForeignKey.to(dim_geography, "country_name", name="fk_dim_franchise_geography"),
        ForeignKey.to(dim_supplier, "supplier_key", name="fk_dim_franchise_supplier"),
    ],
)


dim_payment_method = TableSchema(
    catalog="gold",
    schema="conformed",
    table="dim_payment_method",
    comment="One row per payment method seen on transactions.",
    tags={"domain": "sales", "model_type": "dimension"},
    columns=[
        Column(
            "payment_method_code",
            T.StringType(),
            comment="Lower-cased, trimmed payment method from the ERP; the natural key and the join key from fct_sales.",
        ),
        Column(
            "payment_method_name",
            T.StringType(),
            comment="Display name, e.g. American Express.",
        ),
        Column(
            "payment_method_type",
            T.StringType(),
            comment="Card for known card networks; Other for anything not yet mapped.",
        ),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="payment_method_code",
)


dim_product = TableSchema(
    catalog="gold",
    schema="conformed",
    table="dim_product",
    comment="One row per product sold.",
    tags={"domain": "product", "model_type": "dimension"},
    columns=[
        Column(
            "product_name",
            T.StringType(),
            comment="Product name as it appears on transactions; the natural key.",
        ),
        Column(
            "product_sk",
            T.StringType(),
            comment="silver.product.product's hash key, kept so silver transactions (which carry it) can resolve product_name.",
        ),
        Column(
            "product_unit_price",
            T.DecimalType(18, 6),
            comment="Highest unit price ever charged for the product.",
        ),
        Column(
            "product_first_sold_date",
            T.DateType(),
            comment="Date of the product's first transaction. The ERP has no product master, so this is not a launch date.",
        ),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="product_name",
)
