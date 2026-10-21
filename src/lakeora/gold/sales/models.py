"""Table definitions for gold.sales, kept free of pipeline code so other pipeline files can import them without registering a dataset twice."""

import pyspark.sql.types as T

from lakeora.gold.conformed.models import (
    dim_customer,
    dim_date,
    dim_franchise,
    dim_payment_method,
    dim_product,
)
from lakeora_core.table_schema import Column, ForeignKey, TableSchema

fct_sales = TableSchema(
    catalog="gold",
    schema="sales",
    table="fct_sales",
    comment=(
        "Sales fact, one row per transaction (one product line), keyed to the conformed dimensions. "
        "Streamed from silver and upserted on transaction_id; dimension keys are resolved when a row is processed."
    ),
    tags={"domain": "sales", "model_type": "fact"},
    columns=[
        Column(
            "transaction_id",
            T.LongType(),
            comment="Natural key from the ERP (transactionID).",
        ),
        Column("date_key", T.IntegerType(), comment="The transaction date as yyyymmdd."),
        Column(
            "customer_key",
            T.LongType(),
            comment=(
                "The buying customer as they were at the sale: the dim_customer version whose validity window "
                "holds the sale date for the transaction's customer_id. -1 (unknown member) when none matches."
            ),
        ),
        Column(
            "customer_durable_key",
            T.LongType(),
            nullable=False,
            comment=(
                "The buying customer across all their versions (dim_customer.customer_durable_key); -1 when "
                "unresolved. Count distinct on this, not customer_key, so a customer who changed is counted once."
            ),
        ),
        Column(
            "franchise_key",
            T.LongType(),
            comment=(
                "The franchise where the sale happened: the dim_franchise row whose validity window holds the sale "
                "date for the transaction's franchise_id. -1 (unknown member) when none matches."
            ),
        ),
        Column(
            "product_name",
            T.StringType(),
            comment="The product sold, the key into dim_product.",
        ),
        Column(
            "payment_method_code",
            T.StringType(),
            comment="How the customer paid, the key into dim_payment_method.",
        ),
        Column("transaction_datetime", T.TimestampType()),
        Column("quantity", T.LongType(), comment="Units sold."),
        Column("unit_price", T.DecimalType(18, 6)),
        Column(
            "total_price",
            T.DecimalType(18, 6),
            comment="Line revenue (quantity x unit price) as recorded by the ERP.",
        ),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="transaction_id",
    foreign_keys=[
        ForeignKey.to(dim_date, "date_key", name="fk_fct_sales_date"),
        ForeignKey.to(dim_customer, "customer_key", name="fk_fct_sales_customer"),
        ForeignKey.to(dim_franchise, "franchise_key", name="fk_fct_sales_franchise"),
        ForeignKey.to(dim_product, "product_name", name="fk_fct_sales_product"),
        ForeignKey.to(
            dim_payment_method,
            "payment_method_code",
            name="fk_fct_sales_payment_method",
        ),
    ],
)
