"""Table definitions for silver.sales, kept free of pipeline code so other pipeline files can import them without registering a dataset twice."""

import pyspark.sql.types as T

from lakeora.silver.product.models import product
from lakeora_core.table_schema import Column, ForeignKey, TableSchema

transaction = TableSchema(
    catalog="silver",
    schema="sales",
    table="transaction",
    comment=(
        "One row per ERP sales transaction (one product line per transaction). "
        "Upserted on transaction_id by Auto CDC, so late-arriving or re-sent rows replace the earlier version. "
        "The product key is a hash of the product name; customer and franchise stay as ERP ids and are "
        "resolved to their keys by sale date in fct_sales."
    ),
    tags={"domain": "sales", "source_system": "erp", "contains_pii": "true"},
    columns=[
        Column(
            "transaction_sk",
            T.StringType(),
            comment="Surrogate key, hash of transaction_id.",
        ),
        Column(
            "transaction_id",
            T.LongType(),
            nullable=False,
            comment="Natural key from the ERP (transactionID); the Auto CDC key.",
        ),
        Column(
            "customer_id",
            T.LongType(),
            comment=(
                "The buying customer's ERP id. Not a key on its own (ids can be reused), so fct_sales "
                "resolves it to customer_sk using the sale date."
            ),
        ),
        Column(
            "franchise_id",
            T.LongType(),
            comment=(
                "The ERP id of the franchise where the sale happened. Not a key on its own (ids can be reused), "
                "so fct_sales resolves it to franchise_sk using the sale date."
            ),
        ),
        Column("product_sk", T.StringType(), comment="The product sold."),
        Column("transaction_datetime", T.TimestampType()),
        Column("transaction_quantity", T.LongType()),
        Column("transaction_unit_price", T.DecimalType(18, 6)),
        Column(
            "transaction_total_price",
            T.DecimalType(18, 6),
            comment="Quantity x unit price, as recorded by the ERP.",
        ),
        Column(
            "transaction_payment_method",
            T.StringType(),
            comment="Card network as entered in the ERP (e.g. visa, amex); standardised in gold dim_payment_method.",
        ),
        Column(
            "transaction_card_number",
            T.LongType(),
            comment="Payment card number.",
            tags={"pii": "payment_card"},
        ),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="transaction_sk",
    foreign_keys=[ForeignKey.to(product, "product_sk", name="fk_transaction_product")],
)

# the cleaned, append-only stream of bronze transactions that feeds silver transaction's Auto CDC flow;
# gold fct_sales streams from it too, since the CDC target itself has MERGE commits a stream can't read
TRANSACTION_CHANGES_VIEW = "silver_sales_transaction_changes"
