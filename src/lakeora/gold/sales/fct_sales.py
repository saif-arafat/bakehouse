import pyspark.sql.functions as F
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.gold.conformed.models import dim_customer, dim_franchise, dim_product
from lakeora.gold.sales.models import fct_sales
from lakeora.silver.sales.models import TRANSACTION_CHANGES_VIEW
from lakeora_core.dimensions import lookup_as_of
from lakeora_core.keys import date_key

# A streaming fact (per the Databricks dimensional-modeling guide) fed by silver's append-only changes view,
# and upserted on transaction_id like silver, so a re-sent transaction replaces its row instead of adding one.
# Keys are resolved against the dimensions as each row is processed: dates and payment methods are natural
# keys computed from the row, customers and franchises are looked up as of the sale date.


@dp.temporary_view(name="gold_sales_fct_sales_changes")
@dp.expect_or_fail("transaction_id_not_null", "transaction_id IS NOT NULL")
@dp.expect("customer_resolved", "customer_key <> -1")
@dp.expect("franchise_resolved", "franchise_key <> -1")
@dp.expect("product_resolved", "product_name IS NOT NULL")
def gold_sales_fct_sales_changes():
    sale_date = F.to_date("transaction_datetime")
    sales = spark.readStream.table(TRANSACTION_CHANGES_VIEW)
    sales = lookup_as_of(
        sales,
        spark.table(dim_customer.full_name),
        on=("customer_id", "customer_id"),
        at=sale_date,
        keys={
            "customer_key": "customer_key",
            "customer_durable_key": "customer_durable_key",
        },
        valid_from="customer_valid_from",
        valid_to="customer_valid_to",
    )
    sales = lookup_as_of(
        sales,
        spark.table(dim_franchise.full_name),
        on=("franchise_id", "franchise_id"),
        at=sale_date,
        keys={"franchise_key": "franchise_key"},
        valid_from="franchise_valid_from",
        valid_to="franchise_valid_to",
    )
    # silver carries the product's hash key; the dimension is keyed on the name
    products = spark.table(dim_product.full_name).select(F.col("product_sk").alias("_product_sk"), "product_name")
    df = sales.join(products, sales.product_sk == products._product_sk, "left").select(
        "transaction_id",
        date_key(sale_date).alias("date_key"),
        "customer_key",
        "customer_durable_key",
        "franchise_key",
        "product_name",
        F.lower(F.trim("transaction_payment_method")).alias("payment_method_code"),
        "transaction_datetime",
        F.col("transaction_quantity").alias("quantity"),
        F.col("transaction_unit_price").alias("unit_price"),
        F.col("transaction_total_price").alias("total_price"),
        F.current_timestamp().alias("md_loaded_at_ts"),
    )
    return fct_sales.conform(df)


dp.create_streaming_table(name=fct_sales.full_name, comment=fct_sales.comment, schema=fct_sales.ddl())

dp.create_auto_cdc_flow(
    target=fct_sales.full_name,
    source="gold_sales_fct_sales_changes",
    keys=["transaction_id"],
    sequence_by="md_loaded_at_ts",
    stored_as_scd_type=1,
)
