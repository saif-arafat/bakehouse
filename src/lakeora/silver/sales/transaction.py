import pyspark.sql.functions as F
import pyspark.sql.types as T
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.bronze.erp.models import brz_erp__transaction
from lakeora.silver.sales.models import TRANSACTION_CHANGES_VIEW, transaction
from lakeora_core.keys import surrogate_key


@dp.temporary_view(name=TRANSACTION_CHANGES_VIEW)
@dp.expect_or_fail("transaction_id_not_null", "transaction_id IS NOT NULL")
def silver_sales_transaction_changes():
    df = spark.readStream.table(brz_erp__transaction.full_name).select(
        # only the id: a late row for the same transaction must keep its key
        surrogate_key("transactionid").alias("transaction_sk"),
        F.col("transactionid").alias("transaction_id"),
        F.col("customerid").alias("customer_id"),
        F.col("franchiseid").alias("franchise_id"),
        # same hash as silver product's product_sk
        surrogate_key("product").alias("product_sk"),
        F.col("datetime").alias("transaction_datetime"),
        F.col("quantity").alias("transaction_quantity"),
        F.col("unitprice").cast(T.DecimalType(18, 6)).alias("transaction_unit_price"),
        F.col("totalprice").cast(T.DecimalType(18, 6)).alias("transaction_total_price"),
        F.col("paymentmethod").alias("transaction_payment_method"),
        F.col("cardnumber").alias("transaction_card_number"),
        F.current_timestamp().alias("md_loaded_at_ts"),
    )
    return transaction.conform(df)


dp.create_streaming_table(name=transaction.full_name, comment=transaction.comment, schema=transaction.ddl())

dp.create_auto_cdc_flow(
    target=transaction.full_name,
    source=TRANSACTION_CHANGES_VIEW,
    keys=["transaction_id"],
    sequence_by="md_loaded_at_ts",
    stored_as_scd_type=1,
)
