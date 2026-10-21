import pyspark.sql.functions as F
import pyspark.sql.types as T
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import Window

from lakeora.bronze.erp.models import brz_erp__customer_history
from lakeora.bronze.seeds.models import us_states
from lakeora.silver.customer.models import silver_customer
from lakeora_core.keys import surrogate_key


@dp.materialized_view(
    name=silver_customer.full_name,
    comment=silver_customer.comment,
    schema=silver_customer.ddl(),
)
@dp.expect_all_or_fail(
    {
        "customer_id_not_null": "customer_id IS NOT NULL",
        "customer_durable_sk_not_null": "customer_durable_sk IS NOT NULL",
    }
)
def load_silver_customer_customer():
    by_id = Window.partitionBy("customerid").orderBy("created_date", "__START_AT")
    by_customer = Window.partitionBy("customerid", "created_date").orderBy("__START_AT")
    states = spark.table(us_states.full_name).select("state_from_erp", "state_name")

    versioned = spark.table(brz_erp__customer_history.full_name).withColumn(
        "valid_from",
        # 2 main changes
        # 1. id gets reused for a different person (this changes the created_date from erp)
        # 2. a person changes their details (this keeps the created_date)
        # base case first version takes 1900
        F.when(F.row_number().over(by_id) == 1, F.lit("1900-01-01").cast(T.DateType()))
        # change 1 where new person takes id
        .when(F.row_number().over(by_customer) == 1, F.col("created_date"))
        # change 2 where its same person but new details
        .otherwise(F.col("__START_AT").cast(T.DateType())),
    )

    df = versioned.join(states, versioned.state == states.state_from_erp, "left").select(
        surrogate_key("customerid", "created_date", "__START_AT").alias("customer_sk"),
        surrogate_key("customerid", "created_date").alias("customer_durable_sk"),
        F.col("customerid").alias("customer_id"),
        F.col("created_date").alias("customer_created_date"),
        F.col("valid_from").alias("customer_valid_from"),
        F.lead("valid_from").over(by_id).alias("customer_valid_to"),
        F.col("__END_AT").isNull().alias("customer_is_current"),
        F.concat_ws(" ", "first_name", "last_name").alias("customer_full_name"),
        F.col("first_name").alias("customer_first_name"),
        F.col("last_name").alias("customer_last_name"),
        F.col("email_address").alias("customer_email"),
        F.regexp_replace(F.regexp_replace("phone_number", "x.*$", ""), "[^0-9]", "")
        .cast(T.LongType())
        .alias("customer_phone_number"),
        F.col("phone_number").alias("raw_phone_number"),
        F.col("address").alias("customer_address"),
        F.col("city").alias("customer_city"),
        F.col("state_name").alias("customer_state"),
        F.col("country").alias("customer_country"),
        F.col("continent").alias("customer_continent"),
        F.col("postal_zip_code").cast(T.IntegerType()).alias("customer_postal_code"),
        F.lower(F.trim("gender")).alias("customer_gender"),
        F.current_timestamp().alias("md_loaded_at_ts"),
    )
    return silver_customer.conform(df)
