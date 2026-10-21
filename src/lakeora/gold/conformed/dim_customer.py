import datetime

import pyspark.sql.functions as F
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import Window

from lakeora.gold.conformed.common import with_country_name
from lakeora.gold.conformed.models import dim_customer
from lakeora.silver.core.models import state
from lakeora.silver.customer.models import silver_customer
from lakeora_core.dimensions import unknown_member
from lakeora_core.keys import entity_key, version_key


@dp.materialized_view(name=dim_customer.full_name, comment=dim_customer.comment, schema=dim_customer.ddl())
@dp.expect_all_or_fail(
    {
        "customer_id_not_null": "customer_id IS NOT NULL",
        "customer_durable_key_not_null": "customer_durable_key IS NOT NULL",
    }
)
def load_gold_conformed_dim_customer():
    # versions of one customer in history order; ties on a valid_from day put the current version last and
    # break the rest on silver's version hash, so numbering is deterministic and only ever appends
    by_customer = Window.partitionBy("customer_id", "customer_created_date").orderBy(
        "customer_valid_from", "customer_is_current", "customer_sk"
    )
    # one row per full state name (the seed lists each state under its code and its name)
    states = (
        spark.table(state.full_name)
        .select(
            F.col("state_state_name").alias("_state_name"),
            F.col("state_state_code").alias("customer_state_code"),
            F.col("state_type").alias("customer_state_type"),
            F.col("state_census_region").alias("customer_census_region"),
            F.col("state_census_division").alias("customer_census_division"),
            F.col("state_is_contiguous_us").alias("customer_is_contiguous_us"),
        )
        .distinct()
    )
    customers = spark.table(silver_customer.full_name).withColumn(
        "customer_durable_key", entity_key("customer_id", "customer_created_date")
    )
    df = (
        with_country_name(spark, customers, "customer_country")
        .join(states, F.col("customer_state") == states._state_name, "left")
        .withColumn(
            "customer_key",
            version_key("customer_durable_key", F.row_number().over(by_customer)),
        )
        .withColumn("md_loaded_at_ts", F.current_timestamp())
    )
    unknown = unknown_member(
        spark,
        dim_customer,
        customer_key=-1,
        customer_durable_key=-1,
        customer_id=-1,
        customer_valid_from=datetime.date(1900, 1, 1),
        customer_is_current=True,
        customer_full_name="Unknown",
    )
    return unknown.unionByName(dim_customer.conform(df)).withColumn("md_loaded_at_ts", F.current_timestamp())
