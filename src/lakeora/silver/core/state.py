import pyspark.sql.functions as F
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.bronze.seeds.models import us_states
from lakeora.silver.core.models import state
from lakeora_core.keys import surrogate_key


@dp.materialized_view(name=state.full_name, comment=state.comment, schema=state.ddl())
@dp.expect_or_fail("state_from_erp_not_null", "state_from_erp IS NOT NULL")
def load_silver_core_state():
    # every seed column except the join key gets a state_ prefix
    attributes = [c for c in us_states.column_names if c != "state_from_erp"]
    df = spark.table(us_states.full_name).select(
        surrogate_key("state_from_erp").alias("state_sk"),
        "state_from_erp",
        *[F.col(c).alias(f"state_{c}") for c in attributes],
        F.current_timestamp().alias("md_loaded_at_ts"),
    )
    return state.conform(df)
