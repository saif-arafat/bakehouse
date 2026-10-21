from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.bronze.seeds.models import us_states
from lakeora_core.seeds import read_seed


@dp.materialized_view(name=us_states.full_name, comment=us_states.comment, schema=us_states.ddl())
def load_bronze_seeds_us_states():
    return read_seed(spark, us_states, "us_states.csv")
