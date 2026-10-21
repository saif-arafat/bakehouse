from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.bronze.seeds.models import countries
from lakeora_core.seeds import read_seed


@dp.materialized_view(name=countries.full_name, comment=countries.comment, schema=countries.ddl())
def load_bronze_seeds_countries():
    return read_seed(spark, countries, "countries.csv")
