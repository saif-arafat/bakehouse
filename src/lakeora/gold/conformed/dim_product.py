import pyspark.sql.functions as F
import pyspark.sql.types as T
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.gold.conformed.models import dim_product
from lakeora.silver.product.models import product


@dp.materialized_view(name=dim_product.full_name, comment=dim_product.comment, schema=dim_product.ddl())
def load_gold_conformed_dim_product():
    df = spark.table(product.full_name).select(
        "product_name",
        "product_sk",
        "product_unit_price",
        F.col("product_release_date").cast(T.DateType()).alias("product_first_sold_date"),
        F.current_timestamp().alias("md_loaded_at_ts"),
    )
    return dim_product.conform(df)
