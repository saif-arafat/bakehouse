import pyspark.sql.functions as F
import pyspark.sql.types as T
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.bronze.erp.models import brz_erp__transaction
from lakeora.silver.product.models import product
from lakeora_core.keys import surrogate_key


@dp.materialized_view(name=product.full_name, comment=product.comment, schema=product.ddl())
@dp.expect_or_fail("product_name_not_null", "product_name IS NOT NULL")
def load_silver_product_product():
    df = (
        spark.table(brz_erp__transaction.full_name)
        .groupBy("product")
        .agg(
            F.max(F.col("unitprice").cast(T.DecimalType(18, 6))).alias("product_unit_price"),
            F.min("datetime").alias("product_release_date"),
        )
        .select(
            surrogate_key("product").alias("product_sk"),
            F.col("product").alias("product_name"),
            "product_unit_price",
            "product_release_date",
            F.current_timestamp().alias("md_loaded_at_ts"),
        )
    )
    return product.conform(df)
