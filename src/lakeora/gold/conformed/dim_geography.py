import pyspark.sql.functions as F
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.bronze.seeds.models import countries
from lakeora.gold.conformed.models import dim_geography
from lakeora.silver.branch.models import franchise
from lakeora.silver.customer.models import silver_customer


@dp.materialized_view(
    name=dim_geography.full_name,
    comment=dim_geography.comment,
    schema=dim_geography.ddl(),
)
def load_gold_conformed_dim_geography():
    # every country spelling the ERP uses, for customers and franchises
    erp = (
        spark.table(silver_customer.full_name)
        .select(F.col("customer_country").alias("erp_country"))
        .union(spark.table(franchise.full_name).select("franchise_country"))
        .where("erp_country IS NOT NULL")
        .distinct()
    )
    seed = spark.table(countries.full_name)
    df = (
        erp.join(seed, erp.erp_country == seed.country_from_erp, "left")
        .select(
            # spellings of one country (US, USA) collapse onto its standard name
            F.coalesce(seed.country_name, erp.erp_country).alias("country_name"),
            F.col("iso2_code").alias("country_iso2_code"),
            F.col("iso3_code").alias("country_iso3_code"),
            F.col("subregion").alias("country_subregion"),
            F.col("continent").alias("country_continent"),
        )
        .distinct()
        .withColumn("md_loaded_at_ts", F.current_timestamp())
    )
    return dim_geography.conform(df)
