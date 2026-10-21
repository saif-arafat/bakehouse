import pyspark.sql.functions as F
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.gold.conformed.models import dim_payment_method
from lakeora.silver.sales.models import transaction

# display names of the card networks; anything else is "Other", named by its initcapped code
CARD_NETWORKS = {"amex": "American Express", "mastercard": "Mastercard", "visa": "Visa"}


@dp.materialized_view(
    name=dim_payment_method.full_name,
    comment=dim_payment_method.comment,
    schema=dim_payment_method.ddl(),
)
@dp.expect_or_fail("payment_method_type_accepted", "payment_method_type IN ('Card', 'Other')")
def load_gold_conformed_dim_payment_method():
    names = F.create_map(*[F.lit(x) for kv in CARD_NETWORKS.items() for x in kv])
    code = F.col("payment_method_code")
    df = (
        spark.table(transaction.full_name)
        .select(F.lower(F.trim("transaction_payment_method")).alias("payment_method_code"))
        .where("payment_method_code IS NOT NULL")
        .distinct()
        .select(
            code,
            F.coalesce(names[code], F.initcap(code)).alias("payment_method_name"),
            F.when(code.isin(*CARD_NETWORKS), "Card").otherwise("Other").alias("payment_method_type"),
            F.current_timestamp().alias("md_loaded_at_ts"),
        )
    )
    return dim_payment_method.conform(df)
