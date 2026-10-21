import pyspark.sql.functions as F
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import Window

from lakeora.gold.conformed.models import dim_customer
from lakeora.gold.mart.common import as_of_ts, latest_transaction_ts, ratio, trend_pct, trend_revenue
from lakeora.gold.mart.models import (
    CHURN_RISKS,
    RFM_SEGMENTS,
    VALUE_TIERS,
    customer_value,
)
from lakeora.gold.sales.models import fct_sales

# frequency-monetary decides value, recency splits the valuable ones
#            FM >= 2.5     FM < 2.5
# R >= 4     Champions     Low Value
# R <= 3     At Risk       Low Value
def _rfm_segment(recency, fm):
    return F.when(fm < 2.5, "Low Value").when(recency >= 4, "Champions").otherwise("At Risk")


@dp.materialized_view(
    name=customer_value.full_name,
    comment=customer_value.comment,
    schema=customer_value.ddl(),
)
@dp.expect_all_or_fail(
    {
        "value_tier_accepted": f"value_tier IN {tuple(VALUE_TIERS)}",
        "rfm_segment_accepted": f"rfm_segment IN {tuple(RFM_SEGMENTS)}",
        "churn_risk_accepted": f"churn_risk IN {tuple(CHURN_RISKS)}",
    }
)
def load_gold_mart_customer_value():
    sales = spark.table(fct_sales.full_name)
    as_of = latest_transaction_ts(sales)
    ts = F.col("transaction_datetime")
    days = lambda start, end: F.expr(f"timestampdiff(SECOND, {start}, {end}) / 86400.0")

    per_customer = (
        sales.groupBy("customer_durable_key")
        .agg(
            F.countDistinct("transaction_id").alias("order_count"),
            F.sum("quantity").alias("units_purchased"),
            F.sum("total_price").alias("total_revenue"),
            F.min(ts).alias("first_purchase_ts"),
            F.max(ts).alias("last_purchase_ts"),
            F.countDistinct(F.to_date(ts)).alias("active_days"),
            F.countDistinct("franchise_key").alias("distinct_franchises"),
            F.countDistinct("product_name").alias("distinct_products"),
            *trend_revenue(as_of),
        )
        .withColumn("as_of_ts", as_of_ts(as_of))
        .withColumn("avg_order_value", ratio("total_revenue", "order_count"))
        .withColumn("tenure_days", days("first_purchase_ts", "last_purchase_ts"))
        .withColumn("days_since_last_purchase", days("last_purchase_ts", "as_of_ts"))
        # (last - first purchase) / (orders - 1): the average gap between purchases
        .withColumn("avg_days_between_purchases", ratio("tenure_days", F.col("order_count") - 1))
    )

    everyone = Window.partitionBy()
    by_revenue = Window.orderBy(F.desc("total_revenue"), "customer_durable_key")
    quintile = lambda col: F.ceil(F.cume_dist().over(Window.orderBy(col)) * 5).cast("int")
    scored = (
        per_customer.withColumn("revenue_rank", F.row_number().over(by_revenue))
        .withColumn(
            "revenue_share",
            ratio("total_revenue", F.sum("total_revenue").over(everyone)),
        )
        .withColumn(
            "cumulative_revenue_share",
            ratio(
                F.sum("total_revenue").over(by_revenue.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
                F.sum("total_revenue").over(everyone),
            ),
        )
        # RFM quintiles, 5 = best; cume_dist keeps tied customers in the same bucket
        .withColumn("recency_score", quintile("last_purchase_ts"))
        .withColumn("frequency_score", quintile("order_count"))
        .withColumn("monetary_score", quintile("total_revenue"))
        # a one-time buyer has no rhythm of their own, so judge them by the typical customer's
        .withColumn(
            "expected_days_between_purchases",
            F.coalesce(
                "avg_days_between_purchases",
                F.percentile_approx("avg_days_between_purchases", 0.5).over(everyone),
            ),
        )
        # how many of their usual gaps have passed since they last bought
        .withColumn(
            "purchase_overdue_ratio",
            ratio("days_since_last_purchase", "expected_days_between_purchases"),
        )
        .withColumn("fm_score", (F.col("frequency_score") + F.col("monetary_score")) / 2.0)
        # revenue before this customer: A until the first 50%, B until 80%
        .withColumn(
            "revenue_ahead_share",
            F.col("cumulative_revenue_share") - F.col("revenue_share"),
        )
    )

    # every current customer, also those who never bought
    customers = spark.table(dim_customer.full_name).where("customer_is_current").select("customer_durable_key")
    df = customers.join(scored, "customer_durable_key", "left")
    bought = F.col("order_count").isNotNull()
    overdue, ahead = F.col("purchase_overdue_ratio"), F.col("revenue_ahead_share")
    recent, prior = F.col("revenue_recent_window"), F.col("revenue_prior_window")
    df = df.select(
        "customer_durable_key",
        F.coalesce("total_revenue", F.lit(0)).alias("total_revenue"),
        F.coalesce("order_count", F.lit(0)).alias("order_count"),
        F.coalesce("units_purchased", F.lit(0)).alias("units_purchased"),
        "avg_order_value",
        "revenue_rank",
        "revenue_share",
        "cumulative_revenue_share",
        F.when(~bought, "No Purchases")
        .when(ahead < 0.5, "A")
        .when(ahead < 0.8, "B")
        .otherwise("C")
        .alias("value_tier"),
        "first_purchase_ts",
        "last_purchase_ts",
        "tenure_days",
        F.coalesce("active_days", F.lit(0)).alias("active_days"),
        F.coalesce("distinct_franchises", F.lit(0)).alias("distinct_franchises"),
        F.coalesce("distinct_products", F.lit(0)).alias("distinct_products"),
        "recency_score",
        "frequency_score",
        "monetary_score",
        F.concat("recency_score", "frequency_score", "monetary_score").alias("rfm_code"),
        F.when(~bought, "No Purchases")
        .otherwise(_rfm_segment(F.col("recency_score"), F.col("fm_score")))
        .alias("rfm_segment"),
        "as_of_ts",
        "days_since_last_purchase",
        "avg_days_between_purchases",
        "purchase_overdue_ratio",
        F.coalesce(recent, F.lit(0)).alias("revenue_recent_window"),
        F.coalesce(prior, F.lit(0)).alias("revenue_prior_window"),
        trend_pct("revenue_recent_window", "revenue_prior_window").alias("revenue_trend_pct"),
        F.when(~bought, "No Purchases")
        .when(overdue >= 3, "Lapsed")
        .when(overdue >= 1.5, "At Risk")
        .otherwise("Active")
        .alias("churn_risk"),
        # the list to act on: valuable customers (tier A/B or high FM) who are overdue or whose spend has halved;
        # someone who didn't buy in the prior window can't have halved
        F.coalesce(
            ((ahead < 0.8) | (F.col("fm_score") >= 4)) & ((overdue >= 1.5) | ((prior > 0) & (recent <= 0.5 * prior))),
            F.lit(False),
        ).alias("is_slipping_away"),
        F.current_timestamp().alias("md_loaded_at_ts"),
    )
    return customer_value.conform(df)
