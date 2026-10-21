import pyspark.sql.functions as F
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import Window

from lakeora.gold.conformed.models import dim_franchise
from lakeora.gold.feedback.models import fct_review
from lakeora.gold.mart.common import as_of_ts, latest_transaction_ts, ratio, trend_pct, trend_revenue
from lakeora.gold.mart.models import (
    EXPANSION_CASES,
    PERFORMANCE_TIERS,
    franchise_performance,
)
from lakeora.gold.sales.models import fct_sales


@dp.materialized_view(
    name=franchise_performance.full_name,
    comment=franchise_performance.comment,
    schema=franchise_performance.ddl(),
)
@dp.expect_all_or_fail(
    {
        "performance_tier_accepted": f"performance_tier IN {tuple(PERFORMANCE_TIERS)}",
        "expansion_case_accepted": f"expansion_case IN {tuple(EXPANSION_CASES)}",
        "is_underperforming_not_null": "is_underperforming IS NOT NULL",
    }
)
def load_gold_mart_franchise_performance():
    sales = spark.table(fct_sales.full_name)
    as_of = latest_transaction_ts(sales)
    # the unknown member isn't a real branch and would drag down the network-wide averages
    franchises = (
        spark.table(dim_franchise.full_name)
        .where("franchise_key <> -1")
        .select("franchise_key", "country_name", "franchise_size", "franchise_city")
    )

    per_franchise = sales.groupBy("franchise_key").agg(
        F.countDistinct("transaction_id").alias("order_count"),
        F.sum("quantity").alias("units_sold"),
        F.sum("total_price").alias("total_revenue"),
        F.countDistinct("customer_durable_key").alias("distinct_customers"),
        F.countDistinct(F.to_date("transaction_datetime")).alias("active_days"),
        *trend_revenue(as_of),
    )
    # share of a franchise's customers who came back to it
    repeat = (
        sales.groupBy("franchise_key", "customer_durable_key")
        .agg(F.countDistinct("transaction_id").alias("orders"))
        .groupBy("franchise_key")
        .agg(F.avg(F.when(F.col("orders") > 1, 1.0).otherwise(0.0)).alias("repeat_customer_rate"))
    )
    reviews = (
        spark.table(fct_review.full_name)
        .groupBy("franchise_key")
        .agg(
            F.count("*").alias("review_count"),
            F.sum(F.col("has_star_rating").cast("int")).alias("rated_review_count"),
            F.avg("star_rating").alias("avg_star_rating"),
        )
    )
    country_customers = (
        sales.join(franchises, "franchise_key")
        .groupBy("country_name")
        .agg(F.countDistinct("customer_durable_key").alias("country_distinct_customers"))
    )
    # every franchise, with sales and reviews where they exist
    zero = lambda c: F.coalesce(c, F.lit(0)).alias(c)
    base = (
        franchises.join(per_franchise, "franchise_key", "left")
        .join(repeat, "franchise_key", "left")
        .join(reviews, "franchise_key", "left")
        .select(
            "*",
            F.col("order_count").isNotNull().alias("has_sales"),
        )
        .withColumn("as_of_ts", as_of_ts(as_of))
        .select(
            "franchise_key",
            "country_name",
            "franchise_size",
            "franchise_city",
            "has_sales",
            "as_of_ts",
            "repeat_customer_rate",
            "avg_star_rating",
            *map(
                zero,
                [
                    "total_revenue",
                    "order_count",
                    "units_sold",
                    "distinct_customers",
                    "active_days",
                    "revenue_recent_window",
                    "revenue_prior_window",
                    "review_count",
                    "rated_review_count",
                ],
            ),
        )
    )

    network = Window.partitionBy()
    country = Window.partitionBy("country_name")
    median = lambda w: F.percentile_approx("total_revenue", 0.5).over(w)
    by_revenue = Window.orderBy("total_revenue")
    scored = (
        base.withColumn(
            "revenue_rank",
            F.row_number().over(Window.orderBy(F.desc("total_revenue"), "franchise_key")),
        )
        .withColumn(
            "revenue_share",
            ratio("total_revenue", F.sum("total_revenue").over(network)),
        )
        .withColumn("revenue_percentile", F.cume_dist().over(by_revenue))
        # against the median franchise, and against the median of its own size class (an S store isn't an XXL)
        .withColumn("revenue_index", ratio("total_revenue", median(network)))
        .withColumn(
            "revenue_vs_size_peers_index",
            ratio("total_revenue", median(Window.partitionBy("franchise_size"))),
        )
        .withColumn("country_franchise_count", F.count("*").over(country))
        .withColumn(
            "city_franchise_count",
            F.count("*").over(Window.partitionBy("country_name", "franchise_city")),
        )
        .withColumn("country_revenue", F.sum("total_revenue").over(country))
        .withColumn(
            "country_revenue_recent_window",
            F.sum("revenue_recent_window").over(country),
        )
        .withColumn("country_revenue_prior_window", F.sum("revenue_prior_window").over(country))
        .withColumn(
            "network_revenue_per_franchise",
            F.sum("total_revenue").over(network) / F.count("*").over(network),
        )
        # weighted by rated reviews, so a branch with one rating doesn't count as much as one with fifty
        .withColumn(
            "country_avg_star_rating",
            ratio(
                F.sum(F.col("avg_star_rating") * F.col("rated_review_count")).over(country),
                F.sum("rated_review_count").over(country),
            ),
        )
        .join(country_customers, "country_name", "left")
        .withColumn(
            "country_revenue_per_franchise",
            F.col("country_revenue") / F.col("country_franchise_count"),
        )
        .withColumn(
            "country_demand_index",
            ratio("country_revenue_per_franchise", "network_revenue_per_franchise"),
        )
        .withColumn(
            "country_revenue_trend_pct",
            trend_pct("country_revenue_recent_window", "country_revenue_prior_window"),
        )
        .withColumn(
            "performance_tier",
            F.when(~F.col("has_sales"), "No Sales")
            .when(F.col("revenue_percentile") > 0.75, "Leading")
            .when(F.col("revenue_percentile") > 0.25, "On Par")
            .otherwise("Lagging"),
        )
        # weak, not just small: bottom quartile (or no sales) and below the median of its size class
        .withColumn(
            "is_underperforming",
            F.col("performance_tier").isin("Lagging", "No Sales")
            & (F.coalesce("revenue_vs_size_peers_index", F.lit(0)) < 1),
        )
    )

    # one action per franchise, first match wins; a missing trend counts as flat and a missing rating as
    # 3 stars, so no data doesn't block a franchise
    expansion_case = (
        F.when(F.col("is_underperforming"), "Fix First")
        # each branch in the country already outsells the network, this one included, and the market isn't collapsing
        .when(
            (F.col("country_demand_index") >= 1.2)
            & (F.col("revenue_index") >= 1)
            & (F.coalesce("country_revenue_trend_pct", F.lit(0)) > -0.5)
            & (F.coalesce("country_avg_star_rating", F.lit(3)) >= 3),
            "Expand Market",
        )
        # far busier than stores of the same size: demand is outgrowing the footprint
        .when(
            (F.col("revenue_vs_size_peers_index") >= 1.5) & (F.col("city_franchise_count") == 1),
            "Capacity Strain",
        )
        .otherwise("Hold")
    )
    df = scored.select(
        "franchise_key",
        "country_name",
        "total_revenue",
        "order_count",
        "units_sold",
        ratio("total_revenue", "order_count").alias("avg_order_value"),
        "distinct_customers",
        "repeat_customer_rate",
        "active_days",
        ratio("total_revenue", "active_days").alias("revenue_per_active_day"),
        "revenue_rank",
        "revenue_share",
        "revenue_index",
        "revenue_vs_size_peers_index",
        "review_count",
        "rated_review_count",
        "avg_star_rating",
        "as_of_ts",
        "revenue_recent_window",
        "revenue_prior_window",
        trend_pct("revenue_recent_window", "revenue_prior_window").alias("revenue_trend_pct"),
        "performance_tier",
        "is_underperforming",
        "country_franchise_count",
        "city_franchise_count",
        "country_revenue",
        ratio("total_revenue", "country_revenue").alias("share_of_country_revenue"),
        "country_revenue_per_franchise",
        "country_demand_index",
        "country_distinct_customers",
        "country_avg_star_rating",
        "country_revenue_trend_pct",
        expansion_case.alias("expansion_case"),
        F.current_timestamp().alias("md_loaded_at_ts"),
    )
    return franchise_performance.conform(df)
