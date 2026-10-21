"""Table definitions for gold.mart, kept free of pipeline code so other pipeline files can import them without registering a dataset twice."""

import pyspark.sql.types as T

from lakeora.gold.conformed.models import dim_franchise, dim_geography
from lakeora_core.table_schema import Column, ForeignKey, TableSchema

# revenue_recent_window covers the last N days before as_of_ts, revenue_prior_window the N days before that
TREND_WINDOW_DAYS = 7

VALUE_TIERS = ["A", "B", "C", "No Purchases"]
RFM_SEGMENTS = ["Champions", "At Risk", "Low Value", "No Purchases"]
CHURN_RISKS = ["Active", "At Risk", "Lapsed", "No Purchases"]
PERFORMANCE_TIERS = ["Leading", "On Par", "Lagging", "No Sales"]
EXPANSION_CASES = ["Fix First", "Expand Market", "Capacity Strain", "Hold"]


customer_value = TableSchema(
    catalog="gold",
    schema="mart",
    table="customer_value",
    comment=(
        'One row per customer, built to answer "who deserves our attention?": '
        "who drives revenue (value_tier), how customers group by behaviour (rfm_segment), "
        "and who is slipping away (churn_risk, is_slipping_away). "
        "All recency is measured against as_of_ts (the latest transaction), not today."
    ),
    tags={"domain": "customer", "model_type": "mart"},
    columns=[
        Column(
            "customer_durable_key",
            T.LongType(),
            comment="The customer across all their versions (dim_customer.customer_durable_key).",
        ),
        # 1. who drives revenue
        Column("total_revenue", T.DecimalType(28, 6)),
        Column("order_count", T.LongType()),
        Column("units_purchased", T.LongType()),
        Column("avg_order_value", T.DecimalType(38, 16)),
        Column("revenue_rank", T.IntegerType(), comment="1 = highest-revenue customer."),
        Column(
            "revenue_share",
            T.DecimalType(38, 10),
            comment="This customer's share of total revenue (0-1).",
        ),
        Column(
            "cumulative_revenue_share",
            T.DecimalType(38, 6),
            comment="Running revenue share down the revenue_rank order, including this customer.",
        ),
        Column(
            "value_tier",
            T.StringType(),
            comment="ABC by cumulative revenue: A = first 50% of revenue, B = next 30%, C = tail.",
        ),
        # 2. behaviour
        Column("first_purchase_ts", T.TimestampType()),
        Column("last_purchase_ts", T.TimestampType()),
        Column(
            "tenure_days",
            T.DecimalType(28, 7),
            comment="Days between first and last purchase.",
        ),
        Column(
            "active_days",
            T.LongType(),
            comment="Distinct calendar days with at least one purchase.",
        ),
        Column("distinct_franchises", T.LongType()),
        Column("distinct_products", T.LongType()),
        Column(
            "recency_score",
            T.IntegerType(),
            comment="RFM recency quintile, 5 = bought most recently.",
        ),
        Column(
            "frequency_score",
            T.IntegerType(),
            comment="RFM frequency quintile, 5 = most orders.",
        ),
        Column(
            "monetary_score",
            T.IntegerType(),
            comment="RFM monetary quintile, 5 = highest revenue.",
        ),
        Column(
            "rfm_code",
            T.StringType(),
            comment="Recency, frequency and monetary scores concatenated, e.g. 545.",
        ),
        Column(
            "rfm_segment",
            T.StringType(),
            comment=(
                "Recency score (1-5) x average of frequency and monetary scores (1-5): Champions (recent, FM >= 2.5), "
                "At Risk (not recent, FM >= 2.5), Low Value (FM < 2.5)."
            ),
        ),
        # 3. slipping away
        Column(
            "as_of_ts",
            T.TimestampType(),
            comment='Latest transaction in fct_sales; the "now" every recency metric is measured against.',
        ),
        Column("days_since_last_purchase", T.DecimalType(28, 7)),
        Column(
            "avg_days_between_purchases",
            T.DecimalType(38, 17),
            comment="The customer's own purchase rhythm. Null for one-time buyers.",
        ),
        Column(
            "purchase_overdue_ratio",
            T.DecimalType(38, 6),
            comment="days_since_last_purchase / the customer's own average gap between purchases.",
        ),
        Column(
            "revenue_recent_window",
            T.DecimalType(28, 6),
            comment=f"Revenue in the last {TREND_WINDOW_DAYS} days before as_of_ts.",
        ),
        Column(
            "revenue_prior_window",
            T.DecimalType(28, 6),
            comment="Revenue in the N days before the recent window.",
        ),
        Column(
            "revenue_trend_pct",
            T.DecimalType(38, 9),
            comment=f"Change in revenue between the last {TREND_WINDOW_DAYS} days and the {TREND_WINDOW_DAYS} days before.",
        ),
        Column(
            "churn_risk",
            T.StringType(),
            comment="Active (< 1.5 usual gaps since last purchase), At Risk (1.5-3), Lapsed (3+).",
        ),
        Column(
            "is_slipping_away",
            T.BooleanType(),
            nullable=False,
            comment="Tier A/B or high frequency-monetary customers who are At Risk/Lapsed or whose recent spend has halved.",
        ),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="customer_durable_key",
)


franchise_performance = TableSchema(
    catalog="gold",
    schema="mart",
    table="franchise_performance",
    comment=(
        'One row per franchise, built to answer "where should we grow?": '
        "which locations perform and which lag (performance_tier, is_underperforming), "
        "what geography says about demand (country_* columns, repeated on every franchise in the country), "
        "and where the case for a new branch is (expansion_case). "
        "Demand is measured where sales happen, not where customers live: customer home countries don't line up "
        "with the franchises they buy from. All trends are measured against as_of_ts (the latest transaction), not today."
    ),
    tags={"domain": "branch", "model_type": "mart"},
    columns=[
        Column(
            "franchise_key",
            T.LongType(),
            comment="The franchise; one row per row of dim_franchise, excluding the unknown member.",
        ),
        Column("country_name", T.StringType(), comment="The franchise's country."),
        # 1. which locations perform, which lag
        Column("total_revenue", T.DecimalType(28, 6)),
        Column("order_count", T.LongType()),
        Column("units_sold", T.LongType()),
        Column("avg_order_value", T.DecimalType(38, 16)),
        Column("distinct_customers", T.LongType()),
        Column(
            "repeat_customer_rate",
            T.DecimalType(6, 5),
            comment="Share of the franchise's customers who ordered there more than once (0-1).",
        ),
        Column(
            "active_days",
            T.LongType(),
            comment="Distinct calendar days with at least one sale.",
        ),
        Column("revenue_per_active_day", T.DecimalType(38, 16)),
        Column("revenue_rank", T.IntegerType(), comment="1 = highest-revenue franchise."),
        Column(
            "revenue_share",
            T.DecimalType(38, 10),
            comment="This franchise's share of network revenue (0-1).",
        ),
        Column(
            "revenue_index",
            T.DecimalType(38, 10),
            comment="Revenue / the median franchise's revenue. 1.0 = a typical franchise.",
        ),
        Column(
            "revenue_vs_size_peers_index",
            T.DecimalType(38, 10),
            comment="Revenue / the median revenue of franchises of the same franchise_size. 1.0 = typical for its size.",
        ),
        Column("review_count", T.LongType()),
        Column(
            "rated_review_count",
            T.LongType(),
            comment=(
                "Reviews with an extractable star rating; most reviews don't state one, "
                "so read avg_star_rating with this."
            ),
        ),
        Column(
            "avg_star_rating",
            T.DecimalType(6, 5),
            comment="Average star rating out of 5 across rated reviews. Null when no review states a rating.",
        ),
        Column(
            "as_of_ts",
            T.TimestampType(),
            comment='Latest transaction in fct_sales; the "now" every trend is measured against.',
        ),
        Column(
            "revenue_recent_window",
            T.DecimalType(28, 6),
            comment=f"Revenue in the last {TREND_WINDOW_DAYS} days before as_of_ts.",
        ),
        Column(
            "revenue_prior_window",
            T.DecimalType(28, 6),
            comment="Revenue in the N days before the recent window.",
        ),
        Column(
            "revenue_trend_pct",
            T.DecimalType(38, 9),
            comment="Change in revenue between the last N days and the N days before.",
        ),
        Column(
            "performance_tier",
            T.StringType(),
            comment="By network revenue percentile: Leading = top 25%, On Par = middle 50%, Lagging = bottom 25%.",
        ),
        Column(
            "is_underperforming",
            T.BooleanType(),
            comment="Lagging (or no sales) and below the median of its own size class, so not just a small store.",
        ),
        # 2. what geography says about demand
        Column("country_franchise_count", T.LongType()),
        Column(
            "city_franchise_count",
            T.LongType(),
            comment="Franchises in the same city; above 1 means branches may be splitting local demand.",
        ),
        Column("country_revenue", T.DecimalType(38, 6)),
        Column(
            "share_of_country_revenue",
            T.DecimalType(38, 10),
            comment="This franchise's share of its country's revenue (0-1).",
        ),
        Column("country_revenue_per_franchise", T.DecimalType(38, 6)),
        Column(
            "country_demand_index",
            T.DecimalType(38, 6),
            comment=(
                "country_revenue_per_franchise / network revenue per franchise. Above 1: each branch in this "
                "country sells more than the network average."
            ),
        ),
        Column(
            "country_distinct_customers",
            T.LongType(),
            comment="Distinct customers who bought at any franchise in the country.",
        ),
        Column(
            "country_avg_star_rating",
            T.DecimalType(38, 6),
            comment="Star rating averaged across every rated review in the country.",
        ),
        Column(
            "country_revenue_trend_pct",
            T.DecimalType(38, 6),
            comment="Change in country revenue between the last N days and the N days before.",
        ),
        # 3. where is the case for a new branch
        Column(
            "expansion_case",
            T.StringType(),
            comment=(
                "Fix First = underperforming, fix before growing nearby; "
                "Expand Market = country demand index >= 1.2, this branch at or above the median franchise, "
                "country revenue not down more than 50% and rated 3+ stars: a second branch in the country; "
                "Capacity Strain = 1.5x+ the revenue of same-size peers and alone in its city: demand is outgrowing "
                "the footprint, a case for another branch in the city or upsizing; "
                "Hold = none of the above."
            ),
        ),
        Column("md_loaded_at_ts", T.TimestampType()),
    ],
    primary_key="franchise_key",
    foreign_keys=[
        ForeignKey.to(dim_franchise, "franchise_key", name="fk_franchise_performance_franchise"),
        ForeignKey.to(dim_geography, "country_name", name="fk_franchise_performance_geography"),
    ],
)
