"""Shared logic for the marts; no pipeline code, so it's safe to import."""

import pyspark.sql.functions as F

from lakeora.gold.mart.models import TREND_WINDOW_DAYS


def ratio(numerator, denominator):
    """numerator / denominator, null when the denominator is 0 or null (SQL's x / nullif(y, 0))."""
    d = F.col(denominator) if isinstance(denominator, str) else denominator
    n = F.col(numerator) if isinstance(numerator, str) else numerator
    return n / F.when(d != 0, d)


def latest_transaction_ts(sales):
    """The latest transaction, the "now" recency and trends are measured against (None without sales)."""
    return sales.agg(F.max("transaction_datetime")).first()[0]


def as_of_ts(as_of):
    """The collected as_of value as a timestamp column."""
    return F.lit(as_of).cast("timestamp").alias("as_of_ts")


def trend_revenue(as_of):
    """Aggregates for revenue in the last TREND_WINDOW_DAYS before as_of and the window before that."""
    window = F.expr(f"INTERVAL {TREND_WINDOW_DAYS} DAYS")
    ts, now, revenue = F.col("transaction_datetime"), F.lit(as_of).cast("timestamp"), F.col("total_price")
    recent = ts > now - window
    prior = (ts > now - window - window) & (ts <= now - window)
    return [
        F.sum(F.when(recent, revenue).otherwise(0)).alias("revenue_recent_window"),
        F.sum(F.when(prior, revenue).otherwise(0)).alias("revenue_prior_window"),
    ]


def trend_pct(recent, prior):
    """Relative change from the prior window to the recent one; negative means declining."""
    return ratio(F.col(recent) - F.col(prior), prior)
