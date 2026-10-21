import pyspark.sql.functions as F
from databricks.sdk.runtime import spark
from pyspark import pipelines as dp

from lakeora.gold.conformed.models import dim_date
from lakeora_core.keys import date_key

FIRST_DAY, LAST_DAY = "2000-01-01", "2059-12-31"


# generated, not ingested: static reference data that is cheap to compute
@dp.materialized_view(name=dim_date.full_name, comment=dim_date.comment, schema=dim_date.ddl())
@dp.expect_or_fail("date_date_not_null", "date_date IS NOT NULL")
def load_gold_conformed_dim_date():
    days = spark.range(1).select(
        F.explode(F.sequence(F.to_date(F.lit(FIRST_DAY)), F.to_date(F.lit(LAST_DAY)))).alias("d")
    )
    d = F.col("d")
    year_start, quarter_start, month_start, week_start = (
        F.trunc(d, "year"),
        F.trunc(d, "quarter"),
        F.trunc(d, "month"),
        F.trunc(d, "week"),  # Monday
    )
    iso_year = F.expr("extract(yearofweek FROM d)")
    df = days.select(
        date_key(d).alias("date_key"),
        d.alias("date_date"),
        # year
        F.year(d).alias("date_year"),
        year_start.alias("year_start_date"),
        F.make_date(F.year(d), F.lit(12), F.lit(31)).alias("year_end_date"),
        F.dayofyear(d).alias("date_day_of_year"),
        (F.dayofmonth(F.last_day(F.make_date(F.year(d), F.lit(2), F.lit(1)))) == 29).alias("is_leap_year"),
        # quarter
        F.quarter(d).alias("date_quarter"),
        F.concat(F.lit("Q"), F.quarter(d)).alias("quarter_name"),
        F.concat(F.year(d), F.lit("-Q"), F.quarter(d)).alias("year_quarter"),
        quarter_start.alias("quarter_start_date"),
        F.date_sub(F.add_months(quarter_start, 3), 1).alias("quarter_end_date"),
        # month
        F.month(d).alias("date_month"),
        F.date_format(d, "MMMM").alias("month_name"),
        F.date_format(d, "MMM").alias("month_name_short"),
        F.date_format(d, "yyyy-MM").alias("year_month"),
        month_start.alias("month_start_date"),
        F.last_day(d).alias("month_end_date"),
        F.dayofmonth(F.last_day(d)).alias("days_in_month"),
        (d == month_start).alias("is_month_start"),
        (d == F.last_day(d)).alias("is_month_end"),
        # ISO week: starts Monday, week 1 holds the first Thursday
        F.weekofyear(d).alias("iso_week_of_year"),
        iso_year.alias("iso_year"),
        F.concat(iso_year, F.lit("-W"), F.lpad(F.weekofyear(d).cast("string"), 2, "0")).alias("iso_year_week"),
        week_start.alias("week_start_date"),
        F.date_add(week_start, 6).alias("week_end_date"),
        # day
        F.dayofmonth(d).alias("date_day_of_month"),
        F.dayofweek(d).alias("date_day_of_week"),
        F.expr("extract(dayofweek_iso FROM d)").alias("iso_day_of_week"),
        F.date_format(d, "EEEE").alias("day_name"),
        F.date_format(d, "EEE").alias("day_name_short"),
        F.dayofweek(d).isin(1, 7).alias("is_weekend"),
        (~F.dayofweek(d).isin(1, 7)).alias("is_weekday"),
        # handy comparisons
        F.date_sub(d, 1).alias("prior_day_date"),
        F.add_months(d, -12).alias("same_day_prior_year_date"),
        F.current_timestamp().alias("md_loaded_at_ts"),
    )
    return dim_date.conform(df)
