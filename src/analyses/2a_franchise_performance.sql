-- 2a. Which locations perform, and which lag?
WITH franchise AS (
    SELECT
        franchise_sk,
        franchise_name,
        franchise_city,
        franchise_size
    FROM {{ ref('dim_franchise') }}
),

geography AS (
    SELECT
        geography_sk,
        country_name
    FROM {{ ref('dim_geography') }}
)

SELECT
    fp.revenue_rank,
    f.franchise_name,
    f.franchise_city,
    g.country_name,
    f.franchise_size,
    fp.performance_tier,
    fp.is_underperforming,
    round(fp.total_revenue, 2)               AS total_revenue,
    round(fp.revenue_share * 100, 2)         AS revenue_share_pct,
    round(fp.revenue_index, 2)               AS revenue_vs_median,
    round(fp.revenue_vs_size_peers_index, 2) AS revenue_vs_size_peers,
    fp.order_count,
    round(fp.avg_order_value, 2)             AS avg_order_value,
    fp.distinct_customers,
    round(fp.repeat_customer_rate * 100, 1)  AS repeat_customer_pct,
    round(fp.revenue_per_active_day, 2)      AS revenue_per_active_day,
    round(fp.revenue_trend_pct * 100, 1)     AS revenue_trend_pct,
    round(fp.avg_star_rating, 2)             AS avg_star_rating,
    fp.rated_review_count
FROM {{ ref('franchise_performance') }} AS fp
INNER JOIN franchise AS f
    ON fp.franchise_sk = f.franchise_sk
LEFT JOIN geography AS g
    ON fp.geography_sk = g.geography_sk
ORDER BY
    fp.revenue_rank
