-- 2c. Where is the case for a new branch?
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
    fp.expansion_case,
    CASE fp.expansion_case
        WHEN 'Expand Market' THEN 'Open another branch in ' || g.country_name
        WHEN 'Capacity Strain' THEN 'Add a branch in ' || f.franchise_city || ' or upsize this store'
        WHEN 'Fix First' THEN 'Fix this store before growing nearby'
    END                                          AS recommendation,
    f.franchise_name,
    f.franchise_city,
    g.country_name,
    f.franchise_size,
    round(fp.total_revenue, 2)                   AS total_revenue,
    round(fp.revenue_index, 2)                   AS revenue_vs_median,
    round(fp.revenue_vs_size_peers_index, 2)     AS revenue_vs_size_peers,
    fp.city_franchise_count,
    fp.country_franchise_count,
    round(fp.country_demand_index, 2)            AS country_demand_index,
    round(fp.country_revenue_trend_pct * 100, 1) AS country_revenue_trend_pct,
    round(fp.country_avg_star_rating, 2)         AS country_avg_star_rating,
    round(fp.repeat_customer_rate * 100, 1)      AS repeat_customer_pct
FROM {{ ref('franchise_performance') }} AS fp
INNER JOIN franchise AS f
    ON fp.franchise_sk = f.franchise_sk
LEFT JOIN geography AS g
    ON fp.geography_sk = g.geography_sk
WHERE fp.expansion_case <> 'Hold'
ORDER BY
    CASE fp.expansion_case
        WHEN 'Expand Market' THEN 1
        WHEN 'Capacity Strain' THEN 2
        ELSE 3
    END,
    fp.country_demand_index DESC,
    fp.revenue_vs_size_peers_index DESC
