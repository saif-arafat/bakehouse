-- 2b. What does geography say about demand?
SELECT
    g.country_continent,
    g.country_name,
    fp.country_franchise_count,
    round(fp.country_revenue, 2)                                            AS country_revenue,
    round(fp.country_revenue_per_franchise, 2)                              AS revenue_per_franchise,
    round(fp.country_demand_index, 2)                                       AS demand_index,
    fp.country_distinct_customers,
    round(fp.country_revenue / nullif(fp.country_distinct_customers, 0), 2) AS revenue_per_customer,
    round(fp.country_revenue_trend_pct * 100, 1)                            AS revenue_trend_pct,
    round(fp.country_avg_star_rating, 2)                                    AS avg_star_rating,
    count_if(fp.performance_tier = 'Leading')                               AS leading_franchises,
    count_if(fp.is_underperforming)                                         AS underperforming_franchises
FROM {{ ref('franchise_performance') }} AS fp
LEFT JOIN {{ ref('dim_geography') }} AS g
    ON fp.geography_sk = g.geography_sk
GROUP BY
    g.country_continent,
    g.country_name,
    fp.country_franchise_count,
    fp.country_revenue,
    fp.country_revenue_per_franchise,
    fp.country_demand_index,
    fp.country_distinct_customers,
    fp.country_revenue_trend_pct,
    fp.country_avg_star_rating
ORDER BY fp.country_demand_index DESC
