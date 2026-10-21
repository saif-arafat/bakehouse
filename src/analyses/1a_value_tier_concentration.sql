-- 1a. Which customers drive the most revenue?
SELECT
    value_tier,
    count(*)                                                             AS customers,
    round(count(*) * 100.0 / sum(count(*)) OVER (), 1)                   AS customer_share_pct,
    round(sum(total_revenue), 2)                                         AS total_revenue,
    round(sum(total_revenue) * 100 / sum(sum(total_revenue)) OVER (), 1) AS revenue_share_pct,
    round(avg(total_revenue), 2)                                         AS avg_revenue_per_customer,
    round(avg(order_count), 1)                                           AS avg_orders_per_customer
FROM {{ ref('customer_value') }}
GROUP BY value_tier
ORDER BY value_tier
