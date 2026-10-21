-- 1b. How would you group customers by behaviour?
-- RFM segments: size, value and buying habits of each group
SELECT
    rfm_segment,
    count(*)                                                             AS customers,
    round(count(*) * 100.0 / sum(count(*)) OVER (), 1)                   AS customer_share_pct,
    round(sum(total_revenue), 2)                                         AS total_revenue,
    round(sum(total_revenue) * 100 / sum(sum(total_revenue)) OVER (), 1) AS revenue_share_pct,
    round(avg(total_revenue), 2)                                         AS avg_revenue_per_customer,
    round(avg(order_count), 1)                                           AS avg_orders,
    round(avg(avg_order_value), 2)                                       AS avg_order_value,
    round(avg(distinct_franchises), 1)                                   AS avg_franchises_visited,
    round(avg(distinct_products), 1)                                     AS avg_products_bought,
    round(avg(days_since_last_purchase), 1)                              AS avg_days_since_last_purchase
FROM {{ ref('customer_value') }}
GROUP BY rfm_segment
ORDER BY
    CASE rfm_segment
        WHEN 'Champions' THEN 1
        WHEN 'At Risk' THEN 2
        WHEN 'Low Value' THEN 3
        ELSE 4
    END
