-- 1a. Which customers drive the most revenue?
WITH customer AS (
    SELECT
        customer_durable_sk,
        customer_full_name,
        customer_city,
        customer_country
    FROM {{ ref('dim_customer') }}
    WHERE customer_is_current
)

SELECT
    cv.revenue_rank,
    c.customer_full_name,
    c.customer_city,
    c.customer_country,
    cv.value_tier,
    round(cv.total_revenue, 2)                  AS total_revenue,
    round(cv.revenue_share * 100, 2)            AS revenue_share_pct,
    round(cv.cumulative_revenue_share * 100, 2) AS cumulative_revenue_share_pct,
    cv.order_count,
    round(cv.avg_order_value, 2)                AS avg_order_value,
    cv.rfm_segment,
    cv.churn_risk
FROM {{ ref('customer_value') }} AS cv
INNER JOIN customer AS c
    ON cv.customer_durable_sk = c.customer_durable_sk
-- WHERE cv.revenue_rank <= 5
ORDER BY cv.revenue_rank
