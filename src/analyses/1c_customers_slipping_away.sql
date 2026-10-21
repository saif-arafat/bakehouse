-- 1c. Which customers are slipping away?
WITH customer AS (
    SELECT
        customer_durable_sk,
        customer_full_name,
        customer_email,
        customer_city,
        customer_country
    FROM {{ ref('dim_customer') }}
    WHERE customer_is_current
)

SELECT
    c.customer_full_name,
    c.customer_email,
    c.customer_city,
    c.customer_country,
    cv.value_tier,
    cv.rfm_segment,
    cv.churn_risk,
    round(cv.total_revenue, 2)              AS total_revenue,
    cv.order_count,
    cv.last_purchase_ts,
    round(cv.days_since_last_purchase, 1)   AS days_since_last_purchase,
    round(cv.avg_days_between_purchases, 1) AS usual_days_between_purchases,
    round(cv.purchase_overdue_ratio, 1)     AS purchase_overdue_ratio,
    round(cv.revenue_prior_window, 2)       AS revenue_prior_window,
    round(cv.revenue_recent_window, 2)      AS revenue_recent_window,
    round(cv.revenue_trend_pct * 100, 1)    AS revenue_trend_pct,
    -- why they were flagged
    CASE
        WHEN cv.purchase_overdue_ratio >= 1.5
            AND cv.revenue_prior_window > 0
            AND cv.revenue_recent_window <= 0.5 * cv.revenue_prior_window THEN 'Overdue and spend halved'
        WHEN cv.purchase_overdue_ratio >= 1.5 THEN 'Overdue'
        ELSE 'Spend halved'
    END                                     AS slipping_reason
FROM {{ ref('customer_value') }} AS cv
INNER JOIN customer AS c
    ON cv.customer_durable_sk = c.customer_durable_sk
WHERE cv.is_slipping_away
ORDER BY cv.total_revenue DESC