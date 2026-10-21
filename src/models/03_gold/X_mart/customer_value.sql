{% set trend_window_days = var('customer_value_trend_window_days', 7) %}

WITH sales AS (
    SELECT
        transaction_id,
        customer_durable_sk,
        franchise_sk,
        product_sk,
        transaction_datetime,
        quantity,
        total_price
    FROM {{ ref('fct_sales') }}
),

customer AS (
    SELECT
        customer_sk,
        customer_durable_sk,
        customer_id,
        customer_full_name,
        customer_email,
        customer_city,
        customer_state,
        customer_country
    FROM {{ ref('dim_customer') }}
    WHERE customer_is_current
),

as_of AS (
    SELECT max(transaction_datetime) AS as_of_ts
    FROM sales
),

customer_sales AS (
    SELECT
        s.customer_durable_sk,
        count(DISTINCT s.transaction_id)                     AS order_count,
        sum(s.quantity)                                      AS units_purchased,
        sum(s.total_price)                                   AS total_revenue,
        min(s.transaction_datetime)                          AS first_purchase_ts,
        max(s.transaction_datetime)                          AS last_purchase_ts,
        count(DISTINCT cast(s.transaction_datetime AS date)) AS active_days,
        count(DISTINCT s.franchise_sk)                       AS distinct_franchises,
        count(DISTINCT s.product_sk)                         AS distinct_products,

        -- trend: the latest window vs the window before it
        sum(
            case
                when s.transaction_datetime > (SELECT as_of_ts FROM as_of) - interval {{ trend_window_days }} days
                    then s.total_price
                else 0
            end
        )                                                    AS revenue_recent_window,
        sum(
            case
                when s.transaction_datetime > (SELECT as_of_ts FROM as_of) - interval {{ trend_window_days * 2 }} days
                    AND s.transaction_datetime <= (SELECT as_of_ts FROM as_of) - interval {{ trend_window_days }} days
                    then s.total_price
                else 0
            end
        )                                                    AS revenue_prior_window,
        (SELECT as_of_ts FROM as_of)                         AS as_of_ts
    FROM sales AS s
    GROUP BY s.customer_durable_sk
),

metrics AS (
    SELECT
        cs.*,
        cs.total_revenue / cs.order_count                                          AS avg_order_value,
        timestampdiff(SECOND, cs.first_purchase_ts, cs.last_purchase_ts) / 86400.0 AS tenure_days,
        timestampdiff(SECOND, cs.last_purchase_ts, cs.as_of_ts) / 86400.0          AS days_since_last_purchase,
        -- (last_purchase - first_purchase) / (order_count - 1) = average days between purchases
        CASE WHEN cs.order_count > 1
                THEN timestampdiff(SECOND, cs.first_purchase_ts, cs.last_purchase_ts) / 86400.0
                    / (cs.order_count - 1)
        END                                                                        AS avg_days_between_purchases

    FROM customer_sales AS cs
),

scored AS (
    SELECT
        m.*,

        -- rank customers by revenue
        row_number() OVER (ORDER BY m.total_revenue DESC, m.customer_durable_sk) AS revenue_rank,

        -- percentage of revenue contributed by this customer
        m.total_revenue / sum(m.total_revenue) OVER ()                           AS revenue_share,

        -- cumulative revenue percentage
        sum(m.total_revenue) OVER (
            ORDER BY m.total_revenue DESC, m.customer_durable_sk
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / sum(m.total_revenue) OVER ()                                         AS cumulative_revenue_share,

        -- RFM quintiles, 5 = best; cume_dist keeps tied customers in the same bucket
        -- Recency: how recently did they buy? (days since last purchase)
        -- Frequency: how often do they buy? (order count)
        -- Monetary: how much do they spend? (total revenue)
        cast(ceil(cume_dist() OVER (ORDER BY m.last_purchase_ts) * 5) AS int)    AS recency_score,
        cast(ceil(cume_dist() OVER (ORDER BY m.order_count) * 5) AS int)         AS frequency_score,
        cast(ceil(cume_dist() OVER (ORDER BY m.total_revenue) * 5) AS int)       AS monetary_score,

        -- If you buy once the division avg_days_between_purchases is null
        -- we replace this null with the median of the distribution
        -- can't in metrics since lateral ref in window
        coalesce(
            avg_days_between_purchases,
            percentile_approx(m.avg_days_between_purchases, 0.5) OVER ()
        )                                                                        AS expected_days_between_purchases

    FROM metrics AS m
),

segmented AS (
    SELECT
        s.*,
        -- how many of their usual gaps have passed since they last bought
        s.days_since_last_purchase / nullif(s.expected_days_between_purchases, 0) AS purchase_overdue_ratio,

        -- other columns answer the recency
        -- this answers the value
        (s.frequency_score + s.monetary_score) / 2.0                              AS fm_score
    FROM scored AS s
)

SELECT
    c.customer_durable_sk,

    -- Revenue
    coalesce(s.total_revenue, 0)                                                           AS total_revenue,
    coalesce(s.order_count, 0)                                                             AS order_count,
    coalesce(s.units_purchased, 0)                                                         AS units_purchased,
    s.avg_order_value,
    s.revenue_rank,
    s.revenue_share,
    s.cumulative_revenue_share,
    -- ABC: the customers making up the first 50% of revenue are A, the next 30% B, the tail C
    CASE
        WHEN s.customer_durable_sk IS NULL THEN 'No Purchases'
        WHEN s.cumulative_revenue_share - s.revenue_share < 0.5 THEN 'A'
        WHEN s.cumulative_revenue_share - s.revenue_share < 0.8 THEN 'B'
        ELSE 'C'
    END                                                                                    AS value_tier,

    -- Behaviour
    s.first_purchase_ts,
    s.last_purchase_ts,
    s.tenure_days,
    coalesce(s.active_days, 0)                                                             AS active_days,
    coalesce(s.distinct_franchises, 0)                                                     AS distinct_franchises,
    coalesce(s.distinct_products, 0)                                                       AS distinct_products,
    s.recency_score,
    s.frequency_score,
    s.monetary_score,
    concat(s.recency_score, s.frequency_score, s.monetary_score)                           AS rfm_code,

    -- | Recency ↓ / FM →  | **High** (FM ≥ 2.5) | **Low** (FM < 2.5) |
    -- |---|---|---|
    -- | **Recent** (R ≥ 4) | Champions | Low Value |
    -- | **Not recent** (R ≤ 3) | At Risk | Low Value |
    CASE
        WHEN s.customer_durable_sk IS NULL THEN 'No Purchases'
        WHEN s.fm_score < 2.5 THEN 'Low Value'
        WHEN s.recency_score >= 4 THEN 'Champions'
        ELSE 'At Risk'
    END                                                                                    AS rfm_segment,

    -- Slipping
    s.as_of_ts,
    s.days_since_last_purchase,
    s.avg_days_between_purchases,
    s.purchase_overdue_ratio,
    coalesce(s.revenue_recent_window, 0)                                                   AS revenue_recent_window,
    coalesce(s.revenue_prior_window, 0)                                                    AS revenue_prior_window,
    -- percentage change in revenue over the last two windows, negative means declining
    (s.revenue_recent_window - s.revenue_prior_window) / nullif(s.revenue_prior_window, 0) AS revenue_trend_pct,
    -- overdue ratio: how many of their usual gaps have passed since they last bought
    CASE
        WHEN s.customer_durable_sk IS NULL THEN 'No Purchases'
        WHEN s.purchase_overdue_ratio >= 3 THEN 'Lapsed'
        WHEN s.purchase_overdue_ratio >= 1.5 THEN 'At Risk'
        ELSE 'Active'
    END                                                                                    AS churn_risk,
    -- the list to act on: valuable customers who are overdue or whose spend has halved
    coalesce(

        -- this is someone who pays a lot or buys a lot
        (s.cumulative_revenue_share - s.revenue_share < 0.8 OR s.fm_score >= 4)
        AND (
            -- gaps from last purchase are 1.5x their usual gap
            s.purchase_overdue_ratio >= 1.5
            -- spend in the recent window is at most half the prior window.
            -- if they didn't buy in the prior window, we don't consider them
            OR (s.revenue_prior_window > 0 AND s.revenue_recent_window <= 0.5 * s.revenue_prior_window)
        ),
        FALSE
    )                                                                                      AS is_slipping_away,

    current_timestamp()                                                                    AS md_loaded_at_ts
FROM customer AS c
LEFT JOIN segmented AS s
    ON c.customer_durable_sk = s.customer_durable_sk
