{% set trend_window_days = var('franchise_performance_trend_window_days', 7) %}

WITH sales AS (
    SELECT
        transaction_id,
        customer_durable_sk,
        franchise_sk,
        transaction_datetime,
        quantity,
        total_price
    FROM {{ ref('fct_sales') }}
),

review AS (
    SELECT
        franchise_sk,
        star_rating,
        has_star_rating
    FROM {{ ref('fct_review') }}
),

franchise AS (
    SELECT
        franchise_sk,
        geography_sk,
        franchise_name,
        franchise_size,
        franchise_city
    FROM {{ ref('dim_franchise') }}
    -- the unknown member isn't a real branch and would drag down the network-wide averages below
    WHERE franchise_sk <> '-1'
),

geography AS (
    SELECT
        geography_sk,
        country_name,
        country_continent
    FROM {{ ref('dim_geography') }}
),

as_of AS (
    SELECT max(transaction_datetime) AS as_of_ts
    FROM sales
),

franchise_sales AS (
    SELECT
        s.franchise_sk,
        count(DISTINCT s.transaction_id)                     AS order_count,
        sum(s.quantity)                                      AS units_sold,
        sum(s.total_price)                                   AS total_revenue,
        count(DISTINCT s.customer_durable_sk)                AS distinct_customers,
        count(DISTINCT cast(s.transaction_datetime AS date)) AS active_days,

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
        )                                                    AS revenue_prior_window
    FROM sales AS s
    GROUP BY s.franchise_sk
),

-- customers who came back to the same franchise
franchise_customer AS (
    SELECT
        franchise_sk,
        customer_durable_sk,
        count(DISTINCT transaction_id) AS order_count
    FROM sales
    GROUP BY franchise_sk, customer_durable_sk
),

franchise_repeat AS (
    SELECT
        franchise_sk,
        avg(CASE WHEN order_count > 1 THEN 1.0 ELSE 0.0 END) AS repeat_customer_rate
    FROM franchise_customer
    GROUP BY franchise_sk
),

franchise_review AS (
    SELECT
        franchise_sk,
        count(*)                  AS review_count,
        count_if(has_star_rating) AS rated_review_count,
        avg(star_rating)          AS avg_star_rating
    FROM review
    GROUP BY franchise_sk
),

-- every franchise, with sales and reviews where they exist
base AS (
    SELECT
        f.franchise_sk,
        f.geography_sk,
        f.franchise_name,
        f.franchise_size,
        f.franchise_city,
        g.country_name,
        g.country_continent,
        fs.franchise_sk IS NOT NULL           AS has_sales,
        coalesce(fs.total_revenue, 0)         AS total_revenue,
        coalesce(fs.order_count, 0)           AS order_count,
        coalesce(fs.units_sold, 0)            AS units_sold,
        coalesce(fs.distinct_customers, 0)    AS distinct_customers,
        coalesce(fs.active_days, 0)           AS active_days,
        coalesce(fs.revenue_recent_window, 0) AS revenue_recent_window,
        coalesce(fs.revenue_prior_window, 0)  AS revenue_prior_window,
        fr.repeat_customer_rate,
        coalesce(rv.review_count, 0)          AS review_count,
        coalesce(rv.rated_review_count, 0)    AS rated_review_count,
        rv.avg_star_rating
    FROM franchise AS f
    LEFT JOIN geography AS g
        ON f.geography_sk = g.geography_sk
    LEFT JOIN franchise_sales AS fs
        ON f.franchise_sk = fs.franchise_sk
    LEFT JOIN franchise_repeat AS fr
        ON f.franchise_sk = fr.franchise_sk
    LEFT JOIN franchise_review AS rv
        ON f.franchise_sk = rv.franchise_sk
),

country_customers AS (
    SELECT
        f.geography_sk,
        count(DISTINCT s.customer_durable_sk) AS country_distinct_customers
    FROM sales AS s
    INNER JOIN franchise AS f
        ON s.franchise_sk = f.franchise_sk
    GROUP BY f.geography_sk
),

scored AS (
    SELECT
        b.*,

        (SELECT as_of_ts FROM as_of)                                                 AS as_of_ts,

        -- 1. which locations perform
        row_number() OVER (ORDER BY b.total_revenue DESC, b.franchise_sk)            AS revenue_rank,
        b.total_revenue / sum(b.total_revenue) OVER ()                               AS revenue_share,
        cume_dist() OVER (ORDER BY b.total_revenue)                                  AS revenue_percentile,

        -- compare revenue to the median of all franchises
        b.total_revenue / nullif(percentile_approx(b.total_revenue, 0.5) OVER (), 0) AS revenue_index,

        -- an S store shouldn't be judged against an XXL one
        b.total_revenue / nullif(
            percentile_approx(b.total_revenue, 0.5) OVER (PARTITION BY b.franchise_size), 0
        )                                                                            AS revenue_vs_size_peers_index,

        count(*) OVER (PARTITION BY b.geography_sk)                                  AS country_franchise_count,
        count(*) OVER (PARTITION BY b.geography_sk, b.franchise_city)                AS city_franchise_count,
        sum(b.total_revenue) OVER (PARTITION BY b.geography_sk)                      AS country_revenue,

        -- at country level, how much revenue is coming from the latest window vs the prior window
        sum(b.revenue_recent_window) OVER (PARTITION BY b.geography_sk)              AS country_revenue_recent_window,
        sum(b.revenue_prior_window) OVER (PARTITION BY b.geography_sk)               AS country_revenue_prior_window,

        sum(b.total_revenue) OVER () / count(*) OVER ()                              AS network_revenue_per_franchise,

        sum(b.avg_star_rating * b.rated_review_count) OVER (PARTITION BY b.geography_sk)
        / nullif(sum(b.rated_review_count) OVER (PARTITION BY b.geography_sk), 0)    AS country_avg_star_rating
    FROM base AS b
),

country_scored AS (
    SELECT
        s.*,
        cc.country_distinct_customers,
        s.country_revenue / s.country_franchise_count                                                                  AS country_revenue_per_franchise,

        (s.country_revenue / s.country_franchise_count) / nullif(s.network_revenue_per_franchise, 0)                   AS country_demand_index,
        (s.country_revenue_recent_window - s.country_revenue_prior_window) / nullif(s.country_revenue_prior_window, 0) AS country_revenue_trend_pct,
        (s.revenue_recent_window - s.revenue_prior_window) / nullif(s.revenue_prior_window, 0)                         AS revenue_trend_pct,

        -- performance tier: how does this franchise compare to the rest of the network
        CASE
            WHEN NOT s.has_sales THEN 'No Sales'
            WHEN s.revenue_percentile > 0.75 THEN 'Leading'
            WHEN s.revenue_percentile > 0.25 THEN 'On Par'
            ELSE 'Lagging'
        END                                                                                                            AS performance_tier
    FROM scored AS s
    LEFT JOIN country_customers AS cc
        ON s.geography_sk = cc.geography_sk
)

SELECT
    c.franchise_sk,
    c.geography_sk,

    c.total_revenue,
    c.order_count,
    c.units_sold,
    c.total_revenue / nullif(c.order_count, 0)         AS avg_order_value,
    c.distinct_customers,
    c.repeat_customer_rate,
    c.active_days,
    c.total_revenue / nullif(c.active_days, 0)         AS revenue_per_active_day,
    c.revenue_rank,
    c.revenue_share,
    c.revenue_index,
    c.revenue_vs_size_peers_index,
    c.review_count,
    c.rated_review_count,
    c.avg_star_rating,
    c.as_of_ts,
    c.revenue_recent_window,
    c.revenue_prior_window,
    c.revenue_trend_pct,
    c.performance_tier,
    c.performance_tier IN ('Lagging', 'No Sales')
    AND coalesce(c.revenue_vs_size_peers_index, 0) < 1 AS is_underperforming,

    c.country_franchise_count,
    c.city_franchise_count,
    c.country_revenue,
    c.total_revenue / nullif(c.country_revenue, 0)     AS share_of_country_revenue,
    c.country_revenue_per_franchise,
    c.country_demand_index,
    c.country_distinct_customers,
    c.country_avg_star_rating,
    c.country_revenue_trend_pct,

    -- expansion_case: one action per franchise, checked top to bottom (first match wins)
    --   Fix First       - bottom quartile (or no sales) AND below the median of its size class: weak, not just small
    --   Expand Market   - country's branches avg >= 1.2x network revenue per branch, this branch at least typical,
    --                     country revenue not halving, country rating >= 3: strong market, room for another branch
    --   Capacity Strain - >= 1.5x the median of its size class AND the only branch in its city: demand outgrowing the store
    --   Hold            - none of the above
    -- missing trend counts as flat and missing rating as 3 stars, so no data doesn't block a franchise
    CASE
        WHEN c.performance_tier IN ('Lagging', 'No Sales')
            AND coalesce(c.revenue_vs_size_peers_index, 0) < 1 THEN 'Fix First'
        -- each branch in the country already outsells the network, this one included, and the market isn't collapsing
        WHEN c.country_demand_index >= 1.2
            AND c.revenue_index >= 1
            AND coalesce(c.country_revenue_trend_pct, 0) > -0.5
            AND coalesce(c.country_avg_star_rating, 3) >= 3 THEN 'Expand Market'
        -- far busier than stores of the same size: demand is outgrowing the footprint
        WHEN c.revenue_vs_size_peers_index >= 1.5
            AND c.city_franchise_count = 1 THEN 'Capacity Strain'
        ELSE 'Hold'
    END                                                AS expansion_case,

    current_timestamp()                                AS md_loaded_at_ts
FROM country_scored AS c
