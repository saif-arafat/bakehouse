WITH sales AS (
    SELECT
        transaction_sk,
        transaction_id,
        customer_id,
        franchise_id,
        product_sk,
        transaction_datetime,
        transaction_quantity,
        transaction_unit_price,
        transaction_total_price,
        transaction_payment_method
    FROM {{ ref('transaction') }}
),

customer AS (
    SELECT
        customer_sk,
        customer_durable_sk,
        customer_id,
        customer_valid_from,
        customer_valid_to
    FROM {{ ref('dim_customer') }}
),

franchise AS (
    SELECT
        franchise_sk,
        franchise_id,
        franchise_valid_from,
        franchise_valid_to
    FROM {{ ref('dim_franchise') }}
),

date AS (
    SELECT
        date_sk,
        date_date
    FROM {{ ref('dim_date') }}
),

payment_method AS (
    SELECT
        payment_method_sk,
        payment_method_code
    FROM {{ ref('dim_payment_method') }}
)

SELECT
    s.transaction_sk,
    s.transaction_id,

    -- dimensions
    d.date_sk,
    coalesce(c.customer_sk, '-1')         AS customer_sk,
    coalesce(c.customer_durable_sk, '-1') AS customer_durable_sk,
    coalesce(f.franchise_sk, '-1')        AS franchise_sk,
    s.product_sk,
    p.payment_method_sk,

    s.transaction_datetime,

    -- measures
    s.transaction_quantity                AS quantity,
    s.transaction_unit_price              AS unit_price,
    s.transaction_total_price             AS total_price,

    current_timestamp()                   AS md_loaded_at_ts
FROM sales AS s
LEFT JOIN customer AS c
    ON s.customer_id = c.customer_id
    AND cast(s.transaction_datetime AS date) >= c.customer_valid_from
    AND cast(s.transaction_datetime AS date) < coalesce(c.customer_valid_to, '9999-12-31'::date)
LEFT JOIN franchise AS f
    ON s.franchise_id = f.franchise_id
    AND cast(s.transaction_datetime AS date) >= f.franchise_valid_from
    AND cast(s.transaction_datetime AS date) < coalesce(f.franchise_valid_to, '9999-12-31'::date)
LEFT JOIN date AS d
    ON cast(s.transaction_datetime AS date) = d.date_date
LEFT JOIN payment_method AS p
    ON lower(trim(s.transaction_payment_method)) = p.payment_method_code
