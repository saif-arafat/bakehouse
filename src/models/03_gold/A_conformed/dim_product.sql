WITH product AS (
    SELECT
        product_sk,
        product_name,
        product_unit_price,
        product_release_date
    FROM {{ ref('product') }}
)

SELECT
    product_sk,
    product_name,
    product_unit_price,
    product_release_date::date AS product_first_sold_date,

    current_timestamp()        AS md_loaded_at_ts
FROM product
