WITH product_data AS (
    SELECT
        product,
        unitprice,
        datetime
    FROM
        {{ ref('brz_erp__transaction') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'product',
    ]) }}                          AS product_sk,
    product                        AS product_name,
    max(unitprice::decimal(18, 6)) AS product_unit_price,
    min(datetime)                  AS product_release_date,
    current_timestamp()            AS md_loaded_at_ts
FROM product_data
GROUP BY product