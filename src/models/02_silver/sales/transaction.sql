{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='transaction_id',
    on_schema_change='append_new_columns'
) }}

WITH transaction_data AS (
    SELECT
        transactionid,
        customerid,
        franchiseid,
        datetime,
        product,
        quantity,
        unitprice,
        totalprice,
        paymentmethod,
        cardnumber
    FROM {{ ref('brz_erp__transaction') }}
    {% if is_incremental() %}
    -- lookback for late arrival rows
        WHERE md_ingested_ts > (
                SELECT max(md_loaded_at_ts) - INTERVAL 3 DAYS FROM {{ this }}
            )
    {% endif %}
)

SELECT
    -- for late arriving rows if we include date in the surrogate key then the same transactionid will get a new surrogate key and be treated as a new row, which breaks logic for late arriving
    {{ dbt_utils.generate_surrogate_key([
        'transactionid',
    ]) }}                      AS transaction_sk,
    transactionid              AS transaction_id,
    customerid                 AS customer_id,
    franchiseid                AS franchise_id,
    {{ dbt_utils.generate_surrogate_key([
        'product',
    ]) }}                      AS product_sk,
    datetime                   AS transaction_datetime,
    quantity                   AS transaction_quantity,
    unitprice::decimal(18, 6)  AS transaction_unit_price,
    totalprice::decimal(18, 6) AS transaction_total_price,
    paymentmethod              AS transaction_payment_method,
    cardnumber                 AS transaction_card_number,
    current_timestamp()        AS md_loaded_at_ts
FROM transaction_data
