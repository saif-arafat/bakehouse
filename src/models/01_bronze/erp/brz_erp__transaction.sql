{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}


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
    cardnumber,

    current_timestamp() AS md_ingested_ts
FROM {{ source('erp', 'sales_transactions') }}
{% if is_incremental() %}
    WHERE datetime > (SELECT max(datetime) FROM {{ this }})
{% endif %}
