SELECT
    customerid,
    first_name,
    last_name,
    email_address,
    phone_number,
    address,
    city,
    state,
    country,
    continent,
    postal_zip_code,
    gender,

    '2026-10-19'::date  AS created_date,
    current_timestamp() AS md_ingested_ts
FROM {{ source('erp', 'sales_customers') }}
