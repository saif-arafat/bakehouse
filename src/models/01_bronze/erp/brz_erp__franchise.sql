SELECT
    franchiseid,
    name,
    city,
    district,
    zipcode,
    country,
    size,
    longitude,
    latitude,
    supplierid,

    '2026-10-19'::date  AS created_date,
    current_timestamp() AS md_ingested_ts
FROM {{ source('erp', 'sales_franchises') }}
