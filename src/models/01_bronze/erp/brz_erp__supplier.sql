SELECT
    supplierid,
    name,
    ingredient,
    continent,
    city,
    district,
    size,
    longitude,
    latitude,
    approved,

    '2026-10-19'::date  AS created_date,
    current_timestamp() AS md_ingested_ts
FROM {{ source('erp', 'sales_suppliers') }}
