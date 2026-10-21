WITH supplier AS (
    SELECT
        supplier_sk,
        supplier_id,
        supplier_created_date,
        supplier_valid_from,
        supplier_valid_to,
        supplier_name,
        material_name,
        supplier_continent,
        supplier_city,
        supplier_district,
        supplier_size,
        supplier_location,
        is_approved
    FROM {{ ref('supplier') }}
)


SELECT
    '-1'                AS supplier_sk,
    -1                  AS supplier_id,
    NULL                AS supplier_created_date,
    '1900-01-01'::date  AS supplier_valid_from,
    NULL                AS supplier_valid_to,
    'Unknown'           AS supplier_name,
    NULL                AS supplier_material_name,
    NULL                AS supplier_size,
    NULL                AS is_supplier_approved,
    NULL                AS supplier_city,
    NULL                AS supplier_district,
    NULL                AS supplier_continent,
    NULL                AS supplier_location,
    current_timestamp() AS md_loaded_at_ts

UNION ALL

SELECT
    supplier_sk,
    supplier_id,
    supplier_created_date,
    supplier_valid_from,
    supplier_valid_to,
    supplier_name,
    material_name       AS supplier_material_name,
    supplier_size,
    is_approved         AS is_supplier_approved,

    -- location
    supplier_city,
    supplier_district,
    supplier_continent,
    supplier_location,

    current_timestamp() AS md_loaded_at_ts
FROM supplier

