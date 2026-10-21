WITH franchise AS (
    SELECT
        franchise_sk,
        franchise_id,
        franchise_created_date,
        franchise_valid_from,
        franchise_valid_to,
        supplier_id,
        franchise_name,
        franchise_city,
        franchise_district,
        franchise_zipcode,
        franchise_country,
        franchise_size,
        franchise_location
    FROM {{ ref('franchise') }}
),

-- each franchise has a single supplier
supplier AS (
    SELECT
        supplier_sk,
        supplier_id,
        supplier_valid_to,
        supplier_name,
        material_name,
        supplier_continent,
        supplier_city,
        is_approved
    FROM {{ ref('supplier') }}
),

-- ERP spelling
country AS (
    SELECT
        country_from_erp,
        country_name
    FROM {{ ref('countries') }}
),

geography AS (
    SELECT
        geography_sk,
        country_name
    FROM {{ ref('dim_geography') }}
)
SELECT
    '-1'                AS franchise_sk,
    NULL                AS geography_sk,
    -1                  AS franchise_id,
    NULL                AS franchise_created_date,
    '1900-01-01'::date  AS franchise_valid_from,
    NULL                AS franchise_valid_to,
    'Unknown'           AS franchise_name,
    NULL                AS franchise_size,
    NULL                AS franchise_city,
    NULL                AS franchise_district,
    NULL                AS franchise_zipcode,
    NULL                AS franchise_country,
    NULL                AS franchise_location,
    '-1'                AS supplier_sk,
    NULL                AS supplier_id,
    NULL                AS supplier_name,
    NULL                AS supplier_material_name,
    NULL                AS supplier_continent,
    NULL                AS supplier_city,
    NULL                AS is_supplier_approved,
    current_timestamp() AS md_loaded_at_ts

UNION ALL

SELECT
    f.franchise_sk,
    g.geography_sk,
    f.franchise_id,
    f.franchise_created_date,
    f.franchise_valid_from,
    f.franchise_valid_to,
    f.franchise_name,
    f.franchise_size,

    -- location
    f.franchise_city,
    f.franchise_district,
    f.franchise_zipcode,
    f.franchise_country,
    f.franchise_location,

    -- supplier
    coalesce(s.supplier_sk, '-1') AS supplier_sk,
    f.supplier_id,
    s.supplier_name,
    s.material_name               AS supplier_material_name,
    s.supplier_continent,
    s.supplier_city,
    s.is_approved                 AS is_supplier_approved,

    current_timestamp()           AS md_loaded_at_ts
FROM franchise AS f
-- the ERP is current state, so a franchise's supplierID means whoever holds that id now
LEFT JOIN supplier AS s
    ON f.supplier_id = s.supplier_id
    AND s.supplier_valid_to IS NULL
LEFT JOIN country AS co
    ON f.franchise_country = co.country_from_erp
LEFT JOIN geography AS g
    ON coalesce(co.country_name, f.franchise_country) = g.country_name
