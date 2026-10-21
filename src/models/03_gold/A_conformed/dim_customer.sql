WITH customer AS (
    SELECT
        customer_sk,
        customer_durable_sk,
        customer_id,
        customer_created_date,
        customer_valid_from,
        customer_valid_to,
        customer_is_current,
        customer_full_name,
        customer_first_name,
        customer_last_name,
        customer_email,
        customer_phone_number,
        customer_address,
        customer_city,
        customer_state,
        customer_postal_code,
        customer_country,
        customer_continent,
        customer_gender
    FROM {{ ref('customer') }}
),

state AS (
    SELECT DISTINCT
        state_name,
        state_code,
        type AS state_type,
        census_region,
        census_division,
        is_contiguous_us
    FROM {{ ref('us_states') }}
),

-- ERP spelling fix
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
    '-1'                AS customer_sk,
    '-1'                AS customer_durable_sk,
    NULL                AS geography_sk,
    -1                  AS customer_id,
    NULL                AS customer_created_date,
    '1900-01-01'::date  AS customer_valid_from,
    NULL                AS customer_valid_to,
    TRUE                AS customer_is_current,
    'Unknown'           AS customer_full_name,
    NULL                AS customer_first_name,
    NULL                AS customer_last_name,
    NULL                AS customer_email,
    NULL                AS customer_phone_number,
    NULL                AS customer_gender,
    NULL                AS customer_address,
    NULL                AS customer_city,
    NULL                AS customer_postal_code,
    NULL                AS customer_state,
    NULL                AS customer_state_code,
    NULL                AS customer_state_type,
    NULL                AS customer_census_region,
    NULL                AS customer_census_division,
    NULL                AS customer_is_contiguous_us,
    NULL                AS customer_country,
    NULL                AS customer_continent,
    current_timestamp() AS md_loaded_at_ts

UNION ALL

SELECT
    c.customer_sk,
    c.customer_durable_sk,
    g.geography_sk,
    c.customer_id,
    c.customer_created_date,
    c.customer_valid_from,
    c.customer_valid_to,
    c.customer_is_current,
    c.customer_full_name,
    c.customer_first_name,
    c.customer_last_name,
    c.customer_email,
    c.customer_phone_number,
    c.customer_gender,

    -- address
    c.customer_address,
    c.customer_city,
    c.customer_postal_code,
    c.customer_state,
    s.state_code        AS customer_state_code,
    s.state_type        AS customer_state_type,
    s.census_region     AS customer_census_region,
    s.census_division   AS customer_census_division,
    s.is_contiguous_us  AS customer_is_contiguous_us,
    c.customer_country,
    c.customer_continent,

    current_timestamp() AS md_loaded_at_ts
FROM customer AS c
LEFT JOIN state AS s
    ON c.customer_state = s.state_name
LEFT JOIN country AS co
    ON c.customer_country = co.country_from_erp
LEFT JOIN geography AS g
    ON coalesce(co.country_name, c.customer_country) = g.country_name