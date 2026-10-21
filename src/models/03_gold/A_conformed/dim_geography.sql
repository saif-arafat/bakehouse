WITH erp_country AS (
    SELECT customer_country AS country_from_erp FROM {{ ref('customer') }}
    UNION
    SELECT franchise_country FROM {{ ref('franchise') }}
),

country AS (
    SELECT
        country_from_erp,
        country_name,
        iso2_code,
        iso3_code,
        continent,
        subregion
    FROM {{ ref('countries') }}
),

resolved AS (
    SELECT DISTINCT
        coalesce(c.country_name, e.country_from_erp) AS country_name,
        c.iso2_code,
        c.iso3_code,
        c.continent,
        c.subregion
    FROM erp_country AS e
    LEFT JOIN country AS c
        ON e.country_from_erp = c.country_from_erp
    WHERE e.country_from_erp IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['country_name']) }} AS geography_sk,
    country_name,
    iso2_code                                                AS country_iso2_code,
    iso3_code                                                AS country_iso3_code,
    subregion                                                AS country_subregion,
    continent                                                AS country_continent,

    current_timestamp()                                      AS md_loaded_at_ts
FROM resolved
