WITH franchise_data AS (
    SELECT
        franchiseid,
        created_date,
        name,
        city,
        district,
        zipcode,
        country,
        size,
        longitude,
        latitude,
        supplierid

    FROM {{ ref('brz_erp__franchise') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'franchiseID',
        'created_date',
    ]) }}               AS franchise_sk,
    franchiseid         AS franchise_id,
    created_date        AS franchise_created_date,
    -- for records with same id the first one is the start of history and takes the 1900-01-01
    -- otherwise its the created_date of the record
    CASE
        WHEN row_number() OVER (PARTITION BY franchiseid ORDER BY created_date) = 1
            THEN '1900-01-01'::date
        ELSE created_date
    END                 AS franchise_valid_from,
    -- lead to get previous record's created_date, which is the valid_to of the current record
    lead(created_date) OVER (
        PARTITION BY franchiseid ORDER BY created_date
    )                   AS franchise_valid_to,

    supplierid          AS supplier_id,
    name                AS franchise_name,
    city                AS franchise_city,
    district            AS franchise_district,
    zipcode             AS franchise_zipcode,
    country             AS franchise_country,
    size                AS franchise_size,
    -- EWKT string  Fusion can't parse GEOMETRY column type yet;
    st_asewkt(st_point(
        longitude,
        latitude,
        -- SRID for WGS 84
        4326
    ))                  AS franchise_location,

    current_timestamp() AS md_loaded_at_ts
FROM franchise_data
