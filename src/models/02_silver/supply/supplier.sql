WITH supplier_data AS (
    SELECT
        supplierid,
        created_date,
        name,
        ingredient,
        continent,
        city,
        district,
        size,
        longitude,
        latitude,
        approved
    FROM {{ ref('brz_erp__supplier') }}
)


SELECT
    -- the ERP can reuse an id for a new supplier, so the business key is id + creation date
    {{ dbt_utils.generate_surrogate_key([
        'supplierId',
        'created_date',
    ]) }}                                        AS supplier_sk,
    supplierid                                   AS supplier_id,
    created_date                                 AS supplier_created_date,
    -- for records with same id the first one is the start of history and takes the 1900-01-01
    -- otherwise its the created_date of the record
    CASE
        WHEN row_number() OVER (PARTITION BY supplierid ORDER BY created_date) = 1
            THEN '1900-01-01'::date
        ELSE created_date
    END                                          AS supplier_valid_from,
    -- lead to get previous record's created_date, which is the valid_to of the current record
    lead(created_date) OVER (
        PARTITION BY supplierid ORDER BY created_date
    )                                            AS supplier_valid_to,
    name                                         AS supplier_name,
    ingredient                                   AS material_name,
    continent                                    AS supplier_continent,
    city                                         AS supplier_city,
    district                                     AS supplier_district,
    size                                         AS supplier_size,
    -- EWKT string  Fusion can't parse GEOMETRY column type yet;
    st_asewkt(st_point(
        longitude,
        latitude,
        -- SRID for WGS 84
        4326
    ))                                           AS supplier_location,
    -- change to boolean 
    coalesce(upper(trim(approved)) = 'Y', FALSE) AS is_approved,
    current_timestamp()                          AS md_loaded_at_ts
FROM supplier_data