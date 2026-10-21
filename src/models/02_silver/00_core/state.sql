WITH loc_data AS (
    SELECT
        state_from_erp,
        raw_format,
        state_code,
        state_name,
        type,
        fips_code,
        census_region,
        census_division,
        capital,
        is_contiguous_us
    FROM {{ ref('us_states') }}
)


SELECT
    {{ dbt_utils.generate_surrogate_key([
        'state_from_erp',
    ]) }}               AS state_sk,
    state_from_erp,
    raw_format          AS state_raw_format,
    state_code          AS state_state_code,
    state_name          AS state_state_name,
    type                AS state_type,
    fips_code           AS state_fips_code,
    census_region       AS state_census_region,
    census_division     AS state_census_division,
    capital             AS state_capital,
    is_contiguous_us    AS state_is_contiguous_us,

    current_timestamp() AS md_loaded_at_ts
FROM loc_data