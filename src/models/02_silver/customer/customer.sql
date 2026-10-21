WITH customer_data AS (
    SELECT
        customerid,
        created_date,
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
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('snp_erp__customer') }}
),
state_mapping AS (
    SELECT
        state_from_erp,
        state_name
    FROM {{ ref('us_states') }}
),

versioned AS (
    SELECT
        *,
        -- 2 main changes
        -- 1. id gets reused for a different person (this changes the created_date from erp)
        -- 2. a person changes their details (this keeps teh created_date)
        CASE
            -- base case first version takes 1900
            WHEN row_number() OVER (PARTITION BY customerid ORDER BY created_date, dbt_valid_from) = 1
                THEN '1900-01-01'::date
            -- change 1 where new person takes id
            WHEN row_number() OVER (PARTITION BY customerid, created_date ORDER BY dbt_valid_from) = 1
                THEN created_date
            -- change 2 where its same person but new details
            ELSE dbt_valid_from::date
        END                                                                                        AS valid_from,
        row_number() OVER (PARTITION BY customerid, created_date ORDER BY dbt_valid_from DESC) = 1 AS is_current
    FROM customer_data
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'customerID',
        'created_date',
        'dbt_valid_from',
    ]) }}                                                                                 AS customer_sk,
    {{ dbt_utils.generate_surrogate_key([
        'customerID',
        'created_date',
    ]) }}                                                                                 AS customer_durable_sk,
    customerid                                                                            AS customer_id,
    created_date                                                                          AS customer_created_date,
    valid_from                                                                            AS customer_valid_from,

    lead(valid_from) OVER (PARTITION BY customerid ORDER BY created_date, dbt_valid_from) AS customer_valid_to,
    is_current                                                                            AS customer_is_current,
    concat_ws(' ', first_name, last_name)                                                 AS customer_full_name,
    first_name                                                                            AS customer_first_name,
    last_name                                                                             AS customer_last_name,
    email_address                                                                         AS customer_email,

    regexp_replace(regexp_replace(phone_number, 'x.*$', ''), '[^0-9]', '')::BIGINT        AS customer_phone_number,
    phone_number                                                                          AS raw_phone_number,
    address                                                                               AS customer_address,
    city                                                                                  AS customer_city,
    state_name                                                                            AS customer_state,
    country                                                                               AS customer_country,
    continent                                                                             AS customer_continent,
    postal_zip_code::int                                                                  AS customer_postal_code,
    lower(trim(gender))
        AS customer_gender,
    current_timestamp()                                                                   AS md_loaded_at_ts
FROM versioned
LEFT JOIN state_mapping
    ON versioned.state = state_mapping.state_from_erp
