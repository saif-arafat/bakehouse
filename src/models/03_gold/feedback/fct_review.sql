WITH review AS (
    SELECT
        review_sk,
        review_id,
        franchise_id,
        review_text,
        review_star_rating,
        review_date
    FROM {{ ref('review') }}
),

franchise AS (
    SELECT
        franchise_sk,
        franchise_id,
        franchise_valid_from,
        franchise_valid_to
    FROM {{ ref('dim_franchise') }}
),

date AS (
    SELECT
        date_sk,
        date_date
    FROM {{ ref('dim_date') }}
)

SELECT
    r.review_sk,
    r.review_id,

    -- dimensions
    d.date_sk,

    coalesce(f.franchise_sk, '-1')           AS franchise_sk,

    r.review_date                            AS review_datetime,
    r.review_text,

    -- measures
    r.review_star_rating                     AS star_rating,
    r.review_star_rating IS NOT NULL         AS has_star_rating,
    size(split(trim(r.review_text), '\\s+')) AS review_word_count,
    length(r.review_text)                    AS review_char_count,

    current_timestamp()                      AS md_loaded_at_ts
FROM review AS r
LEFT JOIN franchise AS f
    ON r.franchise_id = f.franchise_id
    AND cast(r.review_date AS date) >= f.franchise_valid_from
    AND cast(r.review_date AS date) < coalesce(f.franchise_valid_to, '9999-12-31'::date)
LEFT JOIN date AS d
    ON cast(r.review_date AS date) = d.date_date
