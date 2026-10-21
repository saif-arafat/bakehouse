-- ai_extract's JSON-schema form isn't in dbt Fusion's type signatures yet (it only knows the array<string> labels form)
{{ config(static_analysis='off') }}

WITH review_data AS (
    SELECT
        review,
        franchiseid,
        review_date,
        new_id

    FROM {{ ref('brz_web__review') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key([
        'new_id',
    ]) }}                                         AS review_sk,
    new_id                                        AS review_id,
    -- an id can belong to different franchises over time push to dim_franchise
    franchiseid::bigint                           AS franchise_id,
    review                                        AS review_text,
    -- VARIANT: {"response": {"star_rating": {"value": 4.5}}}
    ai_extract(review, '{
        "star_rating": {
            "type": "number",
            "description": "Star rating out of 5 explicitly stated in the review, e.g. 4.5. Null if no rating is stated."
        }
    }'):response.star_rating.value::decimal(2, 1) AS review_star_rating,
    review_date,
    current_timestamp()                           AS md_loaded_at_ts
FROM review_data
