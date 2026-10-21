SELECT
    review,
    franchiseid,
    review_date,
    new_id,

    current_timestamp() AS md_ingested_ts
FROM {{ source('web', 'media_customer_reviews') }}
