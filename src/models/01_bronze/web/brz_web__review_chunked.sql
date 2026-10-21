SELECT
    franchiseid,
    review_date,
    chunked_text,
    chunk_id,
    review_uri,

    current_timestamp() AS md_ingested_ts
FROM {{ source('web', 'media_gold_reviews_chunked') }}
