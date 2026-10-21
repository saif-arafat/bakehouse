WITH payment_method AS (
    SELECT DISTINCT lower(trim(transaction_payment_method)) AS payment_method_code
    FROM {{ ref('transaction') }}
    WHERE transaction_payment_method IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['payment_method_code']) }} AS payment_method_sk,
    payment_method_code,
    CASE payment_method_code
        WHEN 'amex' THEN 'American Express'
        WHEN 'mastercard' THEN 'Mastercard'
        WHEN 'visa' THEN 'Visa'
        ELSE initcap(payment_method_code)
    END                                                             AS payment_method_name,

    CASE
        WHEN payment_method_code IN ('amex', 'mastercard', 'visa') THEN 'Card'
        ELSE 'Other'
    END                                                             AS payment_method_type,

    current_timestamp()                                             AS md_loaded_at_ts
FROM payment_method
