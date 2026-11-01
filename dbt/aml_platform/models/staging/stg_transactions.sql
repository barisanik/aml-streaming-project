{{
  config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge'
  )
}}

SELECT
    transaction_id,
    account_id,
    counterparty_id,
    amount,
    currency,
    txn_type,
    merchant_category,
    channel,
    city,
    country,
    event_time,
    produced_at,
    device_id,
    inserted_at
FROM {{ source('raw', 'transactions') }}

{% if is_incremental() %}

-- COALESCE loads everything when the target table exists but is still empty.
WHERE event_time >= (
    SELECT COALESCE(MAX(event_time) - INTERVAL '{{ var("lookback_mins") }} minutes', '-infinity'::timestamptz)
    FROM {{ this }}
)

{% endif %}