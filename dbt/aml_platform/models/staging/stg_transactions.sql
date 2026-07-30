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

WHERE event_time >= (
    SELECT MAX(event_time) - INTERVAL '{{ var("lookback_mins") }} minutes'
    FROM {{ this }}
)

{% endif %}