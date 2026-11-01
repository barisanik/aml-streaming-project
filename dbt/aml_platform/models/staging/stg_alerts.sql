{{
  config(
    materialized='incremental',
    unique_key='alert_id',
    incremental_strategy='merge'
  )
}}

SELECT
    alert_id,
    transaction_id,
    account_id,
    rule_id,
    rule_name,
    severity,
    window_summary,
    event_time,
    alert_time,
    detection_latency_ms,
    inserted_at
FROM {{ source('raw', 'alerts') }}

{% if is_incremental() %}

-- COALESCE loads everything when the target table exists but is still empty.
WHERE inserted_at >= (
    SELECT COALESCE(MAX(inserted_at) - INTERVAL '{{ var("lookback_mins") }} minutes', '-infinity'::timestamptz)
    FROM {{ this }}
)

{% endif %}
