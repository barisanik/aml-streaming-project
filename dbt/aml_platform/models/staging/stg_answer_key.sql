{{
  config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge'
  )
}}

SELECT
    transaction_id,
    scenario_id,
    scenario_type,
    injected_at
FROM {{ source('public', 'answer_key') }}

{% if is_incremental() %}

-- COALESCE loads everything when the target table exists but is still empty.
WHERE injected_at >= (
    SELECT COALESCE(MAX(injected_at) - INTERVAL '{{ var("lookback_mins") }} minutes', '-infinity'::timestamptz)
    FROM {{ this }}
)

{% endif %}
