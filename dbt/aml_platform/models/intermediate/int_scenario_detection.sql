/*
    One row per injected scenario with its detection outcome.

    is_evaluable: the scenario has an implemented expected rule and its last transaction was injected more than
                  scenario_grace_mins ago, so a scenario still in progress is not counted as missed.
    is_detected:  at least one true_positive alert (see int_alert_evaluation).
    first_detection_latency_ms: wall-clock system latency of the first true_positive alert
                                (alert_time minus produced_at of its triggering transaction).
*/

WITH answer_key AS (
    SELECT * FROM {{ ref('stg_answer_key') }}
),

transactions AS (
    SELECT transaction_id, account_id FROM {{ ref('stg_transactions') }}
),

rule_map AS (
    SELECT * FROM {{ ref('scenario_rule_map') }}
),

alert_evaluation AS (
    SELECT * FROM {{ ref('int_alert_evaluation') }}
),

scenarios AS (
    SELECT
        ak.scenario_id,
        ak.scenario_type,
        (ARRAY_AGG(t.account_id) FILTER (WHERE t.account_id IS NOT NULL))[1]  AS account_id,
        COUNT(*)                                                                AS injected_transaction_count,
        COUNT(t.transaction_id)                                                 AS stored_transaction_count,
        MIN(ak.injected_at)                                                     AS first_injected_at,
        MAX(ak.injected_at)                                                     AS last_injected_at
    FROM answer_key AS ak
    LEFT JOIN transactions AS t
        ON t.transaction_id = ak.transaction_id
    GROUP BY ak.scenario_id, ak.scenario_type
),

first_detection AS (
    SELECT DISTINCT ON (matched_scenario_id)
        matched_scenario_id AS scenario_id,
        alert_id,
        alert_time,
        detection_latency_ms
    FROM alert_evaluation
    WHERE is_true_positive_strict
    ORDER BY matched_scenario_id, alert_time, alert_id
),

matched_alerts AS (
    SELECT
        matched_scenario_id                             AS scenario_id,
        COUNT(*) FILTER (WHERE is_true_positive_strict) AS true_positive_alert_count,
        COUNT(*)                                        AS related_alert_count
    FROM alert_evaluation
    WHERE matched_scenario_id IS NOT NULL
    GROUP BY matched_scenario_id
)

SELECT
    s.scenario_id,
    s.scenario_type,
    s.account_id,
    rm.expected_rule_id,
    COALESCE(rm.is_implemented, FALSE)                                          AS has_expected_rule,
    s.injected_transaction_count,
    s.stored_transaction_count,
    s.first_injected_at,
    s.last_injected_at,
    COALESCE(rm.is_implemented, FALSE)
        AND s.last_injected_at < now() - INTERVAL '{{ var("scenario_grace_mins") }} minutes' AS is_evaluable,
    fd.alert_id IS NOT NULL                                                     AS is_detected,
    ma.scenario_id IS NOT NULL                                                  AS is_detected_related,
    fd.alert_id                                                                 AS first_alert_id,
    fd.alert_time                                                               AS first_alert_time,
    fd.detection_latency_ms                                                     AS first_detection_latency_ms,
    EXTRACT(EPOCH FROM (fd.alert_time - s.first_injected_at))                   AS seconds_from_first_injection_to_alert,
    COALESCE(ma.true_positive_alert_count, 0)                                   AS true_positive_alert_count,
    COALESCE(ma.related_alert_count, 0)                                         AS related_alert_count
FROM scenarios AS s
LEFT JOIN rule_map AS rm
    ON rm.scenario_type = s.scenario_type
LEFT JOIN first_detection AS fd
    ON fd.scenario_id = s.scenario_id
LEFT JOIN matched_alerts AS ma
    ON ma.scenario_id = s.scenario_id
