/*
    Detection performance per scenario type and overall (scope = 'overall').

    Recall is measured on evaluable scenarios. Precision is reported on three levels:
     - alert rows:      every raw.alerts row, replay duplicates included.
     - unique triggers: one row per (transaction_id, rule_id). Headline precision uses this level.
     - detections:      one row per (account_id, rule_id, matched scenario). See int_alert_evaluation.
    "strict" counts only true_positive alerts. "related" also counts scenario_related alerts.
*/

WITH rule_map AS (
    SELECT scenario_type, expected_rule_id
    FROM {{ ref('scenario_rule_map') }}
    WHERE is_implemented
),

scenario_metrics AS (
    SELECT
        COALESCE(scenario_type, 'overall')                                              AS scope,
        COUNT(*)                                                                        AS scenarios_evaluated,
        COUNT(*) FILTER (WHERE is_detected)                                             AS scenarios_detected,
        COUNT(*) FILTER (WHERE is_detected_related)                                     AS scenarios_detected_related,
        AVG(first_detection_latency_ms)                                                 AS avg_first_detection_latency_ms,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY first_detection_latency_ms)         AS p50_first_detection_latency_ms,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY first_detection_latency_ms)        AS p95_first_detection_latency_ms,
        AVG(seconds_from_first_injection_to_alert)                                      AS avg_seconds_from_first_injection_to_alert
    FROM {{ ref('int_scenario_detection') }}
    WHERE is_evaluable
    GROUP BY GROUPING SETS ((scenario_type), ())
),

alert_metrics AS (
    SELECT
        COALESCE(rm.scenario_type, 'overall')                                           AS scope,
        COUNT(*)                                                                        AS alert_rows,
        COUNT(*) FILTER (WHERE e.is_true_positive_strict)                               AS alert_rows_true_positive,
        COUNT(*) FILTER (WHERE e.is_first_trigger)                                      AS unique_triggers,
        COUNT(*) FILTER (WHERE e.is_first_trigger AND e.is_true_positive_strict)        AS unique_triggers_true_positive,
        COUNT(*) FILTER (WHERE e.is_first_trigger AND e.is_true_positive_related)       AS unique_triggers_related,
        COUNT(DISTINCT e.detection_key)                                                 AS detections,
        COUNT(DISTINCT e.detection_key) FILTER (WHERE e.matched_scenario_id IS NOT NULL) AS detections_related,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY e.detection_latency_ms)
            FILTER (WHERE e.is_first_trigger)                                           AS p50_detection_latency_ms,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY e.detection_latency_ms)
            FILTER (WHERE e.is_first_trigger)                                           AS p95_detection_latency_ms
    FROM {{ ref('int_alert_evaluation') }} AS e
    INNER JOIN rule_map AS rm
        ON rm.expected_rule_id = e.rule_id
    GROUP BY GROUPING SETS ((rm.scenario_type), ())
),

scopes AS (
    SELECT scenario_type AS scope, expected_rule_id AS rule_id FROM rule_map
    UNION ALL
    SELECT 'overall', NULL
),

combined AS (
    SELECT
        sc.scope,
        sc.rule_id,
        COALESCE(sm.scenarios_evaluated, 0)                                 AS scenarios_evaluated,
        COALESCE(sm.scenarios_detected, 0)                                  AS scenarios_detected,
        COALESCE(sm.scenarios_detected_related, 0)                          AS scenarios_detected_related,
        sm.scenarios_detected::numeric / NULLIF(sm.scenarios_evaluated, 0)  AS recall,
        sm.scenarios_detected_related::numeric / NULLIF(sm.scenarios_evaluated, 0) AS recall_related,
        sm.avg_first_detection_latency_ms,
        sm.p50_first_detection_latency_ms,
        sm.p95_first_detection_latency_ms,
        sm.avg_seconds_from_first_injection_to_alert,
        COALESCE(am.alert_rows, 0)                                          AS alert_rows,
        COALESCE(am.alert_rows_true_positive, 0)                            AS alert_rows_true_positive,
        am.alert_rows_true_positive::numeric / NULLIF(am.alert_rows, 0)     AS precision_alert_rows,
        COALESCE(am.unique_triggers, 0)                                     AS unique_triggers,
        COALESCE(am.unique_triggers_true_positive, 0)                       AS unique_triggers_true_positive,
        COALESCE(am.unique_triggers_related, 0)                             AS unique_triggers_related,
        am.unique_triggers_true_positive::numeric / NULLIF(am.unique_triggers, 0) AS precision_strict,
        am.unique_triggers_related::numeric / NULLIF(am.unique_triggers, 0) AS precision_related,
        COALESCE(am.detections, 0)                                          AS detections,
        COALESCE(am.detections_related, 0)                                  AS detections_related,
        am.detections_related::numeric / NULLIF(am.detections, 0)           AS precision_detections,
        am.p50_detection_latency_ms,
        am.p95_detection_latency_ms
    FROM scopes AS sc
    LEFT JOIN scenario_metrics AS sm
        ON sm.scope = sc.scope
    LEFT JOIN alert_metrics AS am
        ON am.scope = sc.scope
)

SELECT
    *,
    format(
        '%s injected scenarios evaluated, %s%% detected with a mean first detection latency of %s s. Precision %s%%, recall %s%%.',
        scenarios_evaluated,
        COALESCE(ROUND(recall * 100, 1)::text, 'n/a'),
        COALESCE(ROUND((avg_first_detection_latency_ms / 1000.0)::numeric, 2)::text, 'n/a'),
        COALESCE(ROUND(precision_strict * 100, 1)::text, 'n/a'),
        COALESCE(ROUND(recall * 100, 1)::text, 'n/a')
    )           AS summary_sentence,
    now()       AS evaluated_at
FROM combined
