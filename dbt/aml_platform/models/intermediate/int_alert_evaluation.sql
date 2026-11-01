/*
    One row per alert, labelled against the answer key.

    evaluation_label:
     - true_positive:    the triggering transaction is injected and this rule is the expected rule of its scenario.
     - scenario_related: the trigger is not a true positive, but the alert window lists injected transactions
                         of a scenario whose expected rule is this rule (e.g. an organic transaction arriving
                         right after a scenario keeps the window above the threshold).
     - cross_rule:       the trigger is injected, but it belongs to a scenario expected to fire another rule.
     - false_positive:   no link to any injected scenario of this rule.

    Alerts are not suppressed and replay can write the same trigger again with a new alert_id.
    is_first_trigger keeps one row per (transaction_id, rule_id) for trigger level metrics.
*/

WITH alerts AS (
    SELECT * FROM {{ ref('stg_alerts') }}
),

answer_key AS (
    SELECT
        ak.transaction_id,
        ak.scenario_id,
        ak.scenario_type,
        rm.expected_rule_id
    FROM {{ ref('stg_answer_key') }} AS ak
    LEFT JOIN {{ ref('scenario_rule_map') }} AS rm
        ON rm.scenario_type = ak.scenario_type
),

-- Injected transactions listed in each alert window, counted per scenario
window_scenarios AS (
    SELECT
        a.alert_id,
        a.rule_id,
        ak.scenario_id,
        ak.expected_rule_id,
        COUNT(*) AS injected_in_window
    FROM alerts AS a
    CROSS JOIN LATERAL jsonb_array_elements(a.window_summary -> 'transactions') AS w(txn)
    INNER JOIN answer_key AS ak
        ON ak.transaction_id = (w.txn ->> 'transaction_id')::uuid
    GROUP BY a.alert_id, a.rule_id, ak.scenario_id, ak.expected_rule_id
),

-- The scenario of this rule with the most transactions in the window
best_window_scenario AS (
    SELECT DISTINCT ON (alert_id)
        alert_id,
        scenario_id,
        injected_in_window
    FROM window_scenarios
    WHERE expected_rule_id = rule_id
    ORDER BY alert_id, injected_in_window DESC, scenario_id
),

labelled AS (
    SELECT
        a.alert_id,
        a.transaction_id,
        a.account_id,
        a.rule_id,
        a.rule_name,
        a.severity,
        a.event_time,
        a.alert_time,
        a.detection_latency_ms,
        trigger_key.scenario_id                 AS trigger_scenario_id,
        trigger_key.scenario_type               AS trigger_scenario_type,
        bw.scenario_id                          AS window_scenario_id,
        COALESCE(bw.injected_in_window, 0)      AS window_injected_transaction_count,
        CASE
            WHEN trigger_key.expected_rule_id = a.rule_id THEN 'true_positive'
            WHEN bw.scenario_id IS NOT NULL THEN 'scenario_related'
            WHEN trigger_key.transaction_id IS NOT NULL THEN 'cross_rule'
            ELSE 'false_positive'
        END                                     AS evaluation_label,
        ROW_NUMBER() OVER (
            PARTITION BY a.transaction_id, a.rule_id
            ORDER BY a.alert_time, a.alert_id
        )                                       AS trigger_occurrence
    FROM alerts AS a
    LEFT JOIN answer_key AS trigger_key
        ON trigger_key.transaction_id = a.transaction_id
    LEFT JOIN best_window_scenario AS bw
        ON bw.alert_id = a.alert_id
),

final AS (
    SELECT
        *,
        evaluation_label = 'true_positive'                          AS is_true_positive_strict,
        evaluation_label IN ('true_positive', 'scenario_related')   AS is_true_positive_related,
        trigger_occurrence = 1                                      AS is_first_trigger,
        CASE evaluation_label
            WHEN 'true_positive' THEN trigger_scenario_id
            WHEN 'scenario_related' THEN window_scenario_id
        END                                                         AS matched_scenario_id
    FROM labelled
)

SELECT
    *,
    -- One detection per account, rule and matched scenario. Unmatched alerts of an account form one organic detection.
    account_id::text || '|' || rule_id || '|' || COALESCE(matched_scenario_id::text, 'organic') AS detection_key
FROM final
