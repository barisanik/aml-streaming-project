# Scenario ↔ Rule Interaction Matrix

Thresholds and rule metadata live in `configs/scenario_config.yml` and window lengths in `configs/window_config.yml`.
The same mapping is loaded into dbt as `dbt/aml_platform/seeds/scenario_rule_map.csv`. `tests/test_scenario_rule_map.py` fails if this table, the seed and the config drift apart.

| scenario_type | expected rule | status |
|---|---|---|
| structuring | R-001 Structuring | Implemented |
| smurfing | R-002 Smurfing (`transfer_out` only) | Implemented |
| mule_fan_in | R-003 Mule fan-in | Implemented |
| account_takeover | Profile deviation | Not implemented |
| dormant_activation | Profile deviation | Not implemented |
