# Scenario ↔ Rule Interaction Matrix

Thresholds and rule metadata live in `configs/scenario_config.yml` and window lengths in `configs/window_config.yml`.

| scenario_type | expected rule | status |
|---|---|---|
| structuring | R-001 Structuring | Implemented |
| smurfing | R-002 Smurfing (`transfer_out` only) | Implemented |
| mule_fan_in | R-003 Mule fan-in | Implemented |
| account_takeover | Profile deviation | Not implemented |
| dormant_activation | Profile deviation | Not implemented |
