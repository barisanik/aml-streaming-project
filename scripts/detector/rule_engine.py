""" /scripts/detector/rule_engine.py
# ============================================================
#   Script: Rule Engine
# ============================================================
#   Purpose: 
#       Analyzes and detects aml and fraud related transactions.
#
#   Logic:
#    - 
#
#   Usage:
#    - Called by txn_consumer.py, not useable directly.
"""

import sys
import os
import yaml
from pathlib import Path

def load_config(path: Path) -> dict:
    """Loads yaml files specificly."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

## PATHS
# Project Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)      # create logs folder if it does not exist yet

# Config Paths
CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "configs" / "producer_config.yml"
SCN_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "configs" / "scenario_config.yml"
PROFILES_PATH = Path(__file__).resolve().parent.parent / "simulator" / "profiles.json"
SHARED_DIR = PROJECT_ROOT / "shared"

scenario = load_config(SCN_CONFIG_PATH)
scenario_types = scenario["scenario_types"]

# Loading limit and transaction count data from scenario_config.yml
# Structuring
band_low_pct = scenario_types["structuring"]["band_low_pct"]
band_high_pct = scenario_types["structuring"]["band_high_pct"]
structuring_threshold = scenario_types["structuring"]["threshold"]
structuring_limit_max = structuring_threshold * band_high_pct
structuring_limit_min = structuring_threshold * band_low_pct
structuring_min_count = scenario_types["structuring"]["min_count"]
structuring_sum_multiplier = scenario_types["structuring"]["sum_multiplier"]

# Smurfing
smurfing_limit_max = scenario_types["smurfing"]["max_amount"]
smurfing_limit_min = scenario_types["smurfing"]["min_amount"]
smurfing_min_count = scenario_types["smurfing"]["min_count"]

# Mule fan-in
mule_fan_in_limit_max = scenario_types["mule_fan_in"]["max_amount"]
mule_fan_in_limit_min = scenario_types["mule_fan_in"]["min_amount"]
mule_fan_in_min_count = scenario_types["mule_fan_in"]["min_count"]
mule_fan_in_min_distinct_counterparties = scenario_types["mule_fan_in"]["min_distinct_counterparties"]
mule_fan_in_min_total_amount = scenario_types["mule_fan_in"]["min_total_amount"]

def check_structuring(window_state, account_id):
    """Checks if an account's structuring window triggers the rule."""

    account_txn_list = window_state.windows["structuring"][account_id]
    filtered_transactions = []
    total_amount = 0

    for transaction in account_txn_list:
        if structuring_limit_min <= transaction.amount <= structuring_limit_max:
            filtered_transactions.append(transaction)
            total_amount += transaction.amount

    is_triggered = len(filtered_transactions) >= structuring_min_count and total_amount >= structuring_threshold * structuring_sum_multiplier

    return (
        (is_triggered),
        (filtered_transactions)
    )

def check_smurfing(window_state, account_id):
    """Checks if an account's smurfing window triggers the rule."""

    account_txn_list = window_state.windows["smurfing"][account_id]
    filtered_transactions = []
    total_amount = 0

    for transaction in account_txn_list:
        if smurfing_limit_min <= transaction.amount <= smurfing_limit_max:
            filtered_transactions.append(transaction)
            total_amount += transaction.amount

    is_triggered = len(filtered_transactions) >= smurfing_min_count

    return (
        (is_triggered),
        (filtered_transactions)
    )


def check_mule_fan_in(window_state, account_id):
    """Check for many distinct sources transferring into one account."""

    account_txn_list = window_state.windows["mule_fan_in"].get(account_id, [])
    filtered_transactions = [
        transaction
        for transaction in account_txn_list
        if (
            transaction.txn_type.value == scenario_types["mule_fan_in"]["forced_txn_type"]
            and mule_fan_in_limit_min <= transaction.amount <= mule_fan_in_limit_max
        )
    ]
    total_amount = sum((transaction.amount for transaction in filtered_transactions), start=0)
    distinct_counterparties = {
        transaction.counterparty_id for transaction in filtered_transactions
    }
    is_triggered = (
        len(filtered_transactions) >= mule_fan_in_min_count
        and len(distinct_counterparties) >= mule_fan_in_min_distinct_counterparties
        and total_amount >= mule_fan_in_min_total_amount
    )

    return is_triggered, filtered_transactions


def summarize_mule_fan_in(transactions):
    """Build a JSON-serializable explainability summary for an R-003 alert."""

    total_amount = sum((transaction.amount for transaction in transactions), start=0)
    return {
        "qualifying_transaction_count": len(transactions),
        "distinct_counterparty_count": len(
            {transaction.counterparty_id for transaction in transactions}
        ),
        "total_amount": str(total_amount),
        "window_start": min(transaction.event_time for transaction in transactions).isoformat(),
        "window_end": max(transaction.event_time for transaction in transactions).isoformat(),
        "thresholds": {
            "min_amount": mule_fan_in_limit_min,
            "max_amount": mule_fan_in_limit_max,
            "min_count": mule_fan_in_min_count,
            "min_distinct_counterparties": mule_fan_in_min_distinct_counterparties,
            "min_total_amount": mule_fan_in_min_total_amount,
        },
    }
