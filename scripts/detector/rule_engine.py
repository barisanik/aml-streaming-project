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

def check_structuring(window_state, account_id):
    """Checks if an account's structuring window triggers the rule."""

    account_txn_list = window_state.windows["structuring"][account_id]
    filtered_transactions = []
    total_amount = 0

    for transaction in account_txn_list:
        if structuring_limit_min <= transaction.amount <= structuring_limit_max:
            filtered_transactions.append(transaction)
            total_amount += transaction.amount

    return (
        len(filtered_transactions) >= structuring_min_count
        and total_amount >= structuring_threshold * structuring_sum_multiplier
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

    return (
        len(filtered_transactions) >= smurfing_min_count
    )