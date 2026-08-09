# tests/test_txn_producer.py

import sys
import os
import random
import uuid
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts', 'simulator'))

import numpy as np

import txn_producer
from txn_producer import (
    get_time_band,
    create_wait_time,
    get_forced_values,
    create_scenario,
    get_due_scenario,
    write_answer_key,
    delivery_callback,
    create_transaction_event,
)


TEST_PROFILE = {
    "account_id": "11111111-1111-1111-1111-111111111111",
    "home_city": "Istanbul",
    "home_country": "TR",
    "preferred_merchant_categories": ["grocery", "dining"],
    "avg_amount_mu": 5.0,
    "avg_amount_sigma": 0.5,
}

OTHER_PROFILE = {
    "account_id": "22222222-2222-2222-2222-222222222222",
    "home_city": "London",
    "home_country": "GB",
    "preferred_merchant_categories": ["retail", "travel"],
    "avg_amount_mu": 5.0,
    "avg_amount_sigma": 0.5,
}

FIXED_TIME = datetime(2026, 1, 1, 0, 0, 0)


# ── Function: get_time_band ──────────────────────────────────────────────────

def test_get_time_band_boundaries():
    """Checks the band assigned at each boundary hour, since off-by-one errors here silently shift channel probability distributions."""
    expected = {
        7: "morning", 12: "morning",
        13: "afternoon", 18: "afternoon",
        19: "evening", 23: "evening", 0: "evening",
        1: "night", 6: "night",
    }
    for hour, band in expected.items():
        time = FIXED_TIME.replace(hour=hour)
        assert get_time_band(time) == band


# ── Function: create_wait_time ──────────────────────────────────────────────

def test_create_wait_time_returns_positive_value():
    np.random.seed(0)
    time = FIXED_TIME.replace(hour=14)
    wait = create_wait_time(rate=0.579, time=time)
    assert wait > 0

def test_create_wait_time_peak_hour_has_shorter_average_wait_than_trough_hour():
    """Validates if peak hour has a higher intensity multiplier than trough hour."""
    np.random.seed(0)
    peak_time = FIXED_TIME.replace(hour=14)
    peak_waits = [create_wait_time(rate=0.579, time=peak_time) for _ in range(500)]

    np.random.seed(0)
    trough_time = FIXED_TIME.replace(hour=3)
    trough_waits = [create_wait_time(rate=0.579, time=trough_time) for _ in range(500)]

    assert (sum(peak_waits) / len(peak_waits)) < (sum(trough_waits) / len(trough_waits))


# ── Function: get_forced_values ─────────────────────────────────────────────

def test_get_forced_values_structuring_within_band():
    scenario_details = {
        "scenario_type": "structuring",
        "rules": {
            "forced_txn_type": "cash_deposit",
            "threshold": 10000,
            "band_low_pct": 0.85,
            "band_high_pct": 0.99,
        },
    }
    random.seed(0)
    txn_type, amount = get_forced_values(scenario_details)

    assert txn_type == "cash_deposit"
    assert 8500 <= amount <= 9900

def test_get_forced_values_smurfing_within_band():
    scenario_details = {
        "scenario_type": "smurfing",
        "rules": {
            "forced_txn_type": "transfer_out",
            "min_amount": 50,
            "max_amount": 1000,
        },
    }
    random.seed(0)
    txn_type, amount = get_forced_values(scenario_details)

    assert txn_type == "transfer_out"
    assert 50 <= amount <= 1000

def test_get_forced_values_unknown_type_returns_none_amount():
    scenario_details = {
        "scenario_type": "unknown_scenario",
        "rules": {"forced_txn_type": "card_payment"},
    }
    _, amount = get_forced_values(scenario_details)
    assert amount is None


# ── Function: create_scenario ───────────────────────────────────────────────

TEST_SCENARIO_TYPE = {
    "name": "structuring",
    "forced_txn_type": "cash_deposit",
    "series_length": {"min": 3, "max": 5},
    "inter_transaction_gap_minutes": {"min": 30, "max": 180},
    "window_hours": 72,
    "threshold": 10000,
    "band_low_pct": 0.85,
    "band_high_pct": 0.99,
}

def test_create_scenario_due_times_length_is_series_length_minus_one():
    """The first transaction in a series fires immediately, not on a due date, so due_times must hold one fewer entry than scenario_length."""
    random.seed(0)
    scenario = create_scenario(FIXED_TIME, TEST_PROFILE, TEST_SCENARIO_TYPE)
    assert len(scenario["due_times"]) == scenario["scenario_length"] - 1

def test_create_scenario_due_times_are_strictly_increasing():
    random.seed(0)
    scenario = create_scenario(FIXED_TIME, TEST_PROFILE, TEST_SCENARIO_TYPE)
    due_times = scenario["due_times"]
    assert all(due_times[i] < due_times[i + 1] for i in range(len(due_times) - 1))

def test_create_scenario_clips_last_due_time_to_window_limit():
    """A short window combined with large gaps must clip the final due time, otherwise a scenario can leak outside its detection window."""
    tight_scenario_type = dict(TEST_SCENARIO_TYPE)
    tight_scenario_type["window_hours"] = 1
    tight_scenario_type["inter_transaction_gap_minutes"] = {"min": 500, "max": 600}

    random.seed(0)
    scenario = create_scenario(FIXED_TIME, TEST_PROFILE, tight_scenario_type)
    window_limit = FIXED_TIME + timedelta(hours=1)

    assert scenario["due_times"][-1] == window_limit

def test_create_scenario_data_has_expected_keys():
    random.seed(0)
    scenario = create_scenario(FIXED_TIME, TEST_PROFILE, TEST_SCENARIO_TYPE)
    expected_keys = {
        "scenario_id", "current_sim_time", "account_id", "scenario_length",
        "scenario_type", "forced_txn_type", "rules", "due_times",
    }
    assert expected_keys.issubset(scenario.keys())


# ── Function: get_due_scenario ───────────────────────────────────────────────

def test_get_due_scenario_returns_scenario_when_due_time_passed():
    scenario = {"account_id": "acc-1", "due_times": [FIXED_TIME - timedelta(minutes=1)]}
    result = get_due_scenario([scenario], FIXED_TIME)
    assert result is scenario

def test_get_due_scenario_returns_none_when_not_yet_due():
    scenario = {"account_id": "acc-1", "due_times": [FIXED_TIME + timedelta(hours=1)]}
    result = get_due_scenario([scenario], FIXED_TIME)
    assert result is None

def test_get_due_scenario_skips_scenario_with_empty_due_times():
    """A scenario with no remaining due times must be skipped, not raise an IndexError, since completed scenarios can briefly remain in the list."""
    scenario = {"account_id": "acc-1", "due_times": []}
    result = get_due_scenario([scenario], FIXED_TIME)
    assert result is None


# ── Function: write_answer_key ──────────────────────────────────────────────

def test_write_answer_key_executes_insert_with_correct_params():
    cur = MagicMock()
    transaction_id = uuid.uuid4()
    injected_at = "2026-01-01 00:00:00"

    write_answer_key(cur, transaction_id, "scn-1", "structuring", injected_at)

    cur.execute.assert_called_once()
    args, _ = cur.execute.call_args
    assert args[1] == (str(transaction_id), "scn-1", "structuring", injected_at)


# ── Function: delivery_callback ─────────────────────────────────────────────

def test_delivery_callback_logs_error_on_failure():
    msg = MagicMock()
    msg.key.return_value = b"acc-1"

    with patch('txn_producer.logging.error') as mock_error:
        delivery_callback("some kafka error", msg)
        mock_error.assert_called_once()

def test_delivery_callback_does_not_log_on_success():
    msg = MagicMock()
    msg.key.return_value = b"acc-1"

    with patch('txn_producer.logging.error') as mock_error:
        delivery_callback(None, msg)
        mock_error.assert_not_called()


# ── Function: create_transaction_event ──────────────────────────────────────

def test_create_transaction_event_returns_valid_payload_when_not_poisoned():
    mock_producer = MagicMock()

    with patch.object(txn_producer, 'profiles', [TEST_PROFILE, OTHER_PROFILE]), \
         patch.object(txn_producer, 'producer', mock_producer):

        conn = MagicMock()
        result = create_transaction_event(
            current_sim_time=FIXED_TIME,
            customer_profile=TEST_PROFILE,
            is_poisoned=False,
            scenario_details=None,
            conn=conn,
        )

        assert result is not None
        assert result["account_id"] == TEST_PROFILE["account_id"]
        assert result["amount"] > 0
        mock_producer.produce.assert_called_once()

def test_create_transaction_event_poisoned_forces_scenario_txn_type_and_amount():
    """When is_poisoned is True, the transaction's type and amount must be overwritten by get_forced_values."""
    scenario_details = {
        "scenario_id": "scn-1",
        "scenario_type": "structuring",
        "rules": {
            "forced_txn_type": "cash_deposit",
            "threshold": 10000,
            "band_low_pct": 0.85,
            "band_high_pct": 0.99,
        },
    }
    mock_producer = MagicMock()

    with patch.object(txn_producer, 'profiles', [TEST_PROFILE, OTHER_PROFILE]), \
         patch.object(txn_producer, 'producer', mock_producer):

        conn = MagicMock()
        result = create_transaction_event(
            current_sim_time=FIXED_TIME,
            customer_profile=TEST_PROFILE,
            is_poisoned=True,
            scenario_details=scenario_details,
            conn=conn,
        )

        assert result is not None
        assert result["amount"] > 0
        mock_producer.produce.assert_called_once()

        sent_payload = json.loads(mock_producer.produce.call_args.kwargs["value"])
        assert sent_payload["txn_type"] == "cash_deposit"
        assert 8500 <= float(sent_payload["amount"]) <= 9900