# tests/test_rule_engine.py

import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

test_random = random.Random(0)

# Paths
SHARED_DIR = Path(__file__).resolve().parent.parent / "shared"
DETECTOR_DIR = Path(__file__).resolve().parent.parent / "scripts" / "detector"
SCENARIO_CONFIG_PATH = Path(__file__).resolve().parent.parent / "configs" / "scenario_config.yml"
WS_CONFIG_PATH = Path(__file__).resolve().parent.parent / "configs" / "window_config.yml"

sys.path.insert(0, str(SHARED_DIR))
sys.path.insert(0, str(DETECTOR_DIR))

from schemas import Transaction
from window_state import WindowState
import rule_engine
from rule_engine import check_structuring, check_smurfing


def load_config(path: Path) -> dict:
    """Load a YAML file and return its data as a dictionary."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

scenario_config = load_config(SCENARIO_CONFIG_PATH)
scenario_types = scenario_config["scenario_types"]

ws_config = load_config(WS_CONFIG_PATH)
ws_config = ws_config["window_length"]

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


def create_transactions(scenario_type, scenario_config, txn_count, amounts: list = None):
    """Create test transactions and add them to the selected rule window."""
    ws = WindowState(ws_config)
    scenario_config = scenario_config[scenario_type]
    low, high = 0, 0
    txn_type = ""

    if scenario_type == "structuring":
        low = scenario_config["threshold"] * scenario_config["band_low_pct"]
        high = scenario_config["threshold"] * scenario_config["band_high_pct"]
    elif scenario_type == "smurfing":
        low = scenario_config["min_amount"]
        high = scenario_config["max_amount"]

    gap_minutes_min = scenario_config["inter_transaction_gap_minutes"]["min"]
    gap_minutes_max = scenario_config["inter_transaction_gap_minutes"]["max"]
    txn_type = scenario_config["forced_txn_type"]

    base_time = datetime.now(timezone.utc)

    if amounts is None:
        amounts = [test_random.uniform(low, high) for _ in range(txn_count)]
    else:
        if len(amounts) != txn_count:
            raise ValueError(f"Invalid amount count: {len(amounts)}")

    gap_minutes = test_random.uniform(gap_minutes_min, gap_minutes_max)
    event_times = [base_time + timedelta(minutes=i * gap_minutes) for i in range(txn_count)]

    for i in range(txn_count):
        txn_id = f"test-id-{i}"
        txn_amount = amounts[i]
        event_time = event_times[i]

        transaction = Transaction(
            transaction_id=txn_id,
            account_id="acc-1",
            counterparty_id="acc-2",
            amount=txn_amount,
            currency="TRY",
            txn_type=txn_type,
            merchant_category=None,
            channel="mobile",
            city="Istanbul",
            country="TR",
            event_time=event_time,
            produced_at=datetime.now(timezone.utc),
            device_id=None,
        )

        ws.add_transaction(scenario_type, transaction)

    return ws, "acc-1"

# Structuring


def test_structuring_positive_triggers_on_band_and_sum():
    """Check that structuring triggers when the count and total conditions pass."""

    amounts = [8500,9500,8500]
    txn_count = 3

    assert all(
        structuring_limit_min <= amount <= structuring_limit_max
        for amount in amounts
    )
    assert sum(amounts) >= structuring_threshold * structuring_sum_multiplier
    assert len(amounts) == txn_count
    assert structuring_min_count <= txn_count
    
    window_state, account_id = create_transactions(scenario_type="structuring", scenario_config=scenario_types, txn_count=txn_count, amounts=amounts)

    is_structuring, structuring_txns = check_structuring(window_state, account_id)
    assert is_structuring is True
    assert len(structuring_txns) == txn_count
    assert {txn.transaction_id for txn in structuring_txns} == {
        f"test-id-{i}" for i in range(txn_count)
    }


def test_structuring_negative_below_min_count():
    """Check that structuring does not trigger below the minimum transaction count."""

    amounts = [8500,9500]
    txn_count = 2

    assert all(
        structuring_limit_min <= amount <= structuring_limit_max
        for amount in amounts
    )
    assert sum(amounts) >= structuring_threshold * structuring_sum_multiplier
    assert len(amounts) == txn_count
    assert structuring_min_count > txn_count

    window_state, account_id = create_transactions(scenario_type="structuring", scenario_config=scenario_types, txn_count=txn_count, amounts=amounts)

    is_structuring, structuring_txns = check_structuring(window_state, account_id)
    assert is_structuring is False
    assert len(structuring_txns) == txn_count


def test_structuring_negative_sum_below_required_total(monkeypatch):
    """Check that structuring does not trigger when only the total condition fails."""

    amounts = [8500,8500,8500]
    txn_count = 3

    test_sum_multiplier = 3.0
    monkeypatch.setattr(rule_engine, "structuring_sum_multiplier", test_sum_multiplier)

    assert all(
        structuring_limit_min <= amount <= structuring_limit_max
        for amount in amounts
    )
    assert len(amounts) >= structuring_min_count
    assert sum(amounts) < structuring_threshold * test_sum_multiplier

    window_state, account_id = create_transactions(scenario_type="structuring", scenario_config=scenario_types, txn_count=txn_count, amounts=amounts)

    is_structuring, structuring_txns = check_structuring(window_state, account_id)
    assert is_structuring is False
    assert len(structuring_txns) == txn_count


def test_structuring_band_low_boundary_inclusive():
    """Check that an amount equal to the lower band limit is included."""

    amounts = [structuring_limit_min,9000,9000]
    txn_count = 3

    assert all(
        structuring_limit_min <= amount <= structuring_limit_max
        for amount in amounts
    )

    window_state, account_id = create_transactions(scenario_type="structuring", scenario_config=scenario_types, txn_count=txn_count, amounts=amounts)
    
    is_structuring, structuring_txns = check_structuring(window_state, account_id)
    assert is_structuring is True
    assert len(structuring_txns) == 3
    assert any(txn.transaction_id == "test-id-0" and txn.amount == structuring_limit_min for txn in structuring_txns)


def test_structuring_band_high_boundary_inclusive():
    """Check that an amount equal to the upper band limit is included."""

    amounts = [structuring_limit_max,9000,9000]
    txn_count = 3

    assert all(
        structuring_limit_min <= amount <= structuring_limit_max
        for amount in amounts
    )

    window_state, account_id = create_transactions(scenario_type="structuring", scenario_config=scenario_types, txn_count=txn_count, amounts=amounts)
    
    is_structuring, structuring_txns = check_structuring(window_state, account_id)
    assert is_structuring is True
    assert len(structuring_txns) == 3
    assert any(txn.transaction_id == "test-id-0" and txn.amount == structuring_limit_max for txn in structuring_txns)


def test_structuring_excludes_amount_below_band():
    """Check that an amount below the lower band limit is excluded."""

    amounts = [structuring_limit_min,structuring_limit_min,structuring_limit_min-1]
    txn_count = 3

    window_state, account_id = create_transactions(scenario_type="structuring", scenario_config=scenario_types, txn_count=txn_count, amounts=amounts)
    
    is_structuring, structuring_txns = check_structuring(window_state, account_id)
    assert is_structuring is False
    assert len(structuring_txns) == 2
    assert "test-id-2" not in {txn.transaction_id for txn in structuring_txns}


def test_structuring_excludes_amount_above_band():
    """Check that an amount above the upper band limit is excluded."""

    amounts = [structuring_limit_max,structuring_limit_max,structuring_limit_max+1]
    txn_count = 3

    window_state, account_id = create_transactions(scenario_type="structuring", scenario_config=scenario_types, txn_count=txn_count, amounts=amounts)
    
    is_structuring, structuring_txns = check_structuring(window_state, account_id)
    assert is_structuring is False
    assert len(structuring_txns) == 2
    assert "test-id-2" not in {txn.transaction_id for txn in structuring_txns}


def test_structuring_known_blind_spot_out_of_band_large_sum():
    """Show the known blind spot where a large out-of-band total does not trigger."""

    amounts = [structuring_limit_min-500,structuring_limit_max+500,structuring_limit_max+500]
    txn_count = 3

    assert sum(amounts) >= structuring_threshold
    assert all(
        amount < structuring_limit_min or amount > structuring_limit_max
        for amount in amounts
    )

    window_state, account_id = create_transactions(scenario_type="structuring", scenario_config=scenario_types, txn_count=txn_count, amounts=amounts)
    
    is_structuring, structuring_txns = check_structuring(window_state, account_id)
    assert is_structuring is False
    assert structuring_txns == []

# Smurfing


def test_smurfing_positive_triggers_on_min_count():
    """Check that smurfing triggers at the minimum count with valid amounts."""
    amounts = [500] * smurfing_min_count
    window_state, account_id = create_transactions("smurfing", scenario_types, smurfing_min_count, amounts)

    is_smurfing, smurfing_txns = check_smurfing(window_state, account_id)

    assert is_smurfing is True
    assert len(smurfing_txns) == smurfing_min_count
    assert all(txn.txn_type.value == "transfer_out" for txn in smurfing_txns)


def test_smurfing_negative_below_min_count():
    """Check that smurfing does not trigger below the minimum transaction count."""
    txn_count = smurfing_min_count - 1
    amounts = [500] * txn_count
    window_state, account_id = create_transactions("smurfing", scenario_types, txn_count, amounts)

    is_smurfing, smurfing_txns = check_smurfing(window_state, account_id)

    assert is_smurfing is False
    assert len(smurfing_txns) == txn_count


def test_smurfing_min_amount_boundary_inclusive():
    """Check that an amount equal to the minimum smurfing limit is included."""
    amounts = [smurfing_limit_min] + [500] * (smurfing_min_count - 1)
    window_state, account_id = create_transactions("smurfing", scenario_types, smurfing_min_count, amounts)

    is_smurfing, smurfing_txns = check_smurfing(window_state, account_id)

    assert is_smurfing is True
    assert len(smurfing_txns) == smurfing_min_count
    assert any(txn.transaction_id == "test-id-0" and txn.amount == smurfing_limit_min for txn in smurfing_txns)


def test_smurfing_max_amount_boundary_inclusive():
    """Check that an amount equal to the maximum smurfing limit is included."""
    amounts = [smurfing_limit_max] + [500] * (smurfing_min_count - 1)
    window_state, account_id = create_transactions("smurfing", scenario_types, smurfing_min_count, amounts)

    is_smurfing, smurfing_txns = check_smurfing(window_state, account_id)

    assert is_smurfing is True
    assert len(smurfing_txns) == smurfing_min_count
    assert any(txn.transaction_id == "test-id-0" and txn.amount == smurfing_limit_max for txn in smurfing_txns)


def test_smurfing_excludes_amount_below_min():
    """Check that an amount below the minimum smurfing limit is excluded."""
    amounts = [smurfing_limit_min - 1] + [500] * (smurfing_min_count - 1)
    window_state, account_id = create_transactions("smurfing", scenario_types, smurfing_min_count, amounts)

    is_smurfing, smurfing_txns = check_smurfing(window_state, account_id)

    assert is_smurfing is False
    assert len(smurfing_txns) == smurfing_min_count - 1
    assert "test-id-0" not in {txn.transaction_id for txn in smurfing_txns}


def test_smurfing_excludes_amount_above_max():
    """Check that an amount above the maximum smurfing limit is excluded."""
    amounts = [smurfing_limit_max + 1] + [500] * (smurfing_min_count - 1)
    window_state, account_id = create_transactions("smurfing", scenario_types, smurfing_min_count, amounts)

    is_smurfing, smurfing_txns = check_smurfing(window_state, account_id)

    assert is_smurfing is False
    assert len(smurfing_txns) == smurfing_min_count - 1
    assert "test-id-0" not in {txn.transaction_id for txn in smurfing_txns}


def test_smurfing_replayed_transaction_does_not_double_count():
    """Check that replaying the same transaction does not increase the count."""
    unique_txn_count = smurfing_min_count - 1
    amounts = [500] * unique_txn_count
    window_state, account_id = create_transactions("smurfing", scenario_types, unique_txn_count, amounts)
    duplicate_transaction = window_state.windows["smurfing"][account_id][0]

    window_state.add_transaction("smurfing", duplicate_transaction)
    is_smurfing, smurfing_txns = check_smurfing(window_state, account_id)

    assert len(window_state.windows["smurfing"][account_id]) == unique_txn_count
    assert is_smurfing is False
    assert len(smurfing_txns) == unique_txn_count
    assert sum(txn.transaction_id == duplicate_transaction.transaction_id for txn in smurfing_txns) == 1
