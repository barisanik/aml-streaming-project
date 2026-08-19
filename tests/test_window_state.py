# tests/test_window_state.py

from datetime import datetime, timedelta
import sys
from pathlib import Path
import yaml
import pytest

SHARED_DIR = Path(__file__).resolve().parent.parent / "shared"
DETECTOR_DIR = Path(__file__).resolve().parent.parent / "scripts" / "detector"
CONFIG_PATH = Path(__file__).resolve().parent.parent / "configs" / "window_config.yml"
sys.path.insert(0, str(SHARED_DIR))
sys.path.insert(0, str(DETECTOR_DIR))

from schemas import Transaction
from window_state import WindowState

def load_config(path: Path) -> dict:
    """Loads yaml files specificly."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

config = load_config(CONFIG_PATH)
config = config["window_length"]
structuring_window_length = int(config["structuring"])
smurfing_window_length = int(config["smurfing"])

valid_transaction = Transaction(
    transaction_id="test-id-1",
    account_id="acc-1",
    counterparty_id="acc-2",
    amount=100.0,
    currency="TRY",
    txn_type="transfer_out",
    merchant_category=None,
    channel="mobile",
    city="Istanbul",
    country="TR",
    event_time=datetime.now(),
    produced_at=datetime.now(),
    device_id=None,
)

# ── Function: add_transaction ──────────────────────────────────────────────────

def test_add_transaction():
    """Checks that a transaction is added to both rule family windows independently."""
    ws = WindowState(config)
    ws.add_transaction("structuring", valid_transaction)
    ws.add_transaction("smurfing", valid_transaction)
    assert len(ws.windows["structuring"]["acc-1"]) == 1
    assert len(ws.windows["smurfing"]["acc-1"]) == 1

def test_protection_on_invalid_rule_family():
    """Raises a ValueError when adding a transaction with an unknown rule family."""
    ws = WindowState(config)

    with pytest.raises(ValueError):
        ws.add_transaction("invalid_rule_family", valid_transaction)

def test_seperation_of_rule_families():
    """A transaction recorded on a specific rule family must not be seen on another rule family unless it included both families."""
    ws = WindowState(config)
    ws.add_transaction("structuring", valid_transaction)

    assert len(ws.windows["structuring"]["acc-1"]) == 1
    assert "acc-1" not in ws.windows["smurfing"]

def test_seperation_of_account_window_states():
    """A transaction recorded on a specific account must not be seen on another account's deque."""
    ws = WindowState(config)
    ws.add_transaction("structuring", valid_transaction)

    assert len(ws.windows["structuring"]["acc-1"]) == 1
    assert "acc-2" not in ws.windows["structuring"]

# ── Function: check_if_transaction_exists ──────────────────────────────────────────────────

def test_add_transaction_duplicate_prevention():
    """Checks duplicate prevention while adding transaction to deque."""
    ws = WindowState(config)
    ws.add_transaction("structuring", valid_transaction)
    ws.add_transaction("structuring", valid_transaction)
    assert len(ws.windows["structuring"]["acc-1"]) == 1

# ── Function: clear_old_transactions ──────────────────────────────────────────────────

def test_clear_old_transaction_function_structuring_out_of_date():
    """Checks if script clears old transactions while adding new ones for structuring."""
    ws = WindowState(config)
    old_transaction = Transaction(
        transaction_id="test-id-2",
        account_id="acc-1",
        counterparty_id="acc-3",
        amount=200.0,
        currency="EUR",
        txn_type="transfer_out",
        merchant_category=None,
        channel="mobile",
        city="Berlin",
        country="GER",
        event_time=datetime.now() - timedelta(minutes=(structuring_window_length+1)),
        produced_at=datetime.now(),
        device_id=None,
    )

    ws.add_transaction("structuring", old_transaction)
    ws.add_transaction("structuring", valid_transaction)

    assert len(ws.windows["structuring"]["acc-1"]) == 1
    assert ws.windows["structuring"]["acc-1"][0].transaction_id == "test-id-1"

def test_clear_old_transaction_function_structuring_on_time_limit():
    """Checks if script does not clear transactions on time limit while adding new ones for structuring."""
    ws = WindowState(config)
    old_transaction = Transaction(
        transaction_id="test-id-2",
        account_id="acc-1",
        counterparty_id="acc-3",
        amount=200.0,
        currency="EUR",
        txn_type="transfer_out",
        merchant_category=None,
        channel="mobile",
        city="Berlin",
        country="GER",
        event_time=datetime.now() - timedelta(minutes=(structuring_window_length)),
        produced_at=datetime.now(),
        device_id=None,
    )
    
    ws.add_transaction("structuring", old_transaction)
    ws.add_transaction("structuring", valid_transaction)

    assert len(ws.windows["structuring"]["acc-1"]) == 2

def test_clear_old_transaction_function_smurfing_out_of_time():
    """Checks if script clears old transactions while adding new ones for smurfing."""
    ws = WindowState(config)
    old_transaction = Transaction(
        transaction_id="test-id-2",
        account_id="acc-1",
        counterparty_id="acc-3",
        amount=200.0,
        currency="EUR",
        txn_type="transfer_out",
        merchant_category=None,
        channel="mobile",
        city="Berlin",
        country="GER",
        event_time=datetime.now() - timedelta(minutes=(smurfing_window_length+1)),
        produced_at=datetime.now(),
        device_id=None,
    )

    ws.add_transaction("smurfing", old_transaction)
    ws.add_transaction("smurfing", valid_transaction)

    assert len(ws.windows["smurfing"]["acc-1"]) == 1
    assert ws.windows["smurfing"]["acc-1"][0].transaction_id == "test-id-1"

def test_clear_old_transaction_function_smurfing_on_time_limit():
    """Checks if script does not clear transactions on time limit while adding new ones for smurfing."""
    ws = WindowState(config)
    old_transaction = Transaction(
        transaction_id="test-id-2",
        account_id="acc-1",
        counterparty_id="acc-3",
        amount=200.0,
        currency="EUR",
        txn_type="transfer_out",
        merchant_category=None,
        channel="mobile",
        city="Berlin",
        country="GER",
        event_time=datetime.now() - timedelta(minutes=(smurfing_window_length)),
        produced_at=datetime.now(),
        device_id=None,
    )

    ws.add_transaction("smurfing", old_transaction)
    ws.add_transaction("smurfing", valid_transaction)

    assert len(ws.windows["smurfing"]["acc-1"]) == 2