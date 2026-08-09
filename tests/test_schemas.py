# tests/test_schemas.py

import sys
import os
from datetime import datetime
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from pydantic import ValidationError

from shared.schemas import (
    TxnType,
    Channel,
    ScenarioType,
    Severity,
    IncomeSegment,
    EntitySegment,
    Transaction,
    Alert,
    AnswerKey,
    ConsumerHeartbeat,
    Profile,
)


VALID_TRANSACTION = {
    "transaction_id": "11111111-1111-1111-1111-111111111111",
    "account_id": "22222222-2222-2222-2222-222222222222",
    "counterparty_id": "MER-12345678",
    "amount": "150.00",
    "currency": "TRY",
    "txn_type": "card_payment",
    "channel": "mobile",
    "city": "Istanbul",
    "country": "TR",
    "event_time": "2026-01-01T12:00:00",
    "produced_at": "2026-01-01T12:00:01",
}


# ── Enums ─────────────────────────────────────────────────────────────────

def test_txn_type_values_are_all_lowercase():
    """Checks if all transaction type enum values are lowercase."""
    for member in TxnType:
        assert member.value == member.value.lower()

def test_channel_values_are_all_lowercase():
    """Checks if all channel enum values are lowercase."""
    for member in Channel:
        assert member.value == member.value.lower()

def test_scenario_type_values_are_all_lowercase():
    """Checks if all scenario type enum values are lowercase."""
    for member in ScenarioType:
        assert member.value == member.value.lower()

def test_severity_values_are_all_lowercase():
    """Checks if all severity enum values are lowercase."""
    for member in Severity:
        assert member.value == member.value.lower()

def test_txn_type_has_five_members():
    """Checks count of transaction type values."""
    assert len(TxnType) == 5

# ── Transaction ───────────────────────────────────────────────────────────

def test_transaction_accepts_valid_data():
    txn = Transaction(**VALID_TRANSACTION)
    assert txn.txn_type == TxnType.CARD_PAYMENT
    assert txn.channel == Channel.MOBILE

def test_transaction_amount_is_decimal():
    """Checks if money field value is decimal instead of float."""
    txn = Transaction(**VALID_TRANSACTION)
    assert isinstance(txn.amount, Decimal)

def test_transaction_rejects_invalid_txn_type():
    data = dict(VALID_TRANSACTION, txn_type="not_a_real_type")
    with pytest.raises(ValidationError):
        Transaction(**data)

def test_transaction_rejects_invalid_channel():
    data = dict(VALID_TRANSACTION, channel="carrier_pigeon")
    with pytest.raises(ValidationError):
        Transaction(**data)

def test_transaction_rejects_missing_required_field():
    data = dict(VALID_TRANSACTION)
    del data["account_id"]
    with pytest.raises(ValidationError):
        Transaction(**data)

def test_transaction_merchant_category_defaults_to_none():
    """merchant_category is optional -- cash/ATM transactions don't have one."""
    txn = Transaction(**VALID_TRANSACTION)
    assert txn.merchant_category is None

def test_transaction_device_id_defaults_to_none():
    txn = Transaction(**VALID_TRANSACTION)
    assert txn.device_id is None

def test_transaction_rejects_non_numeric_amount():
    data = dict(VALID_TRANSACTION, amount="not-a-number")
    with pytest.raises(ValidationError):
        Transaction(**data)

def test_transaction_json_dump_serializes_amount_as_string():
    """Decimal has no native JSON type, so mode='json' must produce a
    string -- downstream consumers (e.g. tests) must account for this."""
    txn = Transaction(**VALID_TRANSACTION)
    dumped = txn.model_dump(mode="json")
    assert isinstance(dumped["amount"], str)

def test_transaction_json_dump_serializes_enum_as_plain_string():
    """txn_type/channel must dump as raw string values, not Enum repr,
    since the producer sends this payload straight to Kafka as JSON."""
    txn = Transaction(**VALID_TRANSACTION)
    dumped = txn.model_dump(mode="json")
    assert dumped["txn_type"] == "card_payment"
    assert dumped["channel"] == "mobile"


# ── Alert ─────────────────────────────────────────────────────────────────

VALID_ALERT = {
    "alert_id": "alert-1",
    "transaction_id": "11111111-1111-1111-1111-111111111111",
    "account_id": "22222222-2222-2222-2222-222222222222",
    "rule_id": "rule-structuring-01",
    "rule_name": "structuring",
    "severity": "high",
    "window_summary": {"count": 5, "sum": 45000},
    "event_time": "2026-01-01T12:00:00",
    "alert_time": "2026-01-01T12:00:05",
    "detection_latency_ms": 5000,
}

def test_alert_accepts_valid_data():
    alert = Alert(**VALID_ALERT)
    assert alert.severity == Severity.HIGH

def test_alert_rejects_invalid_severity():
    data = dict(VALID_ALERT, severity="critical")
    with pytest.raises(ValidationError):
        Alert(**data)

def test_alert_window_summary_accepts_arbitrary_dict_shape():
    """window_summary is explainability payload, its shape varies per
    rule, so it must accept any dict, not a fixed schema."""
    data = dict(VALID_ALERT, window_summary={"distinct_counterparties": 9, "note": "fan-in"})
    alert = Alert(**data)
    assert alert.window_summary["distinct_counterparties"] == 9

def test_alert_rejects_missing_window_summary():
    data = dict(VALID_ALERT)
    del data["window_summary"]
    with pytest.raises(ValidationError):
        Alert(**data)


# ── AnswerKey ─────────────────────────────────────────────────────────────

VALID_ANSWER_KEY = {
    "transaction_id": "11111111-1111-1111-1111-111111111111",
    "scenario_id": "scn-1",
    "scenario_type": "structuring",
    "injected_at": "2026-01-01T12:00:00",
}

def test_answer_key_accepts_valid_data():
    ak = AnswerKey(**VALID_ANSWER_KEY)
    assert ak.scenario_type == ScenarioType.STRUCTURING

def test_answer_key_rejects_invalid_scenario_type():
    data = dict(VALID_ANSWER_KEY, scenario_type="phishing")
    with pytest.raises(ValidationError):
        AnswerKey(**data)


# ── ConsumerHeartbeat ─────────────────────────────────────────────────────

VALID_HEARTBEAT = {
    "consumer_group": "aml_txn_consumer",
    "topic": "transactions",
    "partition": 0,
    "committed_offset": 1500,
    "messages_processed": 1500,
    "heartbeat_at": "2026-01-01T12:00:00",
}

def test_consumer_heartbeat_accepts_valid_data():
    hb = ConsumerHeartbeat(**VALID_HEARTBEAT)
    assert hb.partition == 0

def test_consumer_heartbeat_rejects_non_integer_partition():
    data = dict(VALID_HEARTBEAT, partition="zero")
    with pytest.raises(ValidationError):
        ConsumerHeartbeat(**data)


# ── Profile ───────────────────────────────────────────────────────────────

VALID_PROFILE = {
    "account_id": "22222222-2222-2222-2222-222222222222",
    "home_city": "Istanbul",
    "home_country": "TR",
    "income_segment": "medium",
    "avg_amount_mu": 5.5,
    "avg_amount_sigma": 0.6,
    "active_hour_start": 8,
    "active_hour_end": 20,
    "weekly_txn_frequency": 10.5,
    "preferred_merchant_categories": ["grocery", "dining"],
    "last_activity_at": "2026-01-01T00:00:00",
}

def test_profile_accepts_valid_data():
    profile = Profile(**VALID_PROFILE)
    assert profile.income_segment == IncomeSegment.MEDIUM

def test_profile_segment_defaults_to_individual():
    """segment is not passed here -- must default without raising, since
    profile_gen.py only sets it explicitly for forward-compatibility, not
    because Phase 0-2 requires per-profile variation."""
    profile = Profile(**VALID_PROFILE)
    assert profile.segment == EntitySegment.INDIVIDUAL

def test_profile_rejects_invalid_income_segment():
    data = dict(VALID_PROFILE, income_segment="ultra_rich")
    with pytest.raises(ValidationError):
        Profile(**data)

def test_profile_preferred_merchant_categories_must_be_list():
    data = dict(VALID_PROFILE, preferred_merchant_categories="grocery")
    with pytest.raises(ValidationError):
        Profile(**data)

def test_profile_rejects_missing_last_activity_at():
    data = dict(VALID_PROFILE)
    del data["last_activity_at"]
    with pytest.raises(ValidationError):
        Profile(**data)