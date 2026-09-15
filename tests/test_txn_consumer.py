# tests/test_txn_consumer.py

import sys
import os
import json
from datetime import datetime
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts', 'simulator'))

import txn_consumer
from txn_consumer import (
    alert_delivery_callback,
    dlq_delivery_callback,
    flush_buffer,
    publish_alert,
    write_alert,
    write_heartbeat,
)
from txn_consumer import Transaction, TxnType, Channel


class FakeTopicPartition:
    def __init__(self, partition, offset):
        self.partition = partition
        self.offset = offset


def build_transaction(transaction_id="11111111-1111-1111-1111-111111111111"):
    return Transaction(
        transaction_id=transaction_id,
        account_id="22222222-2222-2222-2222-222222222222",
        counterparty_id="MER-12345678",
        amount="150.00",
        currency="TRY",
        txn_type=TxnType.CARD_PAYMENT,
        merchant_category="grocery",
        channel=Channel.MOBILE,
        city="Istanbul",
        country="TR",
        event_time=datetime(2026, 1, 1, 12, 0, 0),
        produced_at=datetime(2026, 1, 1, 12, 0, 1),
        device_id=None,
    )


# ── Function: dlq_delivery_callback ──────────────────────────────────────────

def test_dlq_delivery_callback_logs_error_on_failure():
    with patch('txn_consumer.logging.error') as mock_error:
        dlq_delivery_callback("some kafka error", MagicMock())
        mock_error.assert_called_once()

def test_dlq_delivery_callback_does_not_log_on_success():
    with patch('txn_consumer.logging.error') as mock_error:
        dlq_delivery_callback(None, MagicMock())
        mock_error.assert_not_called()


# ── Function: alert_delivery_callback ────────────────────────────────────────

def test_alert_delivery_callback_logs_error_on_failure():
    msg = MagicMock()
    msg.key.return_value = b"account-1"

    with patch('txn_consumer.logging.error') as mock_error:
        alert_delivery_callback("some kafka error", msg)
        mock_error.assert_called_once()

def test_alert_delivery_callback_does_not_log_on_success():
    with patch('txn_consumer.logging.error') as mock_error:
        alert_delivery_callback(None, MagicMock())
        mock_error.assert_not_called()


# ── Function: flush_buffer ───────────────────────────────────────────────────

def test_flush_buffer_does_nothing_on_empty_buffer():
    conn = MagicMock()
    flush_buffer(conn, [])
    conn.cursor.assert_not_called()

def test_flush_buffer_calls_execute_values_once():
    conn = MagicMock()
    buffer = [build_transaction(), build_transaction("33333333-3333-3333-3333-333333333333")]

    with patch('txn_consumer.execute_values') as mock_execute_values:
        flush_buffer(conn, buffer)
        mock_execute_values.assert_called_once()

def test_flush_buffer_row_count_matches_buffer_length():
    conn = MagicMock()
    buffer = [build_transaction(), build_transaction("33333333-3333-3333-3333-333333333333")]

    with patch('txn_consumer.execute_values') as mock_execute_values:
        flush_buffer(conn, buffer)
        _, _, rows = mock_execute_values.call_args[0]
        assert len(rows) == 2

def test_flush_buffer_row_values_use_enum_dot_value_not_enum_instance():
    """Checkss the txn_type and channel values are raw string instead of Enum instance."""
    conn = MagicMock()
    buffer = [build_transaction()]

    with patch('txn_consumer.execute_values') as mock_execute_values:
        flush_buffer(conn, buffer)
        _, _, rows = mock_execute_values.call_args[0]
        row = rows[0]
        assert row[5] == "card_payment"  # txn_type
        assert row[7] == "mobile"        # channel

def test_flush_buffer_sql_contains_on_conflict_do_nothing():
    """Idempotency guard: duplicate transaction_id must not raise or duplicate a row on re-delivery."""
    conn = MagicMock()
    buffer = [build_transaction()]

    with patch('txn_consumer.execute_values') as mock_execute_values:
        flush_buffer(conn, buffer)
        _, sql, _ = mock_execute_values.call_args[0]
        assert "ON CONFLICT (transaction_id) DO NOTHING" in sql

def test_flush_buffer_closes_cursor_even_if_execute_values_raises():
    conn = MagicMock()
    buffer = [build_transaction()]

    with patch('txn_consumer.execute_values', side_effect=Exception("db error")):
        try:
            flush_buffer(conn, buffer)
        except Exception:
            pass
        conn.cursor.return_value.close.assert_called_once()


# ── Function: write_heartbeat ────────────────────────────────────────────────

def test_write_heartbeat_skips_when_no_partitions_assigned():
    conn = MagicMock()
    consumer = MagicMock()
    consumer.assignment.return_value = []

    with patch('txn_consumer.execute_values') as mock_execute_values:
        write_heartbeat(conn, consumer, "test_group", "transactions", {})
        mock_execute_values.assert_not_called()

def test_write_heartbeat_writes_one_row_per_partition():
    conn = MagicMock()
    consumer = MagicMock()
    consumer.assignment.return_value = [
        FakeTopicPartition(partition=0, offset=100),
        FakeTopicPartition(partition=1, offset=200),
    ]

    with patch('txn_consumer.execute_values') as mock_execute_values:
        write_heartbeat(conn, consumer, "test_group", "transactions", {0: 50, 1: 75})
        _, _, rows = mock_execute_values.call_args[0]
        assert len(rows) == 2

def test_write_heartbeat_uses_zero_for_partition_with_no_processed_messages():
    conn = MagicMock()
    consumer = MagicMock()
    consumer.assignment.return_value = [FakeTopicPartition(partition=2, offset=10)]

    with patch('txn_consumer.execute_values') as mock_execute_values:
        write_heartbeat(conn, consumer, "test_group", "transactions", {})
        _, _, rows = mock_execute_values.call_args[0]
        assert rows[0][4] == 0  # messages_processed column

def test_write_heartbeat_row_uses_correct_offset_per_partition():
    conn = MagicMock()
    consumer = MagicMock()
    consumer.assignment.return_value = [
        FakeTopicPartition(partition=0, offset=555),
    ]

    with patch('txn_consumer.execute_values') as mock_execute_values:
        write_heartbeat(conn, consumer, "test_group", "transactions", {0: 10})
        _, _, rows = mock_execute_values.call_args[0]
        assert rows[0][3] == 555  # committed_offset column

def test_write_heartbeat_sql_contains_upsert_on_conflict():
    conn = MagicMock()
    consumer = MagicMock()
    consumer.assignment.return_value = [FakeTopicPartition(partition=0, offset=1)]

    with patch('txn_consumer.execute_values') as mock_execute_values:
        write_heartbeat(conn, consumer, "test_group", "transactions", {})
        _, sql, _ = mock_execute_values.call_args[0]
        assert "ON CONFLICT (consumer_group, topic, partition)" in sql
        assert "DO UPDATE SET" in sql


# ── Function: write_alert and publish_alert ──────────────────────────────────

def test_write_alert_returns_the_stored_alert():
    conn = MagicMock()
    transaction = build_transaction()
    alert_time = datetime(2026, 1, 1, 12, 0, 2)

    with patch('txn_consumer.execute_values') as mock_execute_values:
        alert = write_alert(
            conn,
            transaction,
            "R-001",
            "structuring",
            "high",
            [transaction],
            alert_time,
        )

    assert alert.transaction_id == transaction.transaction_id
    assert alert.window_summary["transactions"][0]["transaction_id"] == transaction.transaction_id
    mock_execute_values.assert_called_once()

def test_publish_alert_uses_alerts_topic_and_valid_json():
    conn = MagicMock()
    producer = MagicMock()
    transaction = build_transaction()
    with patch('txn_consumer.execute_values'):
        alert = write_alert(
            conn,
            transaction,
            "R-002",
            "smurfing",
            "medium",
            [transaction],
            datetime(2026, 1, 1, 12, 0, 2),
        )

    publish_alert(producer, "alerts", alert)

    produce_kwargs = producer.produce.call_args.kwargs
    assert produce_kwargs["topic"] == "alerts"
    assert produce_kwargs["key"] == transaction.account_id.encode("utf-8")
    assert json.loads(produce_kwargs["value"])["alert_id"] == alert.alert_id
    producer.poll.assert_called_once_with(0)
