# tests/test_txn_consumer.py

import sys
import os
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
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
        produced_at=datetime(2026, 1, 1, 12, 0, 1, tzinfo=timezone.utc),
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

def build_heartbeat_consumer(partitions, high_watermark=1000):
    """Mock consumer whose committed() returns the given partitions and whose watermark lookup returns (0, high_watermark)."""
    consumer = MagicMock()
    consumer.assignment.return_value = partitions
    consumer.committed.return_value = partitions
    consumer.get_watermark_offsets.return_value = (0, high_watermark)
    return consumer

def test_write_heartbeat_skips_when_no_partitions_assigned():
    conn = MagicMock()
    consumer = build_heartbeat_consumer([])

    with patch('txn_consumer.execute_values') as mock_execute_values:
        write_heartbeat(conn, consumer, "test_group", "transactions", {})
        mock_execute_values.assert_not_called()

def test_write_heartbeat_writes_one_row_per_partition():
    conn = MagicMock()
    consumer = build_heartbeat_consumer([
        FakeTopicPartition(partition=0, offset=100),
        FakeTopicPartition(partition=1, offset=200),
    ])

    with patch('txn_consumer.execute_values') as mock_execute_values:
        write_heartbeat(conn, consumer, "test_group", "transactions", {0: 50, 1: 75})
        _, _, rows = mock_execute_values.call_args[0]
        assert len(rows) == 2
        assert all(row[6].utcoffset().total_seconds() == 0 for row in rows)

def test_write_heartbeat_uses_zero_for_partition_with_no_processed_messages():
    conn = MagicMock()
    consumer = build_heartbeat_consumer([FakeTopicPartition(partition=2, offset=10)])

    with patch('txn_consumer.execute_values') as mock_execute_values:
        write_heartbeat(conn, consumer, "test_group", "transactions", {})
        _, _, rows = mock_execute_values.call_args[0]
        assert rows[0][5] == 0  # messages_processed column

def test_write_heartbeat_row_uses_offset_returned_by_committed():
    conn = MagicMock()
    consumer = build_heartbeat_consumer([FakeTopicPartition(partition=0, offset=-1000)])
    consumer.committed.return_value = [FakeTopicPartition(partition=0, offset=555)]

    with patch('txn_consumer.execute_values') as mock_execute_values:
        write_heartbeat(conn, consumer, "test_group", "transactions", {0: 10})
        _, _, rows = mock_execute_values.call_args[0]
        assert rows[0][3] == 555  # committed_offset column, not the placeholder offset from assignment()

def test_write_heartbeat_reads_cached_watermark_so_the_loop_is_not_blocked():
    conn = MagicMock()
    consumer = build_heartbeat_consumer([FakeTopicPartition(partition=0, offset=1)])

    with patch('txn_consumer.execute_values'):
        write_heartbeat(conn, consumer, "test_group", "transactions", {})
        assert consumer.get_watermark_offsets.call_args.kwargs["cached"] is True

def test_write_heartbeat_writes_log_end_offset_per_partition():
    conn = MagicMock()
    consumer = build_heartbeat_consumer([FakeTopicPartition(partition=0, offset=900)], high_watermark=1200)

    with patch('txn_consumer.execute_values') as mock_execute_values:
        write_heartbeat(conn, consumer, "test_group", "transactions", {0: 10})
        _, _, rows = mock_execute_values.call_args[0]
        assert rows[0][4] == 1200  # log_end_offset column
        assert rows[0][4] - rows[0][3] == 300  # consumer lag

def test_write_heartbeat_writes_null_log_end_offset_when_watermark_lookup_fails():
    conn = MagicMock()
    consumer = build_heartbeat_consumer([FakeTopicPartition(partition=0, offset=900)])
    consumer.get_watermark_offsets.side_effect = Exception("broker timeout")

    with patch('txn_consumer.execute_values') as mock_execute_values:
        write_heartbeat(conn, consumer, "test_group", "transactions", {0: 10})
        _, _, rows = mock_execute_values.call_args[0]
        assert rows[0][3] == 900
        assert rows[0][4] is None

def test_write_heartbeat_writes_null_log_end_offset_for_invalid_watermark():
    conn = MagicMock()
    consumer = build_heartbeat_consumer([FakeTopicPartition(partition=0, offset=900)], high_watermark=-1001)

    with patch('txn_consumer.execute_values') as mock_execute_values:
        write_heartbeat(conn, consumer, "test_group", "transactions", {0: 10})
        _, _, rows = mock_execute_values.call_args[0]
        assert rows[0][4] is None

def test_write_heartbeat_sql_contains_upsert_on_conflict():
    conn = MagicMock()
    consumer = build_heartbeat_consumer([FakeTopicPartition(partition=0, offset=1)])

    with patch('txn_consumer.execute_values') as mock_execute_values:
        write_heartbeat(conn, consumer, "test_group", "transactions", {})
        _, sql, _ = mock_execute_values.call_args[0]
        assert "ON CONFLICT (consumer_group, topic, partition)" in sql
        assert "DO UPDATE SET" in sql
        assert "log_end_offset = EXCLUDED.log_end_offset" in sql


# ── Function: write_alert and publish_alert ──────────────────────────────────

def test_write_alert_returns_the_stored_alert():
    conn = MagicMock()
    transaction = build_transaction()
    alert_time = datetime(2026, 1, 1, 12, 0, 2, tzinfo=timezone.utc)

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
    assert alert.detection_latency_ms == 1000
    assert alert.alert_time.utcoffset().total_seconds() == 0
    assert alert.window_summary["transactions"][0]["transaction_id"] == transaction.transaction_id
    mock_execute_values.assert_called_once()


def test_write_alert_latency_handles_aware_timezones():
    transaction = build_transaction().model_copy(update={
        "produced_at": datetime(2026, 1, 1, 15, 0, 1, tzinfo=timezone(timedelta(hours=3))),
    })
    alert_time = datetime(2026, 1, 1, 12, 0, 2, tzinfo=timezone.utc)

    with patch('txn_consumer.execute_values'):
        alert = write_alert(
            MagicMock(), transaction, "R-002", "smurfing", "medium",
            [transaction], alert_time,
        )

    assert alert.detection_latency_ms == 1000

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
            datetime(2026, 1, 1, 12, 0, 2, tzinfo=timezone.utc),
        )

    publish_alert(producer, "alerts", alert)

    produce_kwargs = producer.produce.call_args.kwargs
    assert produce_kwargs["topic"] == "alerts"
    assert produce_kwargs["key"] == transaction.account_id.encode("utf-8")
    assert json.loads(produce_kwargs["value"])["alert_id"] == alert.alert_id
    producer.poll.assert_called_once_with(0)


def test_mule_fan_in_alert_persists_metrics_before_publish():
    conn = MagicMock()
    producer = MagicMock()
    transaction = build_transaction().model_copy(update={
        "txn_type": TxnType.TRANSFER_IN,
        "counterparty_id": "33333333-3333-3333-3333-333333333333",
        "amount": Decimal("2000.00"),
    })
    metrics = {
        "qualifying_transaction_count": 10,
        "distinct_counterparty_count": 8,
        "total_amount": "20000.00",
    }

    with patch('txn_consumer.execute_values') as mock_execute_values:
        alert = write_alert(
            conn,
            transaction,
            "R-003",
            "mule_fan_in",
            "medium",
            [transaction],
            datetime(2026, 1, 1, 12, 0, 2, tzinfo=timezone.utc),
            metrics=metrics,
        )
        publish_alert(producer, "alerts", alert)

    assert alert.rule_id == "R-003"
    assert alert.window_summary["metrics"] == metrics
    mock_execute_values.assert_called_once()
    assert producer.produce.call_count == 1
    assert json.loads(producer.produce.call_args.kwargs["value"])["window_summary"]["metrics"] == metrics


# ── Function: parse_transaction ──────────────────────────────────────────────

def test_parse_transaction_returns_valid_transaction():
    dlq_producer = MagicMock()
    raw = json.dumps(build_transaction().model_dump(mode="json")).encode("utf-8")

    transaction = txn_consumer.parse_transaction(raw, dlq_producer, "transactions.dlq")

    assert transaction.transaction_id == build_transaction().transaction_id
    dlq_producer.produce.assert_not_called()

def test_parse_transaction_sends_original_bytes_to_dlq_on_invalid_payload():
    dlq_producer = MagicMock()
    raw = b'{"transaction_id": "not-a-full-transaction"}'

    with patch('txn_consumer.logging.error'):
        transaction = txn_consumer.parse_transaction(raw, dlq_producer, "transactions.dlq")

    assert transaction is None
    produce_kwargs = dlq_producer.produce.call_args.kwargs
    assert produce_kwargs["topic"] == "transactions.dlq"
    assert produce_kwargs["value"] == raw


# ── Function: evaluate_transaction ───────────────────────────────────────────

def build_structuring_series():
    return [
        build_transaction(f"0000000{i}-1111-1111-1111-111111111111").model_copy(update={
            "txn_type": TxnType.CASH_DEPOSIT,
            "counterparty_id": "ATM-12345678",
            "amount": Decimal("9000.00"),
            "event_time": datetime(2026, 1, 1, 12, 0, 0) + timedelta(hours=i),
        })
        for i in range(3)
    ]

def test_evaluate_transaction_writes_alert_to_db_before_publishing():
    calls = MagicMock()
    window_state = txn_consumer.WindowState(txn_consumer.window_config)

    with patch('txn_consumer.execute_values', side_effect=lambda *a, **k: calls.db_write()), \
         patch('txn_consumer.logging.warning'):
        calls.producer.produce.side_effect = lambda *a, **k: calls.publish()
        alerts = []
        for transaction in build_structuring_series():
            alerts = txn_consumer.evaluate_transaction(MagicMock(), window_state, calls.producer, "alerts", transaction)

    assert [a.rule_id for a in alerts] == ["R-001"]
    called = [c[0] for c in calls.mock_calls if c[0] in ("db_write", "publish")]
    assert called == ["db_write", "publish"]

def test_evaluate_transaction_returns_no_alert_below_threshold():
    window_state = txn_consumer.WindowState(txn_consumer.window_config)
    producer = MagicMock()

    with patch('txn_consumer.execute_values') as mock_execute_values:
        alerts = txn_consumer.evaluate_transaction(MagicMock(), window_state, producer, "alerts", build_transaction())

    assert alerts == []
    mock_execute_values.assert_not_called()
    producer.produce.assert_not_called()

def evaluate_series(window_state, transactions):
    """Runs transactions through evaluate_transaction and returns the alerts of each one."""
    with patch('txn_consumer.execute_values'), patch('txn_consumer.logging.warning'):
        return [
            txn_consumer.evaluate_transaction(MagicMock(), window_state, MagicMock(), "alerts", transaction)
            for transaction in transactions
        ]

def test_evaluate_transaction_does_not_alert_on_non_qualifying_transaction_after_threshold():
    """A card payment on an account whose structuring window is already above the threshold is not alerted."""
    window_state = txn_consumer.WindowState(txn_consumer.window_config)
    card_payment = build_transaction("99999999-1111-1111-1111-111111111111").model_copy(update={
        "event_time": datetime(2026, 1, 1, 15, 0, 0),
    })

    results = evaluate_series(window_state, build_structuring_series() + [card_payment])

    assert [a.rule_id for a in results[2]] == ["R-001"]
    assert results[3] == []

def test_evaluate_transaction_keeps_alerting_on_each_qualifying_transaction_after_threshold():
    """No suppression: every further in-band deposit while the window is above the threshold is alerted."""
    window_state = txn_consumer.WindowState(txn_consumer.window_config)
    series = build_structuring_series()
    next_deposit = series[0].model_copy(update={
        "transaction_id": "99999999-1111-1111-1111-111111111111",
        "event_time": datetime(2026, 1, 1, 16, 0, 0),
    })

    results = evaluate_series(window_state, series + [next_deposit])

    assert [a.rule_id for a in results[3]] == ["R-001"]

def test_evaluate_transaction_smurfing_alerts_only_on_small_transfer_out():
    window_state = txn_consumer.WindowState(txn_consumer.window_config)
    min_count = txn_consumer.scenario_config["smurfing"]["min_count"]
    transfers = [
        build_transaction(f"{i:08d}-2222-2222-2222-222222222222").model_copy(update={
            "txn_type": TxnType.TRANSFER_OUT,
            "counterparty_id": "33333333-3333-3333-3333-333333333333",
            "amount": Decimal("200.00"),
            "event_time": datetime(2026, 1, 1, 12, 0, 0) + timedelta(minutes=10 * i),
        })
        for i in range(min_count)
    ]
    card_payment = build_transaction("99999999-2222-2222-2222-222222222222").model_copy(update={
        "event_time": datetime(2026, 1, 1, 20, 0, 0),
    })

    results = evaluate_series(window_state, transfers + [card_payment])

    assert [a.rule_id for a in results[min_count - 1]] == ["R-002"]
    assert results[-1] == []


# ── Function: run ────────────────────────────────────────────────────────────

class FakeMessage:
    def __init__(self, value, partition=0):
        self._value = value
        self._partition = partition

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def error(self):
        return None


def run_with_messages(messages, poll_count):
    """Runs the consume loop for a fixed number of polls. Returns the fake consumer and DLQ producer."""
    consumer = MagicMock()
    consumer.poll.side_effect = messages + [None] * (poll_count - len(messages))
    dlq_producer = MagicMock()
    polls = iter(range(poll_count + 1))

    with patch('txn_consumer.flush_interval_ms', 0), \
         patch('txn_consumer.heartbeat_interval_seconds', 3600), \
         patch('txn_consumer.flush_buffer') as mock_flush, \
         patch('txn_consumer.logging'):
        txn_consumer.run(
            consumer, dlq_producer, MagicMock(), MagicMock(),
            txn_consumer.WindowState(txn_consumer.window_config),
            "test_group", "transactions", "alerts", "transactions.dlq",
            should_stop=lambda: next(polls) >= poll_count,
        )
    return consumer, dlq_producer, mock_flush

def test_run_flushes_and_commits_buffered_transaction():
    raw = json.dumps(build_transaction().model_dump(mode="json")).encode("utf-8")

    consumer, _, mock_flush = run_with_messages([FakeMessage(raw)], poll_count=2)

    flushed_buffer = mock_flush.call_args_list[0][0][1]
    assert mock_flush.call_count == 1
    assert consumer.commit.call_count == 1
    assert flushed_buffer == []     # Buffer is cleared after a successful flush

def test_run_flushes_on_idle_poll_without_waiting_for_next_message():
    raw = json.dumps(build_transaction().model_dump(mode="json")).encode("utf-8")
    consumer = MagicMock()
    consumer.poll.side_effect = [FakeMessage(raw), None]
    polls = iter(range(3))

    with patch('txn_consumer.flush_interval_ms', 10**9), \
         patch('txn_consumer.heartbeat_interval_seconds', 3600), \
         patch('txn_consumer.flush_buffer') as mock_flush, \
         patch('txn_consumer.logging'):
        started = {"flushed_while_running": False}

        def stop():
            n = next(polls)
            if n == 1:      # After the message poll: make the interval elapse for the following idle poll
                txn_consumer.flush_interval_ms = 0
            if n == 2:
                started["flushed_while_running"] = mock_flush.called
            return n >= 2

        txn_consumer.run(
            consumer, MagicMock(), MagicMock(), MagicMock(),
            txn_consumer.WindowState(txn_consumer.window_config),
            "test_group", "transactions", "alerts", "transactions.dlq",
            should_stop=stop,
        )

    assert started["flushed_while_running"] is True
    assert mock_flush.call_count == 1   # No final flush needed, buffer was already empty

def test_run_does_not_buffer_invalid_message():
    consumer, dlq_producer, mock_flush = run_with_messages([FakeMessage(b"not json")], poll_count=2)

    dlq_producer.produce.assert_called_once()
    mock_flush.assert_not_called()
    consumer.commit.assert_not_called()

def test_run_writes_final_heartbeat_after_final_commit_on_shutdown():
    raw = json.dumps(build_transaction().model_dump(mode="json")).encode("utf-8")
    consumer = MagicMock()
    consumer.poll.side_effect = [FakeMessage(raw)]
    calls = MagicMock()
    consumer.commit.side_effect = lambda **kwargs: calls.commit()
    polls = iter(range(2))

    with patch('txn_consumer.flush_interval_ms', 10**9), \
         patch('txn_consumer.heartbeat_interval_seconds', 3600), \
         patch('txn_consumer.flush_buffer'), \
         patch('txn_consumer.write_heartbeat', side_effect=lambda *args: calls.heartbeat(args[4])), \
         patch('txn_consumer.logging'):
        txn_consumer.run(
            consumer, MagicMock(), MagicMock(), MagicMock(),
            txn_consumer.WindowState(txn_consumer.window_config),
            "test_group", "transactions", "alerts", "transactions.dlq",
            should_stop=lambda: next(polls) >= 1,
        )

    # The buffered message is only flushed and committed by the shutdown path, the heartbeat must come after that commit.
    assert [c[0] for c in calls.mock_calls] == ["commit", "heartbeat"]
    assert calls.heartbeat.call_args[0][0] == {0: 1}   # messages_processed_by_partition
