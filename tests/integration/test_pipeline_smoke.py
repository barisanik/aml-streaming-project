# tests/integration/test_pipeline_smoke.py
"""
End-to-end smoke test against the running Redpanda and PostgreSQL services.

Flow:
 - Creates run-scoped topics (transactions, alerts, DLQ) so live topics and consumer groups are not touched.
 - Starts the real consume loop (txn_consumer.run) in a background thread.
 - Emits one structuring, one smurfing and one mule fan-in series with the real producer code
   (txn_producer.create_scenario / create_transaction_event), which also writes public.answer_key.
 - Adds noise: an out-of-scope card payment on the smurfing account, a normal account and one invalid payload.
 - Checks raw.transactions, raw.alerts, public.answer_key, ops.consumer_heartbeat, the alerts topic and the DLQ.
 - Restarts the consumer to document replay behaviour.
 - Deletes every row and topic it created.

Run:
    docker compose up -d
    pytest -m integration -s
"""

import json
import os
import random
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from statistics import median
from unittest.mock import patch

import numpy as np
import pytest
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'scripts', 'simulator'))

import txn_consumer
import txn_producer
from txn_consumer import Channel, Transaction, TxnType, WindowState

pytestmark = pytest.mark.integration

RUN_ID = uuid.uuid4().hex[:8]
BOOTSTRAP_SERVERS = txn_consumer.config["kafka"]["bootstrap_servers"]
TOPICS = {
    "transactions": f"smoke.{RUN_ID}.transactions",
    "alerts": f"smoke.{RUN_ID}.alerts",
    "dlq": f"smoke.{RUN_ID}.transactions.dlq",
}
CONSUMER_GROUP = f"smoke-{RUN_ID}"
SIM_START = datetime(2026, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
INVALID_PAYLOAD = b'{"transaction_id": "smoke-invalid-payload"}'
RULE_FAMILIES = ("structuring", "smurfing", "mule_fan_in")
EXPECTED_RULE = {
    "structuring": txn_consumer.structuring_rule_id,
    "smurfing": txn_consumer.smurfing_rule_id,
    "mule_fan_in": txn_consumer.mule_fan_in_rule_id,
}
RUN_TIMEOUT_SECONDS = 90


class RecordingProducer:
    """Forwards to a real Kafka producer and keeps every produced payload in order."""

    def __init__(self, producer):
        self._producer = producer
        self.values = []

    def produce(self, **kwargs):
        self.values.append(kwargs["value"])
        self._producer.produce(**kwargs)

    def poll(self, timeout):
        return self._producer.poll(timeout)

    def flush(self, timeout=30):
        return self._producer.flush(timeout)


# ── Connections ──────────────────────────────────────────────────────────────

def connect_as_consumer():
    return txn_consumer.connect_postgres(txn_consumer.config["postgres"], os.environ["APP_CONSUMER_DB_PASSWORD"])

def connect_as_producer():
    return txn_consumer.connect_postgres(txn_producer.config["postgres"], os.environ["APP_PRODUCER_DB_PASSWORD"])

def connect_as_superuser():
    """Used only to read public.answer_key and to delete the rows this test created."""
    postgres_config = dict(txn_consumer.config["postgres"], user=os.environ["POSTGRES_USER"])
    return txn_consumer.connect_postgres(postgres_config, os.environ["POSTGRES_PASSWORD"])

def fetch_all(conn, sql, params):
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        cur.close()

def count_rows(conn, table, transaction_ids):
    return fetch_all(conn, f"SELECT count(*) FROM {table} WHERE transaction_id = ANY(%s::uuid[])", (transaction_ids,))[0][0]


# ── Data generation ──────────────────────────────────────────────────────────

def emit_scenario(scenario_name, profile, start_time, producer_conn):
    """Emits a full scenario series the same way txn_producer.main() does, without the random trigger."""
    scenario_type = txn_producer.scenario["scenario_types"][scenario_name]
    details = txn_producer.create_scenario(start_time, profile, scenario_type)
    event_times = [start_time] + list(details["due_times"])
    transaction_ids = []
    for event_time in event_times:
        event = txn_producer.create_transaction_event(event_time, profile, is_poisoned=True, scenario_details=details, conn=producer_conn)
        transaction_ids.append(event["transaction_id"])
    return {"scenario_id": details["scenario_id"], "account_id": profile["account_id"], "transaction_ids": transaction_ids}

def build_noise_transaction(account_id, txn_type, counterparty_id, amount, event_time):
    return Transaction(
        transaction_id=str(uuid.uuid4()),
        account_id=account_id,
        counterparty_id=counterparty_id,
        amount=amount,
        currency="TRY",
        txn_type=txn_type,
        merchant_category="grocery",
        channel=Channel.MOBILE,
        city="Istanbul",
        country="TR",
        event_time=event_time,
        produced_at=datetime.now(timezone.utc),
    )

def produce_raw(producer, key, value):
    producer.produce(topic=TOPICS["transactions"], key=key.encode("utf-8"), value=value, callback=txn_producer.delivery_callback)
    producer.poll(0)

def replay_rules_in_memory(payloads):
    """Expected (transaction_id, rule_id) pairs: runs the produced payloads through WindowState and the rule functions."""
    window_state = WindowState(txn_consumer.window_config)
    checks = (
        (txn_consumer.structuring_rule_id, txn_consumer.check_structuring, txn_consumer.qualifies_structuring),
        (txn_consumer.smurfing_rule_id, txn_consumer.check_smurfing, txn_consumer.qualifies_smurfing),
        (txn_consumer.mule_fan_in_rule_id, txn_consumer.check_mule_fan_in, txn_consumer.qualifies_mule_fan_in),
    )
    expected = set()
    for raw in payloads:
        try:
            transaction = Transaction(**json.loads(raw))
        except Exception:
            continue
        for family in RULE_FAMILIES:
            window_state.add_transaction(family, transaction)
        for rule_id, check, qualifies in checks:
            if qualifies(transaction) and check(window_state, transaction.account_id)[0]:
                expected.add((transaction.transaction_id, rule_id))
    return expected


# ── Consumer runs ────────────────────────────────────────────────────────────

def start_consumer(group_id, done):
    """Starts txn_consumer.run in a thread with its own clients. done(conn) decides when to stop."""
    consumer = txn_consumer.create_consumer(BOOTSTRAP_SERVERS, group_id, TOPICS["transactions"])
    dlq_producer = txn_consumer.create_producer(BOOTSTRAP_SERVERS)
    alert_producer = txn_consumer.create_producer(BOOTSTRAP_SERVERS)
    conn = connect_as_consumer()
    check_conn = connect_as_consumer()
    deadline = time.monotonic() + RUN_TIMEOUT_SECONDS
    result = {"timed_out": False}

    def should_stop():
        if time.monotonic() > deadline:
            result["timed_out"] = True
            return True
        return done(check_conn)

    def target():
        try:
            txn_consumer.run(
                consumer, dlq_producer, alert_producer, conn, WindowState(txn_consumer.window_config),
                group_id, TOPICS["transactions"], TOPICS["alerts"], TOPICS["dlq"], should_stop=should_stop,
            )
        finally:
            dlq_producer.flush(10)
            alert_producer.flush(10)
            consumer.close()
            conn.close()
            check_conn.close()

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread, consumer, result

def wait_for_assignment(consumer, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if consumer.assignment():
            return
        time.sleep(0.2)
    pytest.fail("Consumer did not get a partition assignment")

def read_topic(topic, expected_count, timeout=30):
    consumer = txn_consumer.create_consumer(BOOTSTRAP_SERVERS, f"smoke-reader-{uuid.uuid4().hex[:8]}", topic)
    messages = []
    deadline = time.monotonic() + timeout
    try:
        while len(messages) < expected_count and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                messages.append(msg.value())
        # One extra poll window to catch unexpected extra messages.
        msg = consumer.poll(2.0)
        if msg is not None and not msg.error():
            messages.append(msg.value())
    finally:
        consumer.close()
    return messages


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def smoke_topics():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    try:
        admin.list_topics(timeout=10)
    except Exception as e:
        pytest.fail(f"Redpanda is not reachable at {BOOTSTRAP_SERVERS}. Run 'docker compose up -d' first | error={e}")

    partitions = txn_producer.config["topics"]["transactions_partitions"]
    futures = admin.create_topics([
        NewTopic(TOPICS["transactions"], num_partitions=partitions),
        NewTopic(TOPICS["alerts"], num_partitions=1),
        NewTopic(TOPICS["dlq"], num_partitions=1),
    ])
    for future in futures.values():
        future.result()

    yield

    for future in admin.delete_topics(list(TOPICS.values())).values():
        try:
            future.result()
        except Exception:
            pass

@pytest.fixture(scope="module")
def smoke_run(smoke_topics):
    random.seed(42)
    np.random.seed(42)
    profiles = random.sample(txn_producer.profiles, 4)
    structuring_profile, smurfing_profile, mule_profile, normal_profile = profiles

    producer_conn = connect_as_producer()
    superuser_conn = connect_as_superuser()
    recorder = RecordingProducer(txn_producer.create_producer(BOOTSTRAP_SERVERS))
    scenarios = {}
    noise_ids = []

    try:
        with patch.object(txn_consumer, "heartbeat_interval_seconds", 1):
            state = {"expected_transactions": None, "expected_alerts": None}

            def first_run_done(conn):
                if state["expected_transactions"] is None:
                    return False
                ids = state["transaction_ids"]
                return (count_rows(conn, "raw.transactions", ids) >= state["expected_transactions"]
                        and count_rows(conn, "raw.alerts", ids) >= state["expected_alerts"])

            thread, consumer, result = start_consumer(CONSUMER_GROUP, first_run_done)
            wait_for_assignment(consumer)

            with patch.object(txn_producer, "producer", recorder), \
                 patch.dict(txn_producer.config["topics"], {"transactions_topic": TOPICS["transactions"]}):
                scenarios["structuring"] = emit_scenario("structuring", structuring_profile, SIM_START, producer_conn)
                scenarios["smurfing"] = emit_scenario("smurfing", smurfing_profile, SIM_START, producer_conn)
                scenarios["mule_fan_in"] = emit_scenario("mule_fan_in", mule_profile, SIM_START, producer_conn)

                noise = [
                    # Small but not transfer_out: must not count towards smurfing.
                    build_noise_transaction(smurfing_profile["account_id"], TxnType.CARD_PAYMENT, "MER-00000001", Decimal("100.00"), SIM_START + timedelta(minutes=1)),
                    build_noise_transaction(normal_profile["account_id"], TxnType.CARD_PAYMENT, "MER-00000002", Decimal("75.50"), SIM_START),
                ]
                for transaction in noise:
                    noise_ids.append(transaction.transaction_id)
                    produce_raw(recorder, transaction.account_id, json.dumps(transaction.model_dump(mode="json")).encode("utf-8"))
                produce_raw(recorder, normal_profile["account_id"], INVALID_PAYLOAD)
                recorder.flush()

            injected_ids = [tid for s in scenarios.values() for tid in s["transaction_ids"]]
            all_ids = injected_ids + noise_ids
            expected_alerts = replay_rules_in_memory(recorder.values)
            state.update(transaction_ids=all_ids, expected_alerts=len(expected_alerts), expected_transactions=len(all_ids))

            thread.join(RUN_TIMEOUT_SECONDS + 15)
            assert not result["timed_out"], "First consumer run timed out before all rows arrived"

        verify_conn = connect_as_consumer()
        alerts = fetch_all(verify_conn, """
            SELECT alert_id::text, transaction_id::text, account_id::text, rule_id, window_summary,
                   detection_latency_ms, alert_time, event_time
            FROM raw.alerts WHERE transaction_id = ANY(%s::uuid[])
        """, (all_ids,))
        stored_transaction_ids = {r[0] for r in fetch_all(verify_conn, "SELECT transaction_id::text FROM raw.transactions WHERE transaction_id = ANY(%s::uuid[])", (all_ids,))}
        heartbeat = fetch_all(verify_conn, "SELECT partition, committed_offset, messages_processed FROM ops.consumer_heartbeat WHERE consumer_group = %s", (CONSUMER_GROUP,))
        answer_key = fetch_all(superuser_conn, "SELECT transaction_id::text, scenario_id::text, scenario_type FROM public.answer_key WHERE scenario_id = ANY(%s::uuid[])",
                               ([s["scenario_id"] for s in scenarios.values()],))

        yield {
            "scenarios": scenarios,
            "noise_ids": noise_ids,
            "all_ids": all_ids,
            "injected_ids": injected_ids,
            "expected_alerts": expected_alerts,
            "alerts": alerts,
            "stored_transaction_ids": stored_transaction_ids,
            "heartbeat": heartbeat,
            "answer_key": answer_key,
            "verify_conn": verify_conn,
            "normal_account_id": normal_profile["account_id"],
        }
        verify_conn.close()

    finally:
        ids = [tid for s in scenarios.values() for tid in s["transaction_ids"]] + noise_ids
        cur = superuser_conn.cursor()
        cur.execute("DELETE FROM raw.alerts WHERE transaction_id = ANY(%s::uuid[])", (ids,))
        cur.execute("DELETE FROM raw.transactions WHERE transaction_id = ANY(%s::uuid[])", (ids,))
        cur.execute("DELETE FROM public.answer_key WHERE scenario_id = ANY(%s::uuid[])", ([s["scenario_id"] for s in scenarios.values()],))
        cur.execute("DELETE FROM ops.consumer_heartbeat WHERE consumer_group LIKE %s", (f"smoke-{RUN_ID}%",))
        cur.close()
        superuser_conn.close()
        producer_conn.close()


# ── Tests: first run ─────────────────────────────────────────────────────────

def test_all_valid_transactions_are_stored(smoke_run):
    assert smoke_run["stored_transaction_ids"] == set(smoke_run["all_ids"])

def test_invalid_payload_goes_to_dlq_as_original_bytes(smoke_run):
    assert read_topic(TOPICS["dlq"], expected_count=1) == [INVALID_PAYLOAD]

def test_answer_key_has_every_injected_transaction(smoke_run):
    labels = {tid: scenario_type for tid, _, scenario_type in smoke_run["answer_key"]}
    assert set(labels) == set(smoke_run["injected_ids"])
    for scenario_type, scenario in smoke_run["scenarios"].items():
        assert {labels[tid] for tid in scenario["transaction_ids"]} == {scenario_type}
    assert not set(labels) & set(smoke_run["noise_ids"])

def test_live_alerts_match_in_memory_rule_replay(smoke_run):
    """Nothing lost or duplicated between broker, consumer and raw.alerts."""
    live = {(transaction_id, rule_id) for _, transaction_id, _, rule_id, *_ in smoke_run["alerts"]}
    assert len(smoke_run["alerts"]) == len(live)
    assert live == smoke_run["expected_alerts"]

@pytest.mark.parametrize("scenario_type", ["structuring", "smurfing", "mule_fan_in"])
def test_each_scenario_triggers_only_its_expected_rule(smoke_run, scenario_type):
    scenario = smoke_run["scenarios"][scenario_type]
    rule_ids = [rule_id for _, transaction_id, _, rule_id, *_ in smoke_run["alerts"] if transaction_id in scenario["transaction_ids"]]
    assert rule_ids, f"{scenario_type} scenario produced no alert"
    assert set(rule_ids) == {EXPECTED_RULE[scenario_type]}

def test_normal_account_has_no_alert(smoke_run):
    assert not [a for a in smoke_run["alerts"] if a[2] == smoke_run["normal_account_id"]]

def test_alerts_topic_carries_only_alerts_stored_in_db(smoke_run):
    db_alert_ids = {a[0] for a in smoke_run["alerts"]}
    published = [json.loads(v) for v in read_topic(TOPICS["alerts"], expected_count=len(db_alert_ids))]
    assert {p["alert_id"] for p in published} == db_alert_ids
    assert len(published) == len(db_alert_ids)

def test_smurfing_alert_lists_only_transfer_out(smoke_run):
    smurfing_alerts = [a for a in smoke_run["alerts"] if a[3] == txn_consumer.smurfing_rule_id]
    noise_id = smoke_run["noise_ids"][0]
    for alert in smurfing_alerts:
        listed = alert[4]["transactions"]
        assert {t["txn_type"] for t in listed} == {"transfer_out"}
        assert noise_id not in {t["transaction_id"] for t in listed}

def test_mule_fan_in_alert_metrics_explain_the_trigger(smoke_run):
    rules = txn_producer.scenario["scenario_types"]["mule_fan_in"]
    mule_alerts = [a for a in smoke_run["alerts"] if a[3] == txn_consumer.mule_fan_in_rule_id]
    assert mule_alerts
    for alert in mule_alerts:
        metrics = alert[4]["metrics"]
        assert metrics["qualifying_transaction_count"] == len(alert[4]["transactions"])
        assert metrics["qualifying_transaction_count"] >= rules["min_count"]
        assert metrics["distinct_counterparty_count"] >= rules["min_distinct_counterparties"]
        assert Decimal(metrics["total_amount"]) >= rules["min_total_amount"]
        window = datetime.fromisoformat(metrics["window_end"]) - datetime.fromisoformat(metrics["window_start"])
        assert window <= timedelta(hours=rules["window_hours"])
        assert metrics["thresholds"]["min_total_amount"] == rules["min_total_amount"]

def test_detection_latency_is_wall_clock_and_bounded(smoke_run):
    latencies = sorted(a[5] for a in smoke_run["alerts"])
    for alert in smoke_run["alerts"]:
        assert alert[6].utcoffset() == timedelta(0)             # alert_time is UTC wall-clock
    assert latencies[0] >= 0
    assert latencies[-1] < RUN_TIMEOUT_SECONDS * 1000
    print(f"\n[smoke] alerts={len(latencies)} latency_ms p50={median(latencies)} max={latencies[-1]}")

def test_heartbeat_reports_consumer_progress(smoke_run):
    assert smoke_run["heartbeat"]
    assert sum(r[2] for r in smoke_run["heartbeat"]) > 0


# ── Tests: restart and replay ────────────────────────────────────────────────

def test_restart_with_committed_offsets_creates_no_new_alerts(smoke_run):
    """Same consumer group: committed offsets are respected, nothing is reprocessed."""
    alerts_before = count_rows(smoke_run["verify_conn"], "raw.alerts", smoke_run["all_ids"])
    idle_polls = iter(range(8))
    thread, consumer, _ = start_consumer(CONSUMER_GROUP, lambda conn: next(idle_polls) >= 7)
    thread.join(RUN_TIMEOUT_SECONDS + 15)

    assert count_rows(smoke_run["verify_conn"], "raw.alerts", smoke_run["all_ids"]) == alerts_before

def test_replay_without_committed_offsets_duplicates_alerts_not_transactions(smoke_run):
    """
    A new consumer group stands in for a crash before the offset commit: every message is consumed again
    with an empty window state. Transaction inserts stay idempotent. Alerts are written again with new
    alert_ids, because alert_id is random and alerts are not deduplicated.
    """
    ids = smoke_run["all_ids"]
    expected_alerts = len(smoke_run["expected_alerts"])
    alerts_before = count_rows(smoke_run["verify_conn"], "raw.alerts", ids)

    thread, _, result = start_consumer(f"{CONSUMER_GROUP}-replay", lambda conn: count_rows(conn, "raw.alerts", ids) >= alerts_before + expected_alerts)
    thread.join(RUN_TIMEOUT_SECONDS + 15)

    assert not result["timed_out"]
    assert count_rows(smoke_run["verify_conn"], "raw.transactions", ids) == len(ids)
    alerts_after = fetch_all(smoke_run["verify_conn"], "SELECT alert_id, transaction_id::text, rule_id FROM raw.alerts WHERE transaction_id = ANY(%s::uuid[])", (ids,))
    assert len(alerts_after) == alerts_before + expected_alerts
    assert len({a[0] for a in alerts_after}) == len(alerts_after)
    print(f"\n[smoke] replay duplicated alerts: before={alerts_before} after={len(alerts_after)}")
