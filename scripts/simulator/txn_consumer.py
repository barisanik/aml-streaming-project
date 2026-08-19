""" /scripts/simulator/txn_consumer.py
# ============================================================
#   Script: Transaction Consumer
# ============================================================
#   Purpose: 
#       Listen and record transactions.
#
#   Logic:
#    - Listens for transactions on the Redpanda message queue.
#    - If it does not match the expected schema, sends it to the DLQ (dead letter queue).
#    - Stores transactions on buffer, flushes them to Postgres on periodic intervals or when buffer is full.
#    - Updates heartbeat record periodically for the dashboard.
#
#   Usage:
#    - Change directory to project root
#    - Run following command: "python scripts/simulator/txn_consumer.py"
"""

import sys
import os
from pathlib import Path

import json
import yaml

from datetime import datetime

from pydantic import ValidationError
import logging
from dotenv import load_dotenv
import confluent_kafka
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

### Initial parameters ###

## PATHS
# Project Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)      # Create logs folder if it does not exist yet

# Config Paths
CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "configs" / "consumer_config.yml"
WINDOW_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "configs" / "window_config.yml"
SHARED_DIR = PROJECT_ROOT / "shared"

sys.path.insert(0, str(SHARED_DIR))
from schemas import Channel, Transaction, TxnType

# Logging parameters
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),                                                    # Enables visibility on console
        logging.FileHandler(LOG_DIR / "txn_consumer.log", encoding="utf-8"),        # Enables recording on a file
    ],
)

def load_config(path: Path) -> dict:
    """Loads yaml files specificly."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

## CONFIGS
config = load_config(CONFIG_PATH)
dlq_topic = config["topics"]["dlq_topic"]
heartbeat_interval_seconds = config["heartbeat"]["interval_seconds"]

# Consumer Config Parameters
consumer_group = config["kafka"]["consumer_group"]
transactions_topic = config["topics"]["transactions_topic"]
batch_max_size = config["batch"]["max_size"]
flush_interval_ms = config["batch"]["flush_interval_ms"]

# Kafka Consumer setup
consumer = confluent_kafka.Consumer({
    "bootstrap.servers": config["kafka"]["bootstrap_servers"],
    "group.id": consumer_group,
    "auto.offset.reset": "earliest",   # Start from beginning if no committed offset exists yet
    "enable.auto.commit": False,       # Commit manually, only after a successful batch flush
})
consumer.subscribe([transactions_topic])

# Kafka producer setup for dlq (dead letter queue). Aims to keep transactions that fail validation.
dlq_producer = confluent_kafka.Producer({
    "bootstrap.servers": config["kafka"]["bootstrap_servers"],
})

## FUNCTIONS
def dlq_delivery_callback(err, msg):
    """Logs writing error of DLQ messages."""
    if err is not None:
        logging.error(f"DLQ delivery failed | error={err}")

def flush_buffer(conn, buffer):
    """Writes buffered transactions to raw.transactions in one batch insert."""
    if not buffer:
        return

    rows = [
        (
            t.transaction_id,
            t.account_id,
            t.counterparty_id,
            t.amount,
            t.currency,
            t.txn_type.value,
            t.merchant_category,
            t.channel.value,
            t.city,
            t.country,
            t.event_time,
            t.produced_at,
            t.device_id,
        )
        for t in buffer
    ]

    sql = """
        INSERT INTO raw.transactions (
            transaction_id, 
            account_id, 
            counterparty_id, 
            amount, 
            currency,
            txn_type, 
            merchant_category, 
            channel, 
            city, 
            country,
            event_time, 
            produced_at, 
            device_id
        )
        VALUES %s
        ON CONFLICT (transaction_id) DO NOTHING
    """

    cur = conn.cursor()
    try:
        execute_values(cur, sql, rows)
    finally:
        cur.close()

def write_heartbeat(conn, consumer, consumer_group, topic, messages_processed_by_partition):
    """Writes current consumer progress per partition to ops.consumer_heartbeat."""
    partitions = consumer.assignment()
    if not partitions:
        return  # No partitions assigned yet, nothing to report

    consumer.position(partitions)  # Fills in the .offset field of each partition

    rows = [
        (
            consumer_group,
            topic,
            tp.partition,
            tp.offset,
            messages_processed_by_partition.get(tp.partition, 0),
            datetime.now(),
        )
        for tp in partitions
    ]

    sql = """
        INSERT INTO ops.consumer_heartbeat (
            consumer_group, 
            topic, 
            partition, 
            committed_offset,
            messages_processed, 
            heartbeat_at
        )
        VALUES %s
        ON CONFLICT (consumer_group, topic, partition)
        DO UPDATE SET
            committed_offset = EXCLUDED.committed_offset,
            messages_processed = EXCLUDED.messages_processed,
            heartbeat_at = EXCLUDED.heartbeat_at
    """

    cur = conn.cursor()
    try:
        execute_values(cur, sql, rows)
    finally:
        cur.close()

def main() -> None:

    ## Postgres connection setup
    conn = psycopg2.connect(
        host=       config["postgres"]["host"],
        port=       config["postgres"]["port"],
        user=       config["postgres"]["user"],
        password=   os.environ["APP_CONSUMER_DB_PASSWORD"],
        dbname=     config["postgres"]["dbname"],
    )
    conn.autocommit = True

    logging.info("Consumer started")
    buffer = [] # Transaction buffer
    last_flush_time = datetime.now()
    last_heartbeat_time = datetime.now()
    messages_processed_by_partition = {}  # {partition_number: count}

    try:
        while True:
            msg = consumer.poll(timeout=1.0)    # Checks for new transactions

            heartbeat_elapsed_s = (datetime.now() - last_heartbeat_time).total_seconds()
            if heartbeat_elapsed_s >= heartbeat_interval_seconds:   # If last heartbeat is [heartbeat_interval_seconds] seconds ago.
                try:
                    write_heartbeat(conn, consumer, consumer_group, transactions_topic, messages_processed_by_partition)    # Overwrite current timestamp as heartbeat (non-historical record).
                    last_heartbeat_time = datetime.now()                                                                    # Update the last heartbeat variable.
                except Exception as e:
                    logging.error(f"Heartbeat write failed | error={e}")

            if msg is None:     # If no message found in queue, loop again.
                continue
            
            if msg.error():     # If message is broken log message error and loop again.
                logging.error(f"Consumer error | error={msg.error()}")
                continue
            
            transaction_raw = msg.value()
            try:
                transaction_dict = json.loads(transaction_raw)
                transaction = Transaction(**transaction_dict)           # Validates transaction details
            except (json.JSONDecodeError, ValidationError) as e:        # If validation fails, write message to dead letter queue, and loop again.
                logging.error(f"Validation failed, transaction sent to DLQ | error={e}")
                try:
                    dlq_producer.produce(
                        topic=dlq_topic,
                        value=transaction_raw,   # Send the original raw bytes
                        callback=dlq_delivery_callback,
                    )
                    dlq_producer.poll(0)
                except Exception as dlq_err:
                    logging.critical(f"DLQ publish failed | error={dlq_err}")
                continue

            buffer.append(transaction)  # Add transaction to buffer, waiting for flush on batch max size limit or flush interval.

            partition = msg.partition() # Get partition number from Kafka.
            messages_processed_by_partition[partition] = messages_processed_by_partition.get(partition, 0) + 1  # Increase per-partition processed message count.

            # Calculate milliseconds since last flush.
            elapsed_ms = (datetime.now() - last_flush_time).total_seconds() * 1000
            if len(buffer) >= batch_max_size or elapsed_ms >= flush_interval_ms:    # If it is more than [flush_interval_ms] ms or buffer has [batch_max_size] or more transactions
                try:
                    flush_buffer(conn, buffer)                                          # Record transactions from buffer to Postgres.
                    consumer.commit()                                                   # Commit Kafka offset.
                    logging.info(f"Flushed {len(buffer)} records to raw.transactions")  # Log the flush result.
                    buffer.clear()                                                      # Clear buffer.
                    last_flush_time = datetime.now()                                    # Update last flush time.
                except Exception as e:
                    logging.critical(f"Batch flush failed, will retry next cycle | error={e}")

    except KeyboardInterrupt:
        logging.info("Shutdown signal received")
    finally:

        if buffer:                                          # If loop is broken and buffer is not empty, flush remaining transactions in buffer for the last time.
            try:
                flush_buffer(conn, buffer)
                consumer.commit()
                logging.info(f"Final flush: {len(buffer)} records written before shutdown")
            except Exception as e:
                logging.critical(f"Final flush failed | error={e}")

        conn.close()
        dlq_producer.flush()
        consumer.close()
        logging.info("Consumer closed")

if __name__ == "__main__":
    
    script_start_time = datetime.now()
    logging.info(f"Script started at {script_start_time}.")

    main()
    
    script_end_time = datetime.now()
    logging.info(f"Script ended at {script_end_time}. Execution duration: {script_end_time - script_start_time}")