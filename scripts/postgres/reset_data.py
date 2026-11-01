""" /scripts/postgres/reset_data.py
# ============================================================================ #
    Script: Reset Data
# ============================================================================ #
    Purpose: Clears all run data so a new simulation starts from an empty state.
    Method:
     - Empties raw.transactions, raw.alerts, public.answer_key and ops.consumer_heartbeat with TRUNCATE.
       Tables, roles and grants stay as they are, so the DDL does not need to run again.
     - Drops every dbt table and view in the analytics schema. The schema and its grants stay.
       dbt rebuilds the models on the next run, dbt_scheduler.py loads the seeds again at startup.
     - With --kafka, also deletes the consumer groups and recreates the topics empty.
    Run command:
        python scripts/postgres/reset_data.py            (asks for confirmation)
        python scripts/postgres/reset_data.py --yes      (no confirmation)
        python scripts/postgres/reset_data.py --kafka    (also resets Redpanda topics)
    Stop the producer, consumer, notifier and dbt scheduler before running it.
# ============================================================================ #
"""

import argparse
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import psycopg2
import yaml
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

### Initial parameters ###
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "configs"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)      # create logs folder if it does not exist yet

RUN_TABLES = ["raw.transactions", "raw.alerts", "public.answer_key", "ops.consumer_heartbeat"]

# Logging parameters
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),                                                # enables visibility on console
        logging.FileHandler(LOG_DIR / "reset_data.log", encoding="utf-8"),      # enables recording on a file
    ],
)


def load_config(name: str) -> dict:
    with open(CONFIG_DIR / name, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def reset_postgres(conn) -> None:
    """Truncates the run tables and drops the dbt relations in the analytics schema."""
    cur = conn.cursor()
    cur.execute(f"TRUNCATE {', '.join(RUN_TABLES)}")
    logging.info(f"Tables truncated | tables={RUN_TABLES}")

    cur.execute("""
        SELECT table_name, table_type FROM information_schema.tables
        WHERE table_schema = 'analytics'
    """)
    for table_name, table_type in cur.fetchall():
        kind = "VIEW" if table_type == "VIEW" else "TABLE"
        cur.execute(f'DROP {kind} IF EXISTS analytics."{table_name}" CASCADE')
        logging.info(f"Dropped | relation=analytics.{table_name}")
    cur.close()

def count_rows(conn) -> dict:
    cur = conn.cursor()
    counts = {}
    for table in RUN_TABLES:
        cur.execute(f"SELECT count(*) FROM {table}")
        counts[table] = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'analytics'")
    counts["analytics relations"] = cur.fetchone()[0]
    cur.close()
    return counts

def reset_kafka() -> None:
    """Deletes the consumer groups and recreates the topics empty."""
    producer_config = load_config("producer_config.yml")
    consumer_config = load_config("consumer_config.yml")
    notifier_config = load_config("notifier_config.yml")
    topics_config = producer_config["topics"]
    admin = AdminClient({"bootstrap.servers": producer_config["kafka"]["bootstrap_servers"]})

    groups = [consumer_config["kafka"]["consumer_group"], notifier_config["kafka"]["consumer_group"]]
    for group, future in admin.delete_consumer_groups(groups).items():
        try:
            future.result()
            logging.info(f"Consumer group deleted | group={group}")
        except Exception as e:
            logging.info(f"Consumer group not deleted, skipped | group={group} | error={e}")

    topics = [
        NewTopic(topics_config["transactions_topic"], num_partitions=topics_config["transactions_partitions"]),
        NewTopic(topics_config["alerts_topic"], num_partitions=topics_config["alerts_partitions"]),
        NewTopic(topics_config["dlq_topic"], num_partitions=topics_config["dlq_partitions"]),
    ]
    for topic, future in admin.delete_topics([t.topic for t in topics]).items():
        try:
            future.result()
            logging.info(f"Topic deleted | topic={topic}")
        except Exception as e:
            logging.info(f"Topic not deleted, skipped | topic={topic} | error={e}")

    # Topic deletion is asynchronous, so creation is retried until the old topic is gone.
    pending = topics
    for attempt in range(1, 11):
        failed = []
        for topic in pending:
            try:
                admin.create_topics([topic])[topic.topic].result()
                logging.info(f"Topic created | topic={topic.topic}")
            except Exception as e:
                failed.append(topic)
                last_error = e
        if not failed:
            return
        pending = failed
        time.sleep(1)
    raise RuntimeError(f"Topics could not be created | topics={[t.topic for t in pending]} | error={last_error}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clears all run data for a fresh simulation.")
    parser.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    parser.add_argument("--kafka", action="store_true", help="also delete consumer groups and recreate topics")
    args = parser.parse_args()

    load_dotenv(PROJECT_ROOT / ".env")
    pg_config = load_config("postgres_config.yml")["postgres"]

    if not args.yes:
        scope = "PostgreSQL run data and Redpanda topics" if args.kafka else "PostgreSQL run data"
        answer = input(f"This deletes all {scope} in '{pg_config['dbname']}'. Type 'yes' to continue: ")
        if answer.strip().lower() != "yes":
            print("Cancelled.")
            raise SystemExit(0)

    script_start_time = datetime.now()
    logging.info(f"Script started at {script_start_time}.")
    try:
        conn = psycopg2.connect(
            host=pg_config["host"],
            port=pg_config["port"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            dbname=pg_config["dbname"],
        )
        conn.autocommit = True
        reset_postgres(conn)
        logging.info(f"Row counts after reset | {count_rows(conn)}")
        conn.close()

        if args.kafka:
            reset_kafka()
    except Exception as e:
        logging.critical(f"Script failed: {e}")
        raise
    finally:
        script_end_time = datetime.now()
        logging.info(f"Script ended at {script_end_time}. Execution duration: {script_end_time - script_start_time}")
