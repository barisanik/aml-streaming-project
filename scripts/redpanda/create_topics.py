""" /scripts/redpanda/create_topics.py
# ============================================================================ #
    Script: Create Redpanda Topics
# ============================================================================ #
    Purpose: Create all Redpanda topics needed by the platform, if they do not exist yet.
    Method: Reads topic names and partition counts from producer_config.yml. Uses confluent_kafka AdminClient to create each topic. Safe to run multiple times, existing topics are skipped.
    Run command: python scripts/redpanda/create_topics.py (Run from the repo root with the venv active)
# ============================================================================ #
"""

import logging
from datetime import datetime
from pathlib import Path

import yaml
from confluent_kafka.admin import AdminClient, NewTopic

### Initial parameters ###
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)      # create logs folder if it does not exist yet

CONFIG_PATH = PROJECT_ROOT / "configs" / "producer_config.yml"

# Logging parameters
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),                                                  # enables visibility on console
        logging.FileHandler(LOG_DIR / "create_topics.log", encoding="utf-8"),     # enables recording on a file
    ],
)

# Reads producer_config.yml.
def load_config(path: Path) -> dict:
    """Loads yaml files specificly."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def build_topic_list(config: dict) -> list[NewTopic]:
    """Builds NewTopic objects using topic names and partition counts from config."""
    topics_config = config["topics"]
    return [
        NewTopic(topics_config["transactions_topic"], num_partitions=topics_config["transactions_partitions"]),
        NewTopic(topics_config["alerts_topic"], num_partitions=topics_config["alerts_partitions"]),
        NewTopic(topics_config["dlq_topic"], num_partitions=topics_config["dlq_partitions"]),
    ]

def create_topics(admin_client: AdminClient, topic_list: list[NewTopic]) -> None:
    """Creates each topic and logs the result. Skips a topic if it already exists."""
    futures = admin_client.create_topics(topic_list)

    for topic_name, future in futures.items():
        try:
            future.result()
            logging.info(f"Topic created | topic={topic_name}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logging.info(f"Topic already exists, skipped | topic={topic_name}")
            else:
                logging.critical(f"Topic creation failed | topic={topic_name} | error={e}")

if __name__ == "__main__":

    script_start_time = datetime.now()
    logging.info(f"Script started at {script_start_time}.")

    try:
        config = load_config(CONFIG_PATH)
        admin_client = AdminClient({"bootstrap.servers": config["kafka"]["bootstrap_servers"]})
        topic_list = build_topic_list(config)
        create_topics(admin_client, topic_list)
    except Exception as e:
        logging.critical(f"Script failed: {e}")
        raise
    finally:
        script_end_time = datetime.now()
        logging.info(f"Script ended at {script_end_time}. Execution duration: {script_end_time - script_start_time}")