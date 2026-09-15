"""scripts/simulator/notifier.py
# ============================================================
#   Script: Notifier
# ============================================================
#   Purpose:
#       Listen for AML alerts and send webhook notifications.
#
#   Usage:
#    - Change directory to project root
#    - Run following command: "python scripts/simulator/notifier.py"
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from enum import Enum
from pathlib import Path

import confluent_kafka
import requests
import yaml
from dotenv import load_dotenv
from pydantic import ValidationError


# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = PROJECT_ROOT / "configs" / "notifier_config.yml"
ENV_PATH = PROJECT_ROOT / ".env"
SHARED_DIR = PROJECT_ROOT / "shared"

load_dotenv(ENV_PATH)

sys.path.insert(0, str(SHARED_DIR))

from schemas import Alert

# Logging parameters
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "notifier.log", encoding="utf-8"),
    ],
)


class MessageResult(str, Enum):
    """Result of processing one alert message."""

    DELIVERED = "delivered"
    VALIDATION_FAILED = "validation_failed"
    DELIVERY_FAILED = "delivery_failed"


def load_config(path: Path) -> dict:
    """Load a YAML config file."""
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def get_optional_env(variable_name: str) -> str | None:
    """Read an optional environment variable."""
    return os.getenv(variable_name) or None


def build_consumer(config: dict) -> confluent_kafka.Consumer:
    """Create the notifier consumer with manual offset commits."""
    consumer = confluent_kafka.Consumer(
        {
            "bootstrap.servers": config["kafka"]["bootstrap_servers"],
            "group.id": config["kafka"]["consumer_group"],
            "auto.offset.reset": config["kafka"]["auto_offset_reset"],
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([config["topics"]["alerts_topic"]])
    return consumer


def format_alert_message(alert: Alert) -> str:
    """Build the text sent to Slack or Discord."""
    return "\n".join(
        [
            "AML alert triggered",
            f"Rule: {alert.rule_name}",
            f"Severity: {alert.severity.value}",
            f"Account ID: {alert.account_id}",
            f"Transaction ID: {alert.transaction_id}",
            f"Alert time: {alert.alert_time.isoformat()}",
        ]
    )


def build_webhook_payload(alert: Alert, webhook_type: str) -> dict:
    """Build the JSON body expected by the selected webhook."""
    message = format_alert_message(alert)
    if webhook_type == "slack":
        return {"text": message}
    if webhook_type == "discord":
        return {"content": message}
    raise ValueError(f"Unsupported webhook type: {webhook_type}")


def send_webhook_notification(
    alert: Alert,
    webhook_config: dict,
    webhook_url: str,
) -> bool:
    """Send one alert to the webhook with configured retries."""
    payload = build_webhook_payload(alert, webhook_config["type"])
    retry_config = webhook_config["retry"]
    max_attempts = retry_config["max_attempts"]
    backoff_seconds = retry_config["backoff_seconds"]

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(
                webhook_url,
                json=payload,
                timeout=webhook_config["timeout_seconds"],
            )
        except requests.RequestException as error:
            error_response = getattr(error, "response", None)
            response_body = (
                error_response.text if error_response is not None else "<no response body>"
            )
            logging.error(
                "Webhook request failed | alert_id=%s | attempt=%s/%s | "
                "error=%s | response_body=%s",
                alert.alert_id,
                attempt,
                max_attempts,
                error,
                response_body,
            )
        else:
            if 200 <= response.status_code < 300:
                logging.info(
                    "Webhook notification sent | alert_id=%s | webhook_type=%s",
                    alert.alert_id,
                    webhook_config["type"],
                )
                return True

            logging.error(
                "Webhook request failed | alert_id=%s | attempt=%s/%s | "
                "status_code=%s | response_body=%s",
                alert.alert_id,
                attempt,
                max_attempts,
                response.status_code,
                response.text,
            )

        if attempt < max_attempts:
            time.sleep(backoff_seconds * attempt)

    return False


def send_smtp_notification(alert: Alert, smtp_config: dict) -> None:
    """Log that SMTP is enabled but not implemented yet."""
    logging.warning(
        "SMTP notification is enabled but not implemented | alert_id=%s | host=%s | port=%s",
        alert.alert_id,
        smtp_config["host"],
        smtp_config["port"],
    )


def process_alert_message(
    alert_raw: bytes,
    webhook_config: dict,
    webhook_url: str | None,
    smtp_config: dict,
) -> MessageResult:
    """Validate and deliver one raw alert message."""
    try:
        alert_data = json.loads(alert_raw)
        alert = Alert(**alert_data)
    except (json.JSONDecodeError, UnicodeDecodeError, ValidationError, TypeError) as error:
        logging.error("Alert validation failed, message skipped | error=%s", error)
        return MessageResult.VALIDATION_FAILED

    if webhook_url:
        if not send_webhook_notification(alert, webhook_config, webhook_url):
            return MessageResult.DELIVERY_FAILED
    else:
        logging.info(
            "Webhook URL is empty, notification skipped | alert_id=%s",
            alert.alert_id,
        )

    if smtp_config["enabled"]:
        send_smtp_notification(alert, smtp_config)

    return MessageResult.DELIVERED


def main() -> None:
    """Run the alert notification consumer."""
    config = load_config(CONFIG_PATH)
    webhook_url = get_optional_env(config["webhook"]["url_env_var"])
    consumer = build_consumer(config)

    logging.info("Notifier started")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                logging.error("Consumer error | error=%s", msg.error())
                continue

            result = process_alert_message(
                msg.value(),
                config["webhook"],
                webhook_url,
                config["smtp"],
            )

            if result == MessageResult.DELIVERY_FAILED:
                alert_id = json.loads(msg.value()).get("alert_id", "<unknown>")
                logging.critical(
                    "Webhook delivery failed after all attempts; offset will be committed | "
                    "alert_id=%s | topic=%s | partition=%s | offset=%s",
                    alert_id,
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )

            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        logging.info("Shutdown signal received")
    finally:
        consumer.close()
        logging.info("Notifier closed")


if __name__ == "__main__":
    script_start_time = datetime.now()
    logging.info(f"Script started at {script_start_time}.")

    main()

    script_end_time = datetime.now()
    logging.info(
        f"Script ended at {script_end_time}. "
        f"Execution duration: {script_end_time - script_start_time}"
    )
