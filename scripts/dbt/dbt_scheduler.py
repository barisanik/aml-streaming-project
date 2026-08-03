""" /scripts/dbt/dbt_scheduler.py
# ============================================================
#   Script: dbt Scheduler
# ============================================================
#   Purpose: 
#       Run increnmental load on every 60 secs.
#
#   Logic:
#    - 
#
#   Usage:
#    - 
"""

from dbt.cli.main import dbtRunner
import time
import logging
from pathlib import Path
import yaml

### Initial parameters ###

## PATHS
# Project Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "aml_platform"
CONFIG_DIR = PROJECT_ROOT / "configs" / "dbt_scheduler_config.yml"

# Logging parameters
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "dbt_scheduler.log", encoding="utf-8"),
    ],
)

## FUNCTIONS

# Reads dbt_scheduler_config.yml.
def load_config(path: Path) -> dict:
    """Loads yaml files specificly."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def run_dbt_with_retry() -> bool:
    """Runs dbt models, retrying up to MAX_RETRIES times on failure."""
    for attempt in range(1, MAX_RETRIES + 1):
        runner = dbtRunner()
        run_result = runner.invoke(["run", "--project-dir", str(DBT_PROJECT_DIR)])
        test_result = runner.invoke(["test", "--project-dir", str(DBT_PROJECT_DIR)])

        setup_logging()  # dbt just reset the root logger, restore logging settings

        if run_result.success and test_result.success:
            logging.info(f"dbt run and test succeeded on attempt {attempt}")
            return True

        if not run_result.success:
            logging.error(f"dbt run failed on attempt {attempt}/{MAX_RETRIES}")
        if not test_result.success:
            logging.error(f"dbt test failed on attempt {attempt}/{MAX_RETRIES}")

    return False

def setup_logging() -> None:
    """Configures the root logger."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOG_DIR / "dbt_scheduler.log", encoding="utf-8"),
        ],
        force=True,
    )

if __name__ == "__main__":
    # Get config parameters
    config = load_config(CONFIG_DIR)

    # Loop parameters
    RUN_INTERVAL_SECONDS = config["loop"]["run_interval_seconds"]
    MAX_RETRIES = config["loop"]["max_retries"]

    # Adjust logging settings
    setup_logging() 
    logging.info("dbt scheduler started.")

    try:
        while True:
            success = run_dbt_with_retry()
            if not success:
                logging.critical("dbt run failed after all retries, will try again next cycle.")

            time.sleep(RUN_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logging.info("Shutdown signal received, dbt scheduler stopped.")