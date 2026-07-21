"""    
# ============================================================================ #
    Script: Set Role Passwords
# ============================================================================ #
    Purpose: Set password for every rule
    Method: Connects as the Postgres superuser (gets credentials from .env). Run ALTER ROLE ... WITH PASSWORD ... SQL command for ever role. Gets passwords from .env file.
    Run command: python scripts/postgres/set_role_passwords.py (Run from the repo root with the venv active)
    
    Warning: Ensure that .env file includes following passwords:
    - APP_PRODUCER_DB_PASSWORD
    - APP_CONSUMER_DB_PASSWORD
    - APP_NOTIFIER_DB_PASSWORD
    - APP_DBT_DB_PASSWORD
    - APP_GRAFANA_DB_PASSWORD
# ============================================================================ #
"""

import os
import psycopg2
from pathlib import Path
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

### Initial parameters ###
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)      # create logs folder if it does not exist yet

# Logging parameters
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),                                                        # enables visibility on console
        logging.FileHandler(LOG_DIR / "set_role_passwords.log", encoding="utf-8"),      # enables recording on a file
    ],
)

script_start_time = datetime.now()
logging.info(f"Script started at {script_start_time}.")

try:
    conn = psycopg2.connect(
        host="localhost", 
        port=5432,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"], 
        dbname="aml_platform"
    )
    
    conn.autocommit = True
    cur = conn.cursor()

    roles = ["app_producer", "app_consumer", "app_notifier", "app_dbt", "app_grafana"]

    for role in roles:
        pw = os.environ[f"{role.upper()}_DB_PASSWORD"]
        cur.execute(f"ALTER ROLE {role} WITH PASSWORD %s", (pw,))
        logging.info(f"Password set for user: {role}")
    conn.close()
except Exception as e:
    logging.critical(f"Script failed: {e}")
    raise
finally:
    script_end_time = datetime.now()
    logging.info(f"Script ended at {script_end_time}. Execution duration: {script_end_time - script_start_time}")