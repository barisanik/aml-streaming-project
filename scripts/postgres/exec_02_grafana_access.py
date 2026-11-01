""" /scripts/postgres/exec_02_grafana_access.py
# ============================================================================ #
    Script: Execute 02 Grafana Access SQL
# ============================================================================ #
    Purpose: Execute 02_grafana_access.sql against the running Postgres container.
    Method: Connects as the Postgres superuser (gets credentials from .env). The SQL file only adds a column, an index and a grant, it does not drop any data.
    Run command: python scripts/postgres/exec_02_grafana_access.py (Run from the repo root with the venv active)
# ============================================================================ #
"""

import os
from pathlib import Path
import psycopg2
import yaml
from dotenv import load_dotenv
import logging
from datetime import datetime

### Initial parameters ###
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = PROJECT_ROOT / "configs" / "postgres_config.yml"
SQL_PATH = PROJECT_ROOT / "postgre_sql_files" / "02_grafana_access.sql"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)      # create logs folder if it does not exist yet

# Logging parameters
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),                                                        # enables visibility on console
        logging.FileHandler(LOG_DIR / "exec_02_grafana_access.log", encoding="utf-8"),  # enables recording on a file
    ],
)

load_dotenv()

script_start_time = datetime.now()
logging.info(f"Script started at {script_start_time}.")

try:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        pg_config = yaml.safe_load(f)["postgres"]

    conn = psycopg2.connect(
        host=pg_config["host"],
        port=pg_config["port"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=pg_config["dbname"],
    )
    conn.autocommit = True

    with open(SQL_PATH, encoding="utf-8") as f:
        sql = f.read()

    cur = conn.cursor()
    cur.execute(sql)
    conn.close()
    logging.info("OK - Grafana access migration executed.")
except Exception as e:
    logging.critical(f"Script failed: {e}")
    raise
finally:
    script_end_time = datetime.now()
    logging.info(f"Script ended at {script_end_time}. Execution duration: {script_end_time - script_start_time}")
