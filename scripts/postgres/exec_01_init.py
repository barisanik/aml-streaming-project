
"""    
# ============================================================================ #
    Script: Execute 01 Init SQL
# ============================================================================ #
    Purpose: Execute 001_init_schema.sql against the running Postgres container.
    Method: Connects as the Postgres superuser (gets credentials from .env). Run SQL command in 01_init_shema.sql with superuser access.
"""

import os
from pathlib import Path
import psycopg2
from dotenv import load_dotenv
import logging
from datetime import datetime

### Initial parameters ###
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)      # create logs folder if it does not exist yet

# Logging parameters
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),                                                # enables visibility on console
        logging.FileHandler(LOG_DIR / "exec_01_init.log", encoding="utf-8"),    # enables recording on a file
    ],
)

load_dotenv()

script_start_time = datetime.now()
logging.info(f"Script started at {script_start_time}.")

try:
    SQL_PATH = Path(__file__).resolve().parent.parent.parent / "postgre_sql_files" / "01_init_schema.sql"

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname="aml_platform",
    )
    conn.autocommit = True

    with open(SQL_PATH, encoding="utf-8") as f:
        sql = f.read()

    cur = conn.cursor()
    cur.execute(sql)
    conn.close()
    logging.info(f"OK - DDL executed.")
except Exception as e:
    logging.critical(f"Script failed: {e}")
    raise
finally:
    script_end_time = datetime.now()
    logging.info(f"Script ended at {script_end_time}. Execution duration: {script_end_time - script_start_time}")