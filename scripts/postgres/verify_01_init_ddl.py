"""    
# ============================================================================ #
    Script: Verify Schema Initalization
# ============================================================================ #
    Purpose: Verify that 01_init_schema.sql created the expected tables and roles.
    Method: Connects as the Postgres superuser (gets credentials from .env). Run SQL command in 01_init_shema.sql with superuser access.
    Run command: python scripts/postgres/verify_01_init_ddl.py (Run from the repo root with the venv active)
# ============================================================================ #
"""

import os
from pathlib import Path
import psycopg2
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
        logging.StreamHandler(),                                                    # enables visibility on console
        logging.FileHandler(LOG_DIR / "verify_01_init.log", encoding="utf-8"),      # enables recording on a file
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
        dbname="aml_platform",
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema IN ('raw','ops','public')
        AND table_name IN ('transactions','alerts','answer_key','consumer_heartbeat')
        ORDER BY 1,2
    """)
    for row in cur.fetchall():
        logging.info(f"Table Schema: {row[0]}       |       Table Name: {row[1]}")

    cur.execute("SELECT rolname FROM pg_roles WHERE rolname LIKE 'app_%'")
    logging.info(f"Roles: {[r[0] for r in cur.fetchall()]}")
    conn.close()
except Exception as e:
    logging.critical(f"Script failed: {e}")
    raise
finally:
    script_end_time = datetime.now()
    logging.info(f"Script ended at {script_end_time}. Execution duration: {script_end_time - script_start_time}")


