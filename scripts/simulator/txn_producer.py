""" /scripts/simulator/txn_producer.py
# ============================================================
#   Script: Transaction Producer
# ============================================================
#   Purpose: 
#       Generate randomized transactions with realistic distribution for the AML/Fraud simulator.
#
#   Logic:
#    - Creates randomized transactions with random users with following rules:
#       - Maximum 50.000 transactions per day can be created.
#       - Transaction channel probability differs according to time band of day (morning, afternoon, evening, night).
#       - Transaction distribution over time is defined by sinusoidal hourly intensity multiplier. (Peak hour = 14, trough hour = 3)
#
#   Usage:
#    - Change directory to project root
#    - Run following command: "python scripts/simulator/txn_producer.py"
"""

import json
import logging
import random
import sys
import uuid
from datetime import datetime, timedelta
import time
from pathlib import Path
import numpy as np
import yaml
from faker import Faker

### Initial parameters ###

## PATHS
# Project Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)      # create logs folder if it does not exist yet

# Config Paths
CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "configs" / "producer_config.yml"
PROFILES_PATH = Path(__file__).resolve().parent.parent / "simulator" / "profiles.json"
SHARED_DIR = PROJECT_ROOT / "shared"
sys.path.insert(0, str(SHARED_DIR))
from schemas import Channel

# Logging parameters
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),                                                    # enables visibility on console
        logging.FileHandler(LOG_DIR / "txn_producer.log", encoding="utf-8"),        # enables recording on a file
    ],
)

# Reads profile_config.yml.
def load_config(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_profiles(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        json_file = json.load(f)
        return json_file

## CONFIGS
config = load_config(CONFIG_PATH)
profiles = load_profiles(PROFILES_PATH)

## SEED
seed = config["reproducibility"]["seed"]
random.seed(seed)  # Seed set for reproducability.
np.random.seed(seed)
Faker.seed(seed)

## Time Parameters
time_compression_factor = config["time_model"]["time_compression_factor"]
simulation_start_time = datetime.fromisoformat(config["time_model"]["simulation_start"])

txn_distribution_type = config["hourly_intensity_multiplier"]["type"]
peak_transaction_hour = config["hourly_intensity_multiplier"]["peak_hour"]
trough_transaction_hour = config["hourly_intensity_multiplier"]["trough_hour"]
amplitude = config["hourly_intensity_multiplier"]["amplitude"]
baseline = config["hourly_intensity_multiplier"]["baseline"]

## Sampling Parameters
daily_transaction_limit = config["volume"]["target_transactions_per_simulated_day"]
base_rate_per_second = config["arrival_process"]["base_rate_per_second"]    # average wait time between transactions for exponential distribution

## FUNCTIONS

def get_time_band(time):
    time_band = ""
    if time.hour >= 7 and time.hour < 13:
        time_band = "morning"
    elif time.hour >= 13 and time.hour < 19:
        time_band = "afternoon"
    elif (time.hour >= 19 and time.hour <= 23) or (time.hour >= 0 and time.hour < 1):
        time_band = "evening"
    else:
        time_band = "night"
    return time_band

def create_wait_time(rate, time):

    current_time = time.hour / 24 * 2 * np.pi
    peak_hour_time = peak_transaction_hour / 24 * 2 * np.pi

    min_multiplier = baseline - amplitude # 1.0 - 0.6 = 0.4
    max_multiplier = baseline + amplitude # 1.0 + 0.6 = 1.6
    current_multiplier = baseline + amplitude * np.cos(current_time - peak_hour_time)
    effective_rate = rate * current_multiplier

    scale = 1/effective_rate
    return np.random.exponential(scale)

def create_transaction_event(current_sim_time, customer_profile):
    transaction_id = uuid.uuid4()
    account_id = customer_profile['account_id']
    event_time = current_sim_time
    produced_at = datetime.now()
    merchant_category = random.choice(customer_profile["preferred_merchant_categories"])
    
    # Channel: Sets a random channel according to time band (morning, afternoon, evening and night). Every time band has different probability distribution for each channel.
    channel_order = [c.value for c in Channel]
    time_band = get_time_band(current_sim_time)
    time_band_probabilities = [config["channel"][time_band][c] for c in channel_order]
    channel = random.choices(channel_order, weights=time_band_probabilities)[0]
    
    amount = np.random.lognormal(mean=customer_profile["avg_amount_mu"], sigma=customer_profile["avg_amount_sigma"])

    logging.info(
        f"Transaction created | account={account_id} | channel={channel} | "
        f"amount={amount:.2f} | merchant={merchant_category} | "
        f"event_time={event_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    return {
        "transaction_id":transaction_id,
        "account_id":account_id,
        "merchant_category":merchant_category,
        "amount":amount,
        "channel":channel,
        "event_time":event_time,
        "produced_at":produced_at
    }

def main(current_sim_time) -> None:
    logging.info(f"Simulation started at {current_sim_time}")

    for day in range(0,1):
        logging.info(f"Day: {day}")
        for i in range(0, daily_transaction_limit):

            create_transaction_event(current_sim_time, random.choice(profiles))

            wait_time = create_wait_time(rate=base_rate_per_second, time=current_sim_time)
            current_sim_time = current_sim_time + timedelta(seconds=wait_time)
            time.sleep(wait_time / time_compression_factor)

    logging.info(f"Simulation ended at {current_sim_time}")

if __name__ == "__main__":
    
    script_start_time = datetime.now()
    logging.info(f"Script started at {script_start_time}.")

    main(current_sim_time=simulation_start_time)
    
    script_end_time = datetime.now()
    logging.info(f"Script ended at {script_end_time}. Execution duration: {script_end_time - script_start_time}")