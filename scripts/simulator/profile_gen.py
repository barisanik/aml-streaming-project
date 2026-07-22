""" /scripts/simulator/profile_gen.py
# ============================================================
#   Script: Profile Generator
# ============================================================
#   Purpose: 
#       Generate synthetic customer profiles for the AML/Fraud simulator.
#
#   Logic:
#    - Reads all tunable parameters from configs/profile_config.yml
#    - Writes the result to simulator/profiles.json as one JSON object per account.
#
#   Usage:
#    - Change directory to project root
#    - Run following command: "python scripts/simulator/profile_gen.py"
"""

import json
import logging
import random
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import yaml
from faker import Faker

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from shared.schemas import EntitySegment, IncomeSegment, Profile

### Initial parameters ###

## PATHS
# Project Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)      # create logs folder if it does not exist yet

# Config Paths
CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "configs" / "profile_config.yml"
OUTPUT_PATH = Path(__file__).resolve().parent / "profiles.json"

# Logging parameters
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),                                                    # enables visibility on console
        logging.FileHandler(LOG_DIR / "txn_producer.log", encoding="utf-8"),        # enables recording on a file
    ],
)

## FUNCTIONS
# Reads profile_config.yml.
def load_config(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def sample_income_segment(config: dict, rng: random.Random) -> IncomeSegment:
    segments = list(config["income_segment_weights"].keys())
    weights = list(config["income_segment_weights"].values())
    chosen = rng.choices(segments, weights=weights, k=1)[0]
    return IncomeSegment(chosen)


def sample_last_activity_at(dormant: bool, now: datetime, rng: random.Random) -> datetime:
    if dormant:
        days_back = rng.uniform(30, 90)
    else:
        days_back = rng.uniform(0, 3)
    return now - timedelta(days=days_back)


def build_profile(config: dict, rng: random.Random, np_rng: np.random.Generator, now: datetime) -> Profile:
    location = rng.choice(config["locations"])
    income_segment = sample_income_segment(config, rng)
    segment_params = config["income_segments"][income_segment.value]

    # Per-account jitter around the segment's base lognormal mu, so accounts in the same income segment aren't all statistically identical.
    avg_amount_mu = segment_params["avg_amount_mu"] + np_rng.normal(0, 0.15)
    avg_amount_sigma = segment_params["avg_amount_sigma"]

    freq_low, freq_high = segment_params["weekly_txn_frequency_range"]
    weekly_txn_frequency = rng.uniform(freq_low, freq_high)

    start_low, start_high = config["active_hours_start_range"]
    span_low, span_high = config["active_hours_span_range"]
    active_hour_start = rng.randint(start_low, start_high)
    active_hour_end = min(23, active_hour_start + rng.randint(span_low, span_high))

    cat_low, cat_high = config["preferred_categories_count_range"]
    num_categories = rng.randint(cat_low, cat_high)
    preferred_categories = rng.sample(config["merchant_categories"], num_categories)

    is_dormant = rng.random() < config["dormant_ratio"]
    last_activity_at = sample_last_activity_at(is_dormant, now, rng)

    return Profile(
        account_id=str(uuid.UUID(int=rng.getrandbits(128))),  # seeded, not uuid.uuid4()
        home_city=location["city"],
        home_country=location["country"],
        income_segment=income_segment,
        avg_amount_mu=avg_amount_mu,
        avg_amount_sigma=avg_amount_sigma,
        active_hour_start=active_hour_start,
        active_hour_end=active_hour_end,
        weekly_txn_frequency=weekly_txn_frequency,
        preferred_merchant_categories=preferred_categories,
        last_activity_at=last_activity_at,
        segment=EntitySegment.INDIVIDUAL,
    )


def main() -> None:
    config = load_config(CONFIG_PATH)

    seed = config["seed"]
    rng = random.Random(seed)
    np_rng = np.random.default_rng(seed)
    Faker.seed(seed)  # seed Faker for reproducibility

    now = datetime.fromisoformat(config["reference_time"].replace("Z", "+00:00"))
    num_profiles = config["num_profiles"]   # Defines profile count to create.

    logging.info("Generating %d profiles (seed=%d)", num_profiles, seed)

    # Creates profiles using config.
    profiles = [build_profile(config, rng, np_rng, now) for _ in range(num_profiles)]

    # Gets count of non-active (dormant) profiles for the last 30 days.
    dormant_count = sum(1 for p in profiles if p.last_activity_at < now - timedelta(days=30))
    logging.info("Generated %d profiles, %d flagged dormant (%.1f%%)",
                len(profiles), dormant_count, 100 * dormant_count / len(profiles))

    # Exports profiles to profiles.json
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump([p.model_dump(mode="json") for p in profiles], f, indent=2, ensure_ascii=False)

    logging.info("Wrote profiles to %s", OUTPUT_PATH)


if __name__ == "__main__":
    
    script_start_time = datetime.now()
    logging.info(f"Script started at {script_start_time}.")

    main()
    
    script_end_time = datetime.now()
    logging.info(f"Script ended at {script_end_time}. Execution duration: {script_end_time - script_start_time}")
