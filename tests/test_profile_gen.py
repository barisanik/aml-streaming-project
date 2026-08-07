# tests/test_profile_gen.py

import sys
import os
import random
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts', 'simulator'))

import numpy as np

from profile_gen import (
    sample_income_segment,
    sample_last_activity_at,
    build_profile,
)
from shared.schemas import EntitySegment, IncomeSegment, Profile


TEST_CONFIG = {
    "income_segment_weights": {"low": 0.45, "medium": 0.40, "high": 0.15},
    "income_segments": {
        "low":    {"avg_amount_mu": 4.5, "avg_amount_sigma": 0.6, "weekly_txn_frequency_range": [3, 8]},
        "medium": {"avg_amount_mu": 5.5, "avg_amount_sigma": 0.6, "weekly_txn_frequency_range": [5, 15]},
        "high":   {"avg_amount_mu": 6.5, "avg_amount_sigma": 0.7, "weekly_txn_frequency_range": [8, 25]},
    },
    "locations": [
        {"city": "Istanbul", "country": "TR"},
        {"city": "London", "country": "GB"},
    ],
    "merchant_categories": ["grocery", "dining", "retail", "travel"],
    "preferred_categories_count_range": [2, 4],
    "active_hours_start_range": [6, 10],
    "active_hours_span_range": [8, 14],
    "dormant_ratio": 0.05,
}

FIXED_NOW = datetime(2026, 1, 1)


# ── Function: sample_income_segment ─────────────────────────────────────────

def test_sample_income_segment_returns_valid_enum():
    """Checks if created segment is aligned with IncomeSegment schema."""
    rng = random.Random(0)
    result = sample_income_segment(TEST_CONFIG, rng)
    assert isinstance(result, IncomeSegment)

def test_sample_income_segment_respects_zero_weight():
    """If low and medium have zero weight, every draw must land on high."""
    config = dict(TEST_CONFIG)
    config["income_segment_weights"] = {"low": 0, "medium": 0, "high": 1}
    rng = random.Random(0)

    results = [sample_income_segment(config, rng) for _ in range(20)]

    assert all(r == IncomeSegment("high") for r in results)


# ── Function: sample_last_activity_at ───────────────────────────────────────

def test_sample_last_activity_at_dormant_within_30_to_90_days():
    """Checks if dormant account last activity is within specified range."""
    rng = random.Random(0)
    for _ in range(50):
        result = sample_last_activity_at(dormant=True, now=FIXED_NOW, rng=rng)
        days_back = (FIXED_NOW - result).days
        assert 30 <= days_back <= 90

def test_sample_last_activity_at_active_within_0_to_3_days():
    """Checks if normal account last activity is within specified range."""
    rng = random.Random(0)
    for _ in range(50):
        result = sample_last_activity_at(dormant=False, now=FIXED_NOW, rng=rng)
        days_back = (FIXED_NOW - result).days
        assert 0 <= days_back <= 3

def test_sample_last_activity_at_dormant_never_overlaps_active_range():
    """Dormant and active windows must not overlap, otherwise the 30-day dormancy cutoff used by the rule engine becomes ambiguous."""
    rng = random.Random(0)
    dormant_result = sample_last_activity_at(dormant=True, now=FIXED_NOW, rng=rng)
    active_result = sample_last_activity_at(dormant=False, now=FIXED_NOW, rng=rng)

    assert dormant_result < active_result


# ── Function: build_profile ─────────────────────────────────────────────────

def test_build_profile_returns_profile_instance():
    """Checks if created profile is aligned with Profile schema."""
    rng = random.Random(0)
    np_rng = np.random.default_rng(0)

    profile = build_profile(TEST_CONFIG, rng, np_rng, FIXED_NOW)

    assert isinstance(profile, Profile)

def test_build_profile_segment_is_always_individual():
    """Checks if segment of the created profile is individual always."""
    rng = random.Random(0)
    np_rng = np.random.default_rng(0)

    profile = build_profile(TEST_CONFIG, rng, np_rng, FIXED_NOW)

    assert profile.segment == EntitySegment.INDIVIDUAL

def test_build_profile_preferred_categories_count_within_config_range():

    rng = random.Random(0)
    np_rng = np.random.default_rng(0)
    low, high = TEST_CONFIG["preferred_categories_count_range"]

    profile = build_profile(TEST_CONFIG, rng, np_rng, FIXED_NOW)

    assert low <= len(profile.preferred_merchant_categories) <= high

def test_build_profile_preferred_categories_has_no_duplicates():
    rng = random.Random(0)
    np_rng = np.random.default_rng(0)

    profile = build_profile(TEST_CONFIG, rng, np_rng, FIXED_NOW)

    assert len(profile.preferred_merchant_categories) == len(set(profile.preferred_merchant_categories))

def test_build_profile_active_hour_end_capped_at_23():
    """Start hour plus span can exceed 23; the function must clip active_hour_end so it never runs past the last hour of the day."""
    config = dict(TEST_CONFIG)
    config["active_hours_start_range"] = [23, 23]
    config["active_hours_span_range"] = [14, 14]
    rng = random.Random(0)
    np_rng = np.random.default_rng(0)

    profile = build_profile(config, rng, np_rng, FIXED_NOW)

    assert profile.active_hour_end == 23

def test_build_profile_home_location_matches_config_pool():
    rng = random.Random(0)
    np_rng = np.random.default_rng(0)
    valid_cities = {loc["city"] for loc in TEST_CONFIG["locations"]}

    profile = build_profile(TEST_CONFIG, rng, np_rng, FIXED_NOW)

    assert profile.home_city in valid_cities

def test_build_profile_weekly_txn_frequency_within_segment_range():
    rng = random.Random(0)
    np_rng = np.random.default_rng(0)

    profile = build_profile(TEST_CONFIG, rng, np_rng, FIXED_NOW)

    freq_low, freq_high = TEST_CONFIG["income_segments"][profile.income_segment.value]["weekly_txn_frequency_range"]
    assert freq_low <= profile.weekly_txn_frequency <= freq_high

def test_build_profile_is_reproducible_with_same_seed():
    """Same seed must produce identical profiles, per the project's rule that every randomness source is bound to a seed."""
    rng_a = random.Random(42)
    np_rng_a = np.random.default_rng(42)
    profile_a = build_profile(TEST_CONFIG, rng_a, np_rng_a, FIXED_NOW)

    rng_b = random.Random(42)
    np_rng_b = np.random.default_rng(42)
    profile_b = build_profile(TEST_CONFIG, rng_b, np_rng_b, FIXED_NOW)

    assert profile_a.model_dump() == profile_b.model_dump()

def test_build_profile_different_seeds_produce_different_output():
    rng_a = random.Random(1)
    np_rng_a = np.random.default_rng(1)
    profile_a = build_profile(TEST_CONFIG, rng_a, np_rng_a, FIXED_NOW)

    rng_b = random.Random(2)
    np_rng_b = np.random.default_rng(2)
    profile_b = build_profile(TEST_CONFIG, rng_b, np_rng_b, FIXED_NOW)

    assert profile_a.model_dump() != profile_b.model_dump()