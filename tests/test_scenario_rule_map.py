# tests/test_scenario_rule_map.py
"""
Keeps the dbt seed scenario_rule_map.csv in sync with configs/scenario_config.yml,
the ScenarioType enum and docs/scenario_rule_matrix.md.
"""

import csv
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEED_PATH = PROJECT_ROOT / "dbt" / "aml_platform" / "seeds" / "scenario_rule_map.csv"
SCENARIO_CONFIG_PATH = PROJECT_ROOT / "configs" / "scenario_config.yml"
MATRIX_PATH = PROJECT_ROOT / "docs" / "scenario_rule_matrix.md"

sys.path.insert(0, str(PROJECT_ROOT / "shared"))

from schemas import ScenarioType


def load_seed() -> dict:
    with open(SEED_PATH, "r", encoding="utf-8", newline="") as f:
        return {row["scenario_type"]: row for row in csv.DictReader(f)}

def load_scenario_types() -> dict:
    with open(SCENARIO_CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["scenario_types"]

def load_matrix_status() -> dict:
    """Returns {scenario_type: status} from the markdown table rows."""
    status = {}
    for line in MATRIX_PATH.read_text(encoding="utf-8").splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == 3 and cells[0] in {s.value for s in ScenarioType}:
            status[cells[0]] = cells[2]
    return status


def test_seed_covers_every_scenario_type():
    assert set(load_seed()) == {s.value for s in ScenarioType}

def test_implemented_seed_rows_match_scenario_config():
    seed = load_seed()
    scenario_types = load_scenario_types()
    implemented = {name for name, row in seed.items() if row["is_implemented"] == "true"}
    assert implemented == set(scenario_types)
    for name in implemented:
        assert seed[name]["expected_rule_id"] == scenario_types[name]["rule_id"]
        assert seed[name]["expected_rule_name"] == scenario_types[name]["name"]

def test_not_implemented_seed_rows_have_no_rule_id():
    for row in load_seed().values():
        if row["is_implemented"] == "false":
            assert row["expected_rule_id"] == ""

def test_docs_matrix_status_matches_seed():
    matrix = load_matrix_status()
    seed = load_seed()
    assert set(matrix) == set(seed)
    for name, row in seed.items():
        expected = "Implemented" if row["is_implemented"] == "true" else "Not implemented"
        assert matrix[name] == expected
