"""Delta tests for the footwear package against its tire-derived baseline.

The shared extraction/transform/QA machinery is covered by the tire suite in
this repository; these tests pin the footwear-specific contracts: single
finished game, four strategic manufacturers, HS6-decidable sports families,
and athletic market domains.
"""

import re

import pytest

from scripts.footwear_ab_entry.config import (
    G10_DISCLOSED_SHARES,
    GAMES,
    HS_FAMILIES,
    MANUFACTURER_DESCRIPTION_ALIASES,
    MANUFACTURER_KEYS,
    OUTPUT_ROOT,
    PROBE_FAMILIES,
    SPORTS_FAMILIES,
    STRUCTURAL_WINDOW_YEARS,
    iter_quarters,
)
from scripts.footwear_ab_entry.extract import REQUIRED_OUTPUT_COLUMNS, TEXT_OUTPUT_COLUMNS
from scripts.footwear_ab_entry import extract as extract_module
from scripts.footwear_ab_entry.qa import FINISHED_MARKETS
from scripts.footwear_ab_entry.sql import (
    build_finished_sql,
    reference_hs6_allocation,
)

PARENT_IDS = {
    "NIKE": 101,
    "DECKERS": 202,
    "UNDER_ARMOUR": 303,
    "SKECHERS": 404,
}


def normalized(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip().lower()


def test_footwear_scope_contract():
    assert str(OUTPUT_ROOT).endswith("ab_entry_footwear_v1")
    assert GAMES == ("finished",)
    assert MANUFACTURER_KEYS == ("NIKE", "DECKERS", "UNDER_ARMOUR", "SKECHERS")
    assert MANUFACTURER_DESCRIPTION_ALIASES["NIKE"] == ("nike", "jordan", "converse")
    assert SPORTS_FAMILIES == ("640219", "640319", "640411")
    assert set(SPORTS_FAMILIES) < set(PROBE_FAMILIES)
    assert all(spec.status != "included" or family in SPORTS_FAMILIES
               for family, spec in HS_FAMILIES.items())
    quarters = tuple(iter_quarters())
    assert quarters[0] == "2014Q1" and quarters[-1] == "2025Q4"


def test_structural_window_and_g10_anchor_contract():
    # 2026-08-17 approval: pre-tariff window; Nike FY2017 10-K factory shares.
    assert STRUCTURAL_WINDOW_YEARS == (2016, 2017, 2018)
    assert dict(G10_DISCLOSED_SHARES["NIKE"]) == {
        "Vietnam": 0.46,
        "GREATER_CHINA": 0.27,
        "Indonesia": 0.21,
    }


def test_finished_sql_uses_sports_families_and_athletic_markets():
    sql = normalized(
        build_finished_sql(PARENT_IDS, "2024-01-01", "2024-04-01")
    )
    assert "athletic_sports" in sql
    assert "athletic_escalated_general" in sql
    assert "general_footwear_unreviewed" in sql
    for family in SPORTS_FAMILIES:
        assert repr(family).lower() in sql or f"'{family}'" in sql
    assert "left(hs_full_code, 6) in" in sql
    assert "401110" not in sql
    assert "insert " not in sql
    assert "create " not in sql
    assert "manufacturer_direct" in sql
    assert "distributor_intermediated" in sql


def test_four_firm_parent_mapping_is_enforced():
    with pytest.raises(ValueError, match="keyed parent mapping"):
        build_finished_sql(
            {"NIKE": 101, "DECKERS": 202, "UNDER_ARMOUR": 303},
            "2024-01-01",
            "2024-04-01",
        )
    with pytest.raises(ValueError, match="unique positive"):
        build_finished_sql(
            {**PARENT_IDS, "SKECHERS": 101}, "2024-01-01", "2024-04-01"
        )


def test_single_game_guards():
    with pytest.raises(ValueError, match="one of"):
        extract_module.validate_game("raw")
    assert extract_module.validate_game("finished") == "finished"
    with pytest.raises(ValueError, match="single finished game"):
        extract_module._build_sql("raw", PARENT_IDS, "2024-01-01", "2024-04-01", None)


def test_hs6_allocation_reviews_by_eligible_family_prefix():
    output = reference_hs6_allocation(
        ["6404110090", "6404199020", "640219", "6402120000"], value_usd=120.0
    )
    assert output["640411"]["hs_eligible"] == 1
    # 640419 joined the estimation market via the 2026-08-17 escalation.
    assert output["640419"]["hs_eligible"] == 1
    assert output["640219"]["hs_eligible"] == 1
    assert output["640212"]["hs_eligible"] == 0
    assert output["640411"]["allocated_value_usd"] == pytest.approx(30.0)


def test_athletic_market_domains_and_output_contract():
    assert FINISHED_MARKETS == {
        "athletic_sports",
        "athletic_escalated_general",
        "general_footwear_unreviewed",
    }
    assert "finished_market" in REQUIRED_OUTPUT_COLUMNS
    assert "finished_market" in TEXT_OUTPUT_COLUMNS


def test_no_untyped_null_feeds_required_numeric_columns():
    required_numeric = set(REQUIRED_OUTPUT_COLUMNS).difference(TEXT_OUTPUT_COLUMNS)
    sql = normalized(build_finished_sql(PARENT_IDS, "2024-01-01", "2024-04-01"))
    for column in re.findall(r"\bnull as ([a-z_][a-z0-9_]*)", sql):
        assert column not in required_numeric, column
