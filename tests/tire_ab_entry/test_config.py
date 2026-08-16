from pathlib import Path

import pytest

import scripts.tire_ab_entry.config as config_module
from scripts.tire_ab_entry.config import (
    APPROVED_ACCOUNT,
    END_QUARTER,
    ENTRY_CORE_SHIPMENTS,
    ENTRY_CORE_VALUE_USD,
    ENTRY_VALUE_USD,
    FINISHED_PROBE_PREFIXES,
    MANUFACTURER_PARENT_TARGETS,
    OUTPUT_ROOT,
    PRE_ENTRY_YEARS,
    RAW_GROUPS,
    REVIEWED_ESTIMATION_CODES,
    START_QUARTER,
    iter_quarters,
    validate_output_path,
)


def test_tire_panjiva_scope():
    assert OUTPUT_ROOT == Path(r"C:\panjiva\data\staging\ab_entry_tire_v1")
    quarters = tuple(iter_quarters())
    assert quarters[0] == "2014Q1"
    assert quarters[-1] == "2025Q4"
    assert len(quarters) == 48
    target = OUTPUT_ROOT / "_chunks" / "raw" / "2014Q1.parquet"
    assert validate_output_path(target) == target.resolve(strict=False)


def test_output_boundary_rejects_sibling_prefix_and_alternate_root(tmp_path):
    with pytest.raises(ValueError, match="outside licensed output root"):
        validate_output_path(Path(str(OUTPUT_ROOT) + "_copy") / "result.parquet")
    with pytest.raises(ValueError, match="outside licensed output root"):
        validate_output_path(tmp_path / "result.parquet")


def test_quarter_and_entry_threshold_constants_are_exact():
    assert APPROVED_ACCOUNT == "VLC67107"
    assert START_QUARTER == "2014Q1"
    assert END_QUARTER == "2025Q4"
    assert ENTRY_VALUE_USD == 100_000
    assert ENTRY_CORE_VALUE_USD == 1_000_000
    assert ENTRY_CORE_SHIPMENTS == 3
    assert PRE_ENTRY_YEARS == 2


def test_raw_groups_distinguish_reviewed_steel_families():
    assert tuple(RAW_GROUPS) == (
        "4001",
        "4002",
        "4005",
        "280300",
        "5902",
        "7312",
        "7217",
        "7228",
    )
    assert tuple(group.hs_prefix for group in RAW_GROUPS.values()) == (
        "4001",
        "4002",
        "4005",
        "280300",
        "5902",
        "7312",
        "7217",
        "7228",
    )
    assert tuple(group.status for group in RAW_GROUPS.values()) == (
        "included",
        "included",
        "included",
        "included",
        "included",
        "requires_review",
        "requires_review",
        "requires_review",
    )
    with pytest.raises(TypeError):
        RAW_GROUPS["other"] = RAW_GROUPS["4001"]


def test_finished_scope_keeps_probe_prefixes_separate_from_reviewed_codes():
    assert FINISHED_PROBE_PREFIXES == ("401110", "401120")
    assert REVIEWED_ESTIMATION_CODES
    assert all(code.startswith(("401110", "401120")) for code in REVIEWED_ESTIMATION_CODES)
    assert any(code.startswith("401120") and len(code) > 6 for code in REVIEWED_ESTIMATION_CODES)
    assert REVIEWED_ESTIMATION_CODES != FINISHED_PROBE_PREFIXES
    # 2026-08-16 reviewed decision: HS6 4011.10 is passenger-car tires by
    # heading definition, so every 401110-family depth is estimation-eligible;
    # bare 401120 stays out because HS6 cannot separate light from heavy truck.
    assert "401110" in REVIEWED_ESTIMATION_CODES
    assert "401120" not in REVIEWED_ESTIMATION_CODES
    assert all(
        len(code) == 10
        for code in REVIEWED_ESTIMATION_CODES
        if code.startswith("401120")
    )


def test_parent_targets_are_exact_and_immutable():
    assert MANUFACTURER_PARENT_TARGETS == (
        "Michelin",
        "Goodyear",
        "Hankook Tire & Technology",
    )


def test_quarter_iterator_uses_configured_boundaries(monkeypatch):
    monkeypatch.setattr(config_module, "START_QUARTER", "2020Q4")
    monkeypatch.setattr(config_module, "END_QUARTER", "2021Q2")
    assert tuple(config_module.iter_quarters()) == ("2020Q4", "2021Q1", "2021Q2")


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        ("2020Q0", "2021Q1", "quarter syntax"),
        ("20Q1", "2021Q1", "quarter syntax"),
        ("2021Q2", "2021Q1", "before or equal"),
    ],
)
def test_quarter_iterator_rejects_invalid_or_reversed_ranges(
    monkeypatch, start, end, message
):
    monkeypatch.setattr(config_module, "START_QUARTER", start)
    monkeypatch.setattr(config_module, "END_QUARTER", end)
    with pytest.raises(ValueError, match=message):
        tuple(config_module.iter_quarters())


def test_output_validation_rejects_non_windows_runtime(monkeypatch):
    monkeypatch.setattr(config_module, "_runtime_platform", lambda: "Linux", raising=False)
    with pytest.raises(RuntimeError, match="Windows"):
        validate_output_path(OUTPUT_ROOT / "result.parquet")
