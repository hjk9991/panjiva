from pathlib import Path

import pandas as pd
import pytest

from scripts.tire_ab_entry.transforms import (
    apply_manual_reviews,
    build_annual_entries,
    build_dynamic_link_moments,
    build_game_artifacts,
    build_quarterly_panels,
    load_verified_game_chunks,
)
import scripts.tire_ab_entry.extract as extract_module
import scripts.tire_ab_entry.transforms as transforms_module
from tests.tire_ab_entry.test_extract import output_frame


def test_core_entry_requires_two_observed_inactive_years_and_persistence():
    frame = pd.DataFrame(
        {
            "manufacturer_parent_id": [1, 1, 1, 1],
            "link_id": ["KOR"] * 4,
            "year": [2022, 2023, 2024, 2025],
            "value_usd": [0.0, 0.0, 2_000_000.0, 1_000_000.0],
            "shipment_count": [0.0, 0.0, 4.0, 2.0],
            "observed": [1, 1, 1, 1],
        }
    )
    result = build_annual_entries(frame)
    row = result.loc[result["year"].eq(2024)].iloc[0]
    assert row["entry_raw"] == 1
    assert row["entry_value"] == 1
    assert row["entry_core"] == 1
    assert row["entry_core_censored"] == 0
    assert row["entry_core_count_basis"] == "allocated_shipment_equivalent"
    assert row["entry_core_measurement_status"] == "pilot_proxy_exact_distinct_unavailable"


def test_2025_core_entry_is_right_censored_and_missing_year_is_not_zero():
    frame = pd.DataFrame(
        {
            "manufacturer_parent_id": [1, 1, 1],
            "link_id": ["MEX"] * 3,
            "year": [2023, 2024, 2025],
            "value_usd": [0.0, 0.0, 2_000_000.0],
            "shipment_count": [0.0, 0.0, 4.0],
            "observed": [1, 1, 1],
        }
    )
    result = build_annual_entries(frame)
    row = result.loc[result["year"].eq(2025)].iloc[0]
    assert row["entry_core_censored"] == 1
    assert pd.isna(row["entry_core"])

    missing = frame.loc[frame["year"].ne(2024)]
    result = build_annual_entries(missing, study_years=[2023, 2024, 2025])
    missing_row = result.loc[result["year"].eq(2024)].iloc[0]
    assert missing_row["observed"] == 0
    assert pd.isna(missing_row["active"])
    assert pd.isna(result.loc[result["year"].eq(2025), "entry_raw"]).all()


def test_annual_entries_accept_canonical_shipment_equivalent_proxy():
    frame = pd.DataFrame(
        {
            "manufacturer_parent_id": [1] * 4,
            "link_id": ["KOR"] * 4,
            "year": [2022, 2023, 2024, 2025],
            "value_usd": [0.0, 0.0, 2_000_000.0, 1_000_000.0],
            "shipment_equivalent_sum": [0.0, 0.0, 3.0, 1.0],
            "observed": [1] * 4,
        }
    )
    row = build_annual_entries(frame).loc[lambda data: data["year"].eq(2024)].iloc[0]
    assert row["entry_core"] == 1


def test_dynamic_link_moments_use_balanced_prefit_adjacent_observed_pairs():
    frame = pd.DataFrame(
        {
            "manufacturer_parent_id": [1] * 7,
            "link_id": ["KOR"] * 7,
            "year": list(range(2016, 2023)),
            "active": [0, 1, 1, 0, 1, 1, 0],
            "observed": [1] * 7,
        }
    )
    result = build_dynamic_link_moments(frame, start_year=2016, end_year=2021)
    assert result["entry_rate"].iat[0] == 1.0
    assert result["exit_rate"].iat[0] == pytest.approx(1 / 3)
    assert result["persistence_rate"].iat[0] == pytest.approx(2 / 3)
    assert result["targeted"].iat[0] == 0
    assert result["window_start"].iat[0] == 2016
    assert result["window_end"].iat[0] == 2021


def _source_rows(game="raw"):
    common = {
        "manufacturer_parent_id": [1, 1, 1],
        "shipper_up": [10, 10, pd.NA],
        "origin_country": ["KOR", "KOR", "JPN"],
        "year_quarter": ["2024Q1"] * 3,
        "input_group": ["rubber", "rubber", "rubber"],
        "finished_market": ["passenger"] * 3,
        "import_route": ["manufacturer_direct", "manufacturer_direct", "unattributed"],
        "value_usd": [60.0, 40.0, 20.0],
        "weight_kg": [6.0, 4.0, 2.0],
        "teu": [0.6, 0.4, 0.2],
        "container_count": [0.6, 0.4, 0.2],
        "shipment_equivalent": [0.6, 0.4, 1.0],
        "shipment_count_nonadditive": [1, 1, 1],
        "manufacturer_conflict_value_usd": [0.0, 0.0, 0.0],
        "importer_pit_same_parent_overlap_value_usd": [0.0, 0.0, 0.0],
        "shipper_pit_same_parent_overlap_value_usd": [0.0, 0.0, 0.0],
        "importer_current_parent_fallback_value_usd": [6.0, 0.0, 0.0],
        "shipper_current_parent_fallback_value_usd": [0.0, 4.0, 0.0],
        "description_candidate_value_usd": [0.0, 0.0, 20.0],
        "description_ambiguous_value_usd": [0.0, 0.0, 0.0],
        "hs_review_value_usd": [0.0, 10.0, 0.0],
        "estimation_eligible": [1, 1, 1],
        "sensitivity_eligible": [1, 1, 0],
    }
    return pd.DataFrame(common)


@pytest.mark.parametrize("game", ["raw", "finished"])
def test_quarterly_panels_preserve_additive_totals_and_explicit_diagnostics(game):
    result = build_quarterly_panels(_source_rows(game), game=game)
    origin = result["origin"]
    supplier = result["supplier"]
    assert origin["value_usd"].sum() == 120.0
    assert origin["shipment_equivalent"].sum() == 2.0
    assert "shipment_count_source_group_sum_nonadditive" in origin
    assert "shipment_count" not in origin
    assert origin["shipment_count_source_group_sum_nonadditive"].sum() == 3
    assert set(origin["shipment_measurement_status"]) == {
        "shipment_equivalent_additive;distinct_panel_count_unavailable"
    }
    assert supplier["value_usd"].sum() == 120.0
    assert set(origin["link_level"]) == {"origin"}
    assert set(supplier["link_level"]) == {"supplier"}
    assert origin["current_parent_fallback_value_share"].between(0, 1).all()
    assert origin["description_review_value_share"].between(0, 1).all()
    assert origin["manufacturer_direct_value_share"].between(0, 1).all()
    assert origin["unattributed_value_share"].between(0, 1).all()


def test_split_hs_rows_sum_to_one_equivalent_but_not_one_distinct_count():
    source = _source_rows().iloc[:2].copy()
    result = build_quarterly_panels(source, game="raw")["origin"]
    assert result["shipment_equivalent"].iat[0] == 1.0
    assert result["shipment_count_source_group_sum_nonadditive"].iat[0] == 2


def test_manual_decisions_define_main_and_confirmed_sensitivity_value():
    source = _source_rows().assign(hs_full_code=["SYN-A", "SYN-B", "SYN-C"])
    reviews = pd.DataFrame(
        {
            "game": ["raw"] * 3,
            "review_group": ["rubber"] * 3,
            "review_item_id": ["SYN-A", "SYN-B", "SYN-C"],
            "review_status": ["confirmed", "probable", "unclear"],
            "source_note": ["human A", "human B", "human C"],
        }
    )
    reviewed = apply_manual_reviews(source, game="raw", reviews=reviews)
    assert list(reviewed["manual_main_eligible"]) == [1, 1, 0]
    assert list(reviewed["manual_confirmed_eligible"]) == [1, 0, 0]
    panel = build_quarterly_panels(reviewed, game="raw")["origin"]
    assert panel["manual_main_eligible_value_usd"].sum() == 100.0
    assert panel["manual_confirmed_value_usd"].sum() == 60.0
    assert panel["manual_main_eligible_shipment_equivalent"].sum() == 1.0


def test_quarterly_panel_contract_rejects_unexpected_columns_and_duplicate_rows():
    source = _source_rows()
    with pytest.raises(ValueError, match="unexpected"):
        build_quarterly_panels(source.assign(secret_extra=1), game="raw")
    with pytest.raises(ValueError, match="duplicate"):
        build_quarterly_panels(pd.concat([source, source.iloc[[0]]]), game="raw")


def test_verified_chunk_loader_rejects_missing_extra_stale_or_corrupt_manifest(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(extract_module, "validate_output_path", lambda path: Path(path).resolve())
    monkeypatch.setattr(transforms_module, "validate_output_path", lambda path: Path(path).resolve())
    extract_module.extract_chunk(
        object(), "raw", "2024Q1", {"MICHELIN": 1, "GOODYEAR": 2, "HANKOOK": 3},
        parent_seed_sha256="seed", output_root=tmp_path,
        query_fn=lambda connection, sql: output_frame(),
    )
    loaded = load_verified_game_chunks(
        "raw", parent_seed_sha256="seed", output_root=tmp_path,
        expected_quarters=["2024Q1"],
    )
    assert len(loaded) == 1

    manifest_path = tmp_path / "_manifests" / "raw.json"
    manifest = pd.read_json(manifest_path, typ="series").to_dict()
    manifest["chunks"]["raw/2024Q2"] = dict(manifest["chunks"]["raw/2024Q1"])
    manifest_path.write_text(__import__("json").dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="exact expected chunks"):
        load_verified_game_chunks(
            "raw", parent_seed_sha256="seed", output_root=tmp_path,
            expected_quarters=["2024Q1"],
        )


def test_build_game_artifacts_annualizes_primary_origin_and_keeps_game_separate():
    source = pd.concat(
        [_source_rows().assign(year_quarter="2023Q1"), _source_rows()],
        ignore_index=True,
    )
    artifacts = build_game_artifacts(source, game="raw", study_years=[2022, 2023, 2024, 2025])
    assert set(artifacts) == {"origin_quarterly", "supplier_quarterly", "annual", "dynamic_moments"}
    annual = artifacts["annual"]
    assert set(annual["game"]) == {"raw"}
    assert annual["year"].min() == 2022 and annual["year"].max() == 2025
    assert annual["entry_core_count_basis"].eq("allocated_shipment_equivalent").all()
    assert artifacts["dynamic_moments"]["targeted"].eq(0).all()
