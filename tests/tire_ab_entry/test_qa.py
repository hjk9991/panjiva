from pathlib import Path

import pandas as pd
import pytest

import scripts.tire_ab_entry.extract as extract_module
import scripts.tire_ab_entry.qa as qa_module
from tests.tire_ab_entry.test_extract import output_frame
from scripts.tire_ab_entry.qa import (
    build_manual_review_queue,
    capture_g0_validation,
    evaluate_gates,
)


def test_manual_review_queue_requires_human_status_and_covers_crossing_top_90():
    items = pd.DataFrame(
        {
            "game": ["raw"] * 3,
            "review_group": ["rubber"] * 3,
            "review_item_id": ["SYN-A", "SYN-B", "SYN-C"],
            "value_usd": [60.0, 31.0, 9.0],
        }
    )
    reviews = pd.DataFrame(
        {
            "game": ["raw", "raw"],
            "review_group": ["rubber", "rubber"],
            "review_item_id": ["SYN-A", "SYN-B"],
            "review_status": ["confirmed", "probable"],
            "source_note": ["synthetic source A", "synthetic source B"],
        }
    )
    queue = build_manual_review_queue(items, reviews)
    assert list(queue["review_item_id"]) == ["SYN-A", "SYN-B", "SYN-C"]
    assert list(queue["required_top90"]) == [1, 1, 0]
    assert queue.loc[queue["required_top90"].eq(1), "review_complete"].all()
    assert queue.loc[queue["review_item_id"].eq("SYN-A"), "main_eligible"].iat[0] == 1
    assert queue.loc[queue["review_item_id"].eq("SYN-A"), "confirmed_eligible"].iat[0] == 1
    assert queue.loc[queue["review_item_id"].eq("SYN-B"), "confirmed_eligible"].iat[0] == 0


def test_manual_review_queue_never_auto_approves_and_rejects_bad_review_contract():
    items = pd.DataFrame(
        {"game": ["finished"], "review_group": ["passenger"],
         "review_item_id": ["SYN-X"], "value_usd": [1.0]}
    )
    queue = build_manual_review_queue(items)
    assert pd.isna(queue["review_status"]).all()
    assert queue["review_complete"].eq(0).all()
    bad = items.assign(review_status="estimated", source_note="model")
    with pytest.raises(ValueError, match="contract|review_status"):
        build_manual_review_queue(items, bad)


def test_g3_requires_approved_keys_and_g7_requires_both_games(tmp_path):
    raw, finished, panels, seed, validation, queue, annual, inside = _qa_fixture(tmp_path)
    seed.loc[2, "manufacturer_key"] = "UNAPPROVED"
    queue = queue.loc[queue["game"].eq("raw")]
    result = evaluate_gates(
        chunks={"raw": raw, "finished": finished}, origin_panels=panels,
        parent_seed=seed, validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside], licensed_root=tmp_path / "licensed",
    )
    gates = result["gates"].set_index("gate")
    assert gates.loc["G3", "status"] == "fail"
    assert gates.loc["G7", "status"] == "fail"


def test_g6_uses_only_estimation_sample_value(tmp_path):
    raw, finished, panels, seed, validation, queue, annual, inside = _qa_fixture(tmp_path)
    raw["shipper_up"] = pd.Series([pd.NA] * len(raw), dtype="Int64")
    raw.loc[:, "estimation_eligible"] = 0
    extra = raw.iloc[[0]].copy()
    extra["shipper_up"] = 11
    extra["estimation_eligible"] = 1
    raw = pd.concat([raw, extra], ignore_index=True)
    result = evaluate_gates(
        chunks={"raw": raw, "finished": finished}, origin_panels=panels,
        parent_seed=seed, validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside], licensed_root=tmp_path / "licensed",
    )
    assert result["gates"].set_index("gate").loc["G6", "status"] == "pass"


def test_g1_rejects_nonpositive_manufacturer_and_blank_market_code(tmp_path):
    raw, finished, panels, seed, validation, queue, annual, inside = _qa_fixture(tmp_path)
    panels["raw"].loc[0, "manufacturer_parent_id"] = 0
    panels["finished"]["finished_market"] = ""
    result = evaluate_gates(
        chunks={"raw": raw, "finished": finished}, origin_panels=panels,
        parent_seed=seed, validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside], licensed_root=tmp_path / "licensed",
    )
    assert result["gates"].set_index("gate").loc["G1", "status"] == "fail"


def _qa_fixture(tmp_path):
    raw = pd.DataFrame(
        {
            "manufacturer_parent_id": [1, 1, 2, 2, 3, 3],
            "shipper_up": [11, 12, 21, 22, 31, 32],
            "origin_country": ["KOR", "JPN"] * 3,
            "year_quarter": ["2024Q1"] * 6,
            "value_usd": [10.0] * 6,
            "weight_kg": [1.0] * 6,
            "teu": [0.1] * 6,
            "container_count": [0.1] * 6,
            "shipment_equivalent": [1.0] * 6,
            "shipment_count_nonadditive": [1] * 6,
            "manufacturer_conflict": [0] * 6,
            "manufacturer_conflict_value_usd": [0.0] * 6,
            "importer_pit_same_parent_overlap_value_usd": [0.0] * 6,
            "shipper_pit_same_parent_overlap_value_usd": [0.0] * 6,
            "estimation_eligible": [1] * 6,
        }
    )
    finished = raw.drop(columns="shipper_up").assign(
        import_route=["manufacturer_direct", "unattributed"] * 3
    )
    origin_panels = {}
    for game, source in (("raw", raw), ("finished", finished)):
        panel = source.groupby(
            ["manufacturer_parent_id", "origin_country", "year_quarter"],
            as_index=False,
        ).agg(
            value_usd=("value_usd", "sum"),
            weight_kg=("weight_kg", "sum"),
            teu=("teu", "sum"),
            container_count=("container_count", "sum"),
            shipment_equivalent=("shipment_equivalent", "sum"),
        )
        origin_panels[game] = panel
    seed = pd.DataFrame(
        {"manufacturer_key": ["MICHELIN", "GOODYEAR", "HANKOOK"],
         "manufacturer_parent_id": [1, 2, 3], "review_status": ["reviewed"] * 3}
    )
    validation = pd.DataFrame(
        {"game": ["raw", "finished"], "quarter": ["2024Q1"] * 2,
         "direct_row_count": [6, 6], "direct_value_usd": [60.0, 60.0],
         "direct_shipment_equivalent": [6.0, 6.0],
         "isolated_row_count": [6, 6], "isolated_value_usd": [60.0, 60.0],
         "isolated_shipment_equivalent": [6.0, 6.0], "reconciled": [1, 1]}
    )
    queue = pd.DataFrame(
        {"game": ["raw", "finished"], "review_group": ["all", "all"],
         "review_item_id": ["SYN-R", "SYN-F"], "value_usd": [60.0, 60.0],
         "cumulative_value_share": [1.0, 1.0], "required_top90": [1, 1],
         "review_status": ["confirmed", "probable"], "source_note": ["a", "b"],
         "review_complete": [1, 1], "main_eligible": [1, 1],
         "confirmed_eligible": [1, 0]}
    )
    annual_origin = pd.DataFrame(
        [(m, origin, year, 1) for m in (1, 2, 3) for origin in ("KOR", "JPN")
         for year in (2022, 2023, 2024)],
        columns=["manufacturer_parent_id", "link_id", "year", "active"],
    )
    inside = tmp_path / "licensed" / "panel.parquet"
    return raw, finished, origin_panels, seed, validation, queue, annual_origin, inside


def test_all_gates_pass_and_g6_only_controls_supplier_eligibility(tmp_path):
    raw, finished, panels, seed, validation, queue, annual, inside = _qa_fixture(tmp_path)
    result = evaluate_gates(
        chunks={"raw": raw, "finished": finished}, origin_panels=panels,
        parent_seed=seed, validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside], licensed_root=tmp_path / "licensed",
    )
    assert result["exit_code"] == 0
    assert result["supplier_game_eligible"] is True
    assert set(result["gates"]["status"]) == {"pass"}

    raw["shipper_up"] = pd.NA
    result = evaluate_gates(
        chunks={"raw": raw, "finished": finished}, origin_panels=panels,
        parent_seed=seed, validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside], licensed_root=tmp_path / "licensed",
    )
    assert result["exit_code"] == 0
    assert result["supplier_game_eligible"] is False
    assert result["gates"].set_index("gate").loc["G6", "status"] == "fail"


@pytest.mark.parametrize("gate", ["G0", "G1", "G2", "G3", "G4", "G5", "G7", "G8", "G9"])
def test_required_gate_failure_is_nonzero(tmp_path, gate):
    raw, finished, panels, seed, validation, queue, annual, inside = _qa_fixture(tmp_path)
    kwargs = dict(
        chunks={"raw": raw, "finished": finished}, origin_panels=panels,
        parent_seed=seed, validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside], licensed_root=tmp_path / "licensed",
    )
    if gate == "G0":
        validation.loc[validation["game"].eq("raw"), "direct_value_usd"] = 999
    elif gate == "G1":
        panels["raw"] = pd.concat([panels["raw"], panels["raw"].iloc[[0]]])
    elif gate == "G2":
        panels["raw"].loc[0, "value_usd"] += 1
    elif gate == "G3":
        seed.loc[2, "manufacturer_parent_id"] = 2
    elif gate == "G4":
        raw.loc[0, "manufacturer_conflict_value_usd"] = 1
    elif gate == "G5":
        finished.loc[0:4, "import_route"] = ""
    elif gate == "G7":
        queue.loc[0, "review_complete"] = 0
    elif gate == "G8":
        annual = annual.loc[~((annual["manufacturer_parent_id"] == 1) & (annual["link_id"] == "JPN") & (annual["year"] == 2024))]
        kwargs["annual_origin"] = annual
    else:
        kwargs["licensed_paths"] = [tmp_path / "escaped.parquet"]
    result = evaluate_gates(**kwargs)
    assert result["exit_code"] != 0
    assert result["gates"].set_index("gate").loc[gate, "status"] == "fail"


def test_zero_denominators_are_explicit_not_applicable_and_do_not_pass(tmp_path):
    raw, finished, panels, seed, validation, queue, annual, inside = _qa_fixture(tmp_path)
    raw["value_usd"] = 0.0
    finished["value_usd"] = 0.0
    panels["raw"]["value_usd"] = 0.0
    panels["finished"]["value_usd"] = 0.0
    validation["direct_value_usd"] = 0.0
    validation["isolated_value_usd"] = 0.0
    result = evaluate_gates(
        chunks={"raw": raw, "finished": finished}, origin_panels=panels,
        parent_seed=seed, validation_metrics=validation, review_queue=queue.iloc[0:0],
        annual_origin=annual, licensed_paths=[inside], licensed_root=tmp_path / "licensed",
    )
    gates = result["gates"].set_index("gate")
    assert gates.loc["G5", "status"] == "not_applicable"
    assert gates.loc["G6", "status"] == "not_applicable"
    assert gates.loc["G7", "status"] == "not_applicable"
    assert result["exit_code"] != 0
    assert result["supplier_game_eligible"] is False


def test_capture_g0_validation_writes_only_inside_validation_run(tmp_path, monkeypatch):
    monkeypatch.setattr(extract_module, "validate_output_path", lambda path: Path(path).resolve())
    monkeypatch.setattr(qa_module, "validate_output_path", lambda path: Path(path).resolve())
    validation_root = tmp_path / "_validation" / "run1"
    for game in ("raw", "finished"):
        extract_module.extract_chunk(
            object(), game, "2024Q1",
            {"MICHELIN": 1, "GOODYEAR": 2, "HANKOOK": 3},
            parent_seed_sha256="seed", output_root=validation_root,
            query_fn=lambda connection, sql: output_frame(),
        )
    calls = []
    metrics = [
        pd.DataFrame({"ROW_COUNT": [1], "VALUE_USD": [12.0], "SHIPMENT_EQUIVALENT": [1.0]}),
        pd.DataFrame({"ROW_COUNT": [1], "VALUE_USD": [12.0], "SHIPMENT_EQUIVALENT": [1.0]}),
    ]
    result = capture_g0_validation(
        object(), "2024Q1", parent_ids={"MICHELIN": 1, "GOODYEAR": 2, "HANKOOK": 3},
        parent_seed_sha256="seed", validation_root=validation_root, output_root=tmp_path,
        query_fn=lambda connection, sql: calls.append(sql) or metrics.pop(0),
    )
    assert len(calls) == 2
    assert result["metrics_path"].endswith("g0_direct_metrics.parquet")
    saved = pd.read_parquet(result["metrics_path"])
    assert list(saved["game"]) == ["raw", "finished"]
    assert saved["reconciled"].eq(1).all()
    assert not (tmp_path / "panel_source_quarter_raw.parquet").exists()
