from pathlib import Path
import json

import pandas as pd
import pytest

import scripts.tire_ab_entry.extract as extract_module
import scripts.tire_ab_entry.qa as qa_module
import scripts.tire_ab_entry.transforms as transforms_module
from tests.tire_ab_entry.test_extract import output_frame
from scripts.tire_ab_entry.transforms import (
    apply_manual_reviews,
    build_game_artifacts,
    build_game_outputs,
    build_review_items,
    read_manual_review_snapshot,
)
from scripts.tire_ab_entry.qa import (
    build_manual_review_queue,
    capture_g0_validation,
    evaluate_gates,
    validate_transform_artifact,
)


def _review_items(game="raw", values=(60.0, 31.0, 9.0)):
    size = len(values)
    return pd.DataFrame(
        {
            "game": [game] * size,
            "manufacturer_parent_id": [1] * size,
            "review_group": ["rubber" if game == "raw" else "passenger"] * size,
            "origin_country": ["KOR"] * size,
            "input_group": ["rubber" if game == "raw" else pd.NA] * size,
            "finished_market": [pd.NA if game == "raw" else "passenger"] * size,
            "link_identity_type": ["ultimate_parent"] * size,
            "link_identity_value": [f"SYN-{i}" for i in range(size)],
            "review_item_id": [f"HASH-{game}-{i}" for i in range(size)],
            "value_usd": list(values),
        }
    )


def test_manual_review_queue_requires_human_status_and_covers_crossing_top_90():
    items = _review_items()
    reviews = items.iloc[:2].drop(columns="value_usd").assign(
        review_status=["confirmed", "probable"],
        source_note=["synthetic source A", "synthetic source B"],
    )
    queue = build_manual_review_queue(items, reviews.astype("string"))
    assert list(queue["review_item_id"]) == ["HASH-raw-0", "HASH-raw-1", "HASH-raw-2"]
    assert list(queue["required_top90"]) == [1, 1, 0]
    assert queue.loc[queue["required_top90"].eq(1), "review_complete"].all()
    assert queue.loc[queue["review_item_id"].eq("HASH-raw-0"), "main_eligible"].iat[0] == 1
    assert queue.loc[queue["review_item_id"].eq("HASH-raw-0"), "confirmed_eligible"].iat[0] == 1
    assert queue.loc[queue["review_item_id"].eq("HASH-raw-1"), "confirmed_eligible"].iat[0] == 0


def test_manual_review_queue_never_auto_approves_and_rejects_bad_review_contract():
    items = _review_items("finished", (1.0,))
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
    assert gates.loc["G7", "status"] == "not_applicable"


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


def test_g1_allows_na_manufacturer_only_for_finished_unattributed_rows(tmp_path):
    raw, finished, panels, seed, validation, queue, annual, inside = _qa_fixture(tmp_path)
    extra = panels["finished"].iloc[[0]].copy()
    extra["manufacturer_parent_id"] = pd.NA
    extra["import_route"] = "unattributed"
    extra["origin_country"] = "MEX"
    panels["finished"] = pd.concat([panels["finished"], extra], ignore_index=True)
    result = evaluate_gates(
        chunks={"raw": raw, "finished": finished}, origin_panels=panels,
        parent_seed=seed, validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside], licensed_root=tmp_path / "licensed",
    )
    assert result["gates"].set_index("gate").loc["G1", "status"] == "pass"

    attributed_na = {key: value.copy() for key, value in panels.items()}
    attributed_na["finished"].loc[
        attributed_na["finished"].index[-1], "import_route"
    ] = "manufacturer_direct"
    result = evaluate_gates(
        chunks={"raw": raw, "finished": finished}, origin_panels=attributed_na,
        parent_seed=seed, validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside], licensed_root=tmp_path / "licensed",
    )
    assert result["gates"].set_index("gate").loc["G1", "status"] == "fail"

    raw_na = {key: value.copy() for key, value in panels.items()}
    raw_na["finished"] = raw_na["finished"].iloc[:-1]
    raw_na["raw"].loc[0, "manufacturer_parent_id"] = pd.NA
    result = evaluate_gates(
        chunks={"raw": raw, "finished": finished}, origin_panels=raw_na,
        parent_seed=seed, validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside], licensed_root=tmp_path / "licensed",
    )
    assert result["gates"].set_index("gate").loc["G1", "status"] == "fail"


def test_g1_allows_unreported_measures_but_rejects_negative_values(tmp_path):
    raw, finished, panels, seed, validation, queue, annual, inside = _qa_fixture(tmp_path)
    # Panjiva reports no TEU for air and LCL shipments; the source contract
    # keeps those measures null, so an all-null group is a valid panel value.
    panels["raw"].loc[0, "teu"] = pd.NA
    result = evaluate_gates(
        chunks={"raw": raw, "finished": finished}, origin_panels=panels,
        parent_seed=seed, validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside], licensed_root=tmp_path / "licensed",
    )
    assert result["gates"].set_index("gate").loc["G1", "status"] == "pass"

    panels["raw"].loc[0, "teu"] = -1.0
    result = evaluate_gates(
        chunks={"raw": raw, "finished": finished}, origin_panels=panels,
        parent_seed=seed, validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside], licensed_root=tmp_path / "licensed",
    )
    assert result["gates"].set_index("gate").loc["G1", "status"] == "fail"


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


@pytest.mark.parametrize(
    ("game", "column", "value"),
    [
        ("raw", "year_quarter", "9999Q4"),
        ("raw", "input_group", "UNKNOWN_RAW_GROUP"),
        ("raw", "finished_market", "passenger_vehicle"),
        ("raw", "import_route", "conflict"),
        ("finished", "finished_market", "UNKNOWN_FINISHED_MARKET"),
        ("finished", "input_group", "4001"),
        ("finished", "import_route", "unknown_route"),
        ("finished", "origin_country", " KOR"),
        ("finished", "origin_country", "KOR\nJPN"),
    ],
)
def test_g1_rejects_out_of_contract_source_domains(
    tmp_path, game, column, value
):
    raw, finished, panels, seed, validation, queue, annual, inside = _qa_fixture(tmp_path)
    chunks = {"raw": raw, "finished": finished}
    chunks[game].loc[0, column] = value
    result = evaluate_gates(
        chunks=chunks, origin_panels=panels, parent_seed=seed,
        validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside],
        licensed_root=tmp_path / "licensed",
    )
    row = result["gates"].set_index("gate").loc["G1"]
    assert row["status"] == "fail"
    assert "origin_basis=" in row["detail"]


def _qa_fixture(tmp_path):
    raw = pd.DataFrame(
        {
            "manufacturer_parent_id": [1, 1, 2, 2, 3, 3],
            "shipper_up": [11, 12, 21, 22, 31, 32],
            "origin_country": ["KOR", "JPN"] * 3,
            "year_quarter": ["2024Q1"] * 6,
            "input_group": ["4001"] * 6,
            "finished_market": [pd.NA] * 6,
            "import_route": ["manufacturer_direct"] * 6,
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
            "importer_pit_same_parent_overlap": [0] * 6,
            "shipper_pit_same_parent_overlap": [0] * 6,
            "importer_pit_same_parent_overlap_shipment_count_nonadditive": [0] * 6,
            "shipper_pit_same_parent_overlap_shipment_count_nonadditive": [0] * 6,
            "importer_pit_same_parent_overlap_shipment_equivalent": [0.0] * 6,
            "shipper_pit_same_parent_overlap_shipment_equivalent": [0.0] * 6,
            "estimation_eligible": [1] * 6,
        }
    )
    finished = raw.drop(columns="shipper_up").assign(
        input_group=pd.NA,
        finished_market=["passenger_vehicle", "light_truck_on_highway"] * 3,
        import_route=["manufacturer_direct", "unattributed"] * 3,
    )
    origin_panels = {}
    for game, source in (("raw", raw), ("finished", finished)):
        keys = ["manufacturer_parent_id", "origin_country"]
        if game == "raw":
            keys.append("input_group")
        else:
            keys.extend(["import_route", "finished_market"])
        keys.append("year_quarter")
        panel = source.groupby(
            keys,
            as_index=False,
            dropna=False,
        ).agg(
            value_usd=("value_usd", "sum"),
            weight_kg=("weight_kg", "sum"),
            teu=("teu", "sum"),
            container_count=("container_count", "sum"),
            shipment_equivalent=("shipment_equivalent", "sum"),
        )
        panel["shipment_count_source_group_sum_nonadditive"] = 1
        origin_panels[game] = panel
    seed = pd.DataFrame(
        {"manufacturer_key": ["MICHELIN", "GOODYEAR", "HANKOOK"],
         "manufacturer_parent_id": [1, 2, 3], "review_status": ["reviewed"] * 3}
    )
    validation = pd.DataFrame(
        {"game": ["raw", "finished"], "quarter": ["2024Q1"] * 2,
         "direct_output_row_count": [6, 6], "direct_unique_shipment_count": [6, 6],
         "direct_value_usd": [60.0, 60.0],
         "direct_shipment_equivalent": [6.0, 6.0],
         "isolated_row_count": [6, 6],
         "isolated_unique_shipment_count": [6, 6],
         "isolated_value_usd": [60.0, 60.0],
         "isolated_shipment_equivalent": [6.0, 6.0], "reconciled": [1, 1]}
    )
    queue = pd.concat(
        [
            build_manual_review_queue(
                items,
                items.drop(columns="value_usd").assign(
                    review_status="confirmed", source_note="human"
                ),
            )
            for items in (_review_items("raw", (60.0,)), _review_items("finished", (60.0,)))
        ],
        ignore_index=True,
    )
    annual_origin = pd.DataFrame(
        [(m, origin, year, 1) for m in (1, 2, 3) for origin in ("KOR", "JPN")
         for year in (2022, 2023, 2024)],
        columns=["manufacturer_parent_id", "link_id", "year", "active"],
    )
    inside = tmp_path / "licensed" / "panel.parquet"
    annual = {"raw": annual_origin.copy(), "finished": annual_origin.copy()}
    return raw, finished, origin_panels, seed, validation, queue, annual, inside


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
        annual["finished"] = annual["finished"].loc[
            ~(
                (annual["finished"]["manufacturer_parent_id"] == 1)
                & (annual["finished"]["link_id"] == "JPN")
            )
        ]
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


def test_g7_ignores_zero_value_groups_when_positive_groups_are_covered(tmp_path):
    raw, finished, panels, seed, validation, queue, annual, inside = _qa_fixture(tmp_path)
    zero_group = queue.iloc[[0]].copy()
    zero_group["manufacturer_parent_id"] = 2
    zero_group["review_group"] = "zero-value-only"
    zero_group["review_item_id"] = "HASH-ZERO"
    zero_group["link_identity_value"] = "SYN-ZERO"
    zero_group["value_usd"] = 0.0
    zero_group["cumulative_value_share"] = 0.0
    zero_group["required_top90"] = 0
    zero_group["review_status"] = "unclear"
    zero_group["source_note"] = "zero-value group evidence"
    zero_group["review_complete"] = 1
    zero_group["main_eligible"] = 0
    zero_group["confirmed_eligible"] = 0
    queue = pd.concat([queue, zero_group], ignore_index=True)
    result = evaluate_gates(
        chunks={"raw": raw, "finished": finished}, origin_panels=panels,
        parent_seed=seed, validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside],
        licensed_root=tmp_path / "licensed",
    )
    assert result["gates"].set_index("gate").loc["G7", "status"] == "pass"


def test_g7_is_not_applicable_when_a_required_game_has_no_positive_denominator(
    tmp_path,
):
    raw, finished, panels, seed, validation, queue, annual, inside = _qa_fixture(tmp_path)
    queue.loc[queue["game"].eq("finished"), "value_usd"] = 0.0
    queue.loc[queue["game"].eq("finished"), "required_top90"] = 0
    result = evaluate_gates(
        chunks={"raw": raw, "finished": finished}, origin_panels=panels,
        parent_seed=seed, validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside],
        licensed_root=tmp_path / "licensed",
    )
    assert result["gates"].set_index("gate").loc["G7", "status"] == "not_applicable"
    assert result["exit_code"] != 0


def test_g3_fails_on_zero_value_pit_overlap_occurrence(tmp_path):
    raw, finished, panels, seed, validation, queue, annual, inside = _qa_fixture(tmp_path)
    raw.loc[0, "importer_pit_same_parent_overlap"] = 1
    raw.loc[0, "importer_pit_same_parent_overlap_value_usd"] = 0.0
    result = evaluate_gates(
        chunks={"raw": raw, "finished": finished}, origin_panels=panels,
        parent_seed=seed, validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside], licensed_root=tmp_path / "licensed",
    )
    assert result["gates"].set_index("gate").loc["G3", "status"] == "fail"


def test_g8_counts_distinct_active_links_across_the_2022_2024_window(tmp_path):
    raw, finished, panels, seed, validation, queue, annual, inside = _qa_fixture(tmp_path)
    # Manufacturer 1 keeps KOR only in 2022 and JPN only in 2023: two distinct
    # links across the window although every single year has just one.
    frame = annual["finished"]
    annual["finished"] = frame.loc[
        ~(
            (frame["manufacturer_parent_id"] == 1)
            & (
                ((frame["link_id"] == "KOR") & (frame["year"] != 2022))
                | ((frame["link_id"] == "JPN") & (frame["year"] != 2023))
            )
        )
    ]
    result = evaluate_gates(
        chunks={"raw": raw, "finished": finished}, origin_panels=panels,
        parent_seed=seed, validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside], licensed_root=tmp_path / "licensed",
    )
    assert result["gates"].set_index("gate").loc["G8", "status"] == "pass"


def test_g8_reports_each_game_and_does_not_pool_origin_coverage(tmp_path):
    raw, finished, panels, seed, validation, queue, annual, inside = _qa_fixture(tmp_path)
    annual["finished"] = annual["finished"].iloc[0:0]
    result = evaluate_gates(
        chunks={"raw": raw, "finished": finished}, origin_panels=panels,
        parent_seed=seed, validation_metrics=validation, review_queue=queue,
        annual_origin=annual, licensed_paths=[inside], licensed_root=tmp_path / "licensed",
    )
    row = result["gates"].set_index("gate").loc["G8"]
    assert row["status"] == "fail"
    assert "raw" in row["detail"] and "finished" in row["detail"]


def test_capture_g0_validation_writes_only_inside_validation_run(tmp_path, monkeypatch):
    monkeypatch.setattr(extract_module, "validate_output_path", lambda path: Path(path).resolve())
    monkeypatch.setattr(qa_module, "validate_output_path", lambda path: Path(path).resolve())
    validation_root = tmp_path / "_validation" / "run1"
    for game in ("raw", "finished"):
        isolated = output_frame()
        isolated["shipment_equivalent"] = 0.5
        extract_module.extract_chunk(
            object(), game, "2024Q1",
            {"MICHELIN": 1, "GOODYEAR": 2, "HANKOOK": 3},
            parent_seed_sha256="seed", output_root=validation_root,
            query_fn=lambda connection, sql, frame=isolated: frame.copy(),
        )
    calls = []
    metrics = [
        pd.DataFrame({"OUTPUT_ROW_COUNT": [1], "UNIQUE_SHIPMENT_COUNT": [1], "VALUE_USD": [12.0], "SHIPMENT_EQUIVALENT": [0.5]}),
        pd.DataFrame({"OUTPUT_ROW_COUNT": [1], "UNIQUE_SHIPMENT_COUNT": [1], "VALUE_USD": [12.0], "SHIPMENT_EQUIVALENT": [0.5]}),
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
    assert saved["isolated_unique_shipment_count"].eq(1).all()
    assert saved["direct_shipment_equivalent"].eq(0.5).all()
    assert result["reconciled"] is True
    assert not (tmp_path / "panel_source_quarter_raw.parquet").exists()


@pytest.mark.parametrize(
    "mutation", ["extra", "missing", "dtype", "text_dtype", "duplicate"]
)
def test_transform_artifact_validator_is_exact_and_fail_closed(mutation):
    source = output_frame().assign(
        review_pending_technically_eligible=1,
        shipper_up=10,
        shipper_companyid=11,
        shipper_panjiva_id=12,
    )
    reviewed = apply_manual_reviews(source, game="raw", reviews=None)
    artifacts = build_game_artifacts(reviewed, game="raw")
    frame = artifacts["origin_quarterly"].copy()
    validate_transform_artifact("origin_quarterly", "raw", frame)
    if mutation == "extra":
        frame["unexpected"] = 1
    elif mutation == "missing":
        frame = frame.drop(columns="value_usd")
    elif mutation == "dtype":
        frame["value_usd"] = "not-numeric"
    elif mutation == "text_dtype":
        frame["origin_country"] = pd.Series(
            [123] * len(frame), index=frame.index, dtype="object"
        )
    else:
        frame = pd.concat([frame, frame], ignore_index=True)
    with pytest.raises(ValueError, match="artifact"):
        validate_transform_artifact("origin_quarterly", "raw", frame)


@pytest.mark.parametrize("game", ["raw", "finished"])
def test_all_named_transform_artifacts_satisfy_runtime_contract(game):
    source = output_frame().assign(
        review_pending_technically_eligible=1,
        shipper_up=10,
        shipper_companyid=11,
        shipper_panjiva_id=12,
    )
    artifacts = build_game_artifacts(
        apply_manual_reviews(source, game=game, reviews=None), game=game
    )
    artifacts["review_queue"] = build_manual_review_queue(
        build_review_items(source, game=game), None
    )
    for name, frame in artifacts.items():
        validate_transform_artifact(name, game, frame)


@pytest.mark.parametrize("mutation", ["modified", "deleted", "created"])
def test_transform_manifests_pin_one_manual_review_byte_snapshot(
    tmp_path, monkeypatch, mutation
):
    monkeypatch.setattr(
        transforms_module, "validate_output_path", lambda path: Path(path).resolve()
    )
    monkeypatch.setattr(
        transforms_module.artifact_io,
        "validate_output_path",
        lambda path: Path(path).resolve(),
    )
    source = output_frame().assign(
        shipper_up=10,
        shipper_companyid=11,
        shipper_panjiva_id=12,
        input_group="4001",
        finished_market="passenger_vehicle",
        origin_country="KOR",
    )

    def synthetic_chunks(game, **kwargs):
        frame = source.copy()
        frame.attrs["source_manifest_sha256"] = f"manifest-{game}"
        frame.attrs["source_chunk_sha256"] = (f"chunk-{game}",)
        return frame

    monkeypatch.setattr(
        transforms_module, "load_verified_game_chunks", synthetic_chunks
    )
    review_path = tmp_path / "review" / "manual_link_reviews.csv"
    review_path.parent.mkdir(parents=True)
    reviews = pd.concat(
        [
            build_review_items(source, game=game).drop(columns="value_usd").assign(
                review_status="confirmed", source_note="synthetic human evidence"
            )
            for game in ("raw", "finished")
        ],
        ignore_index=True,
    )
    if mutation != "created":
        reviews.to_csv(review_path, index=False)
    for game in ("raw", "finished"):
        build_game_outputs(
            game=game,
            parent_seed_sha256="seed",
            manufacturer_parent_ids=[101],
            output_root=tmp_path,
        )
    manifests = [
        json.loads(
            (tmp_path / "_manifests" / f"transform_{game}.json").read_text()
        )
        for game in ("raw", "finished")
    ]
    assert manifests[0]["manual_review_snapshot"] == manifests[1][
        "manual_review_snapshot"
    ]
    if mutation == "modified":
        reviews.assign(source_note="changed after build").to_csv(
            review_path, index=False
        )
    elif mutation == "deleted":
        review_path.unlink()
    else:
        reviews.to_csv(review_path, index=False)
    current_snapshot, _ = read_manual_review_snapshot(review_path)
    continuity_snapshot, _ = transforms_module.read_ownership_continuity_snapshot(
        tmp_path / "review" / "importer_ownership_continuity.csv"
    )
    with pytest.raises(ValueError, match="review snapshot"):
        qa_module._load_transform_outputs(
            "raw",
            tmp_path,
            "seed",
            source_manifest_sha256="manifest-raw",
            source_chunk_sha256=["chunk-raw"],
            manual_review_snapshot=current_snapshot,
            ownership_continuity_snapshot=continuity_snapshot,
        )
