"""Measurement review queues and fail-closed G0--G9 research gates."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
import math
import hashlib
import json
from pathlib import Path

import pandas as pd

from .config import MANUFACTURER_KEYS, OUTPUT_ROOT, validate_output_path
from . import artifacts as artifact_io
from . import extract as extraction
from .transforms import load_verified_game_chunks


REVIEW_KEYS = ("game", "review_group", "review_item_id")
REVIEW_STATUSES = {"confirmed", "probable", "unclear"}
ADDITIVE = ("value_usd", "weight_kg", "teu", "container_count", "shipment_equivalent")
REQUIRED_GATES = {"G0", "G1", "G2", "G3", "G4", "G5", "G7", "G8", "G9"}
G0_COLUMNS = (
    "game",
    "quarter",
    "direct_row_count",
    "direct_value_usd",
    "direct_shipment_equivalent",
    "isolated_row_count",
    "isolated_value_usd",
    "isolated_shipment_equivalent",
    "reconciled",
)


def build_manual_review_queue(
    items: pd.DataFrame,
    reviews: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Order review items by value and apply only explicit human decisions."""

    required_items = {*REVIEW_KEYS, "value_usd"}
    if set(items.columns) != required_items:
        missing = sorted(required_items.difference(items.columns))
        extra = sorted(set(items.columns).difference(required_items))
        raise ValueError(f"review items contract mismatch (missing={missing}, extra={extra})")
    if items.duplicated(list(REVIEW_KEYS)).any():
        raise ValueError("review items have duplicate keys")
    queue = items.copy()
    queue["value_usd"] = pd.to_numeric(queue["value_usd"], errors="coerce")
    if queue["value_usd"].isna().any() or (queue["value_usd"] < 0).any():
        raise ValueError("review item value_usd must be finite and nonnegative")
    if not queue["value_usd"].map(lambda value: math.isfinite(float(value))).all():
        raise ValueError("review item value_usd must be finite and nonnegative")
    queue = queue.sort_values(
        ["game", "review_group", "value_usd", "review_item_id"],
        ascending=[True, True, False, True],
        kind="stable",
    ).reset_index(drop=True)
    group = queue.groupby(["game", "review_group"], sort=False)["value_usd"]
    total = group.transform("sum")
    prior = group.cumsum() - queue["value_usd"]
    queue["cumulative_value_share"] = group.cumsum().div(total.where(total.gt(0)))
    queue["required_top90"] = (total.gt(0) & prior.div(total).lt(0.90)).astype("int8")

    review_columns = [*REVIEW_KEYS, "review_status", "source_note"]
    if reviews is None:
        for column in ("review_status", "source_note"):
            queue[column] = pd.NA
    else:
        if set(reviews.columns) != set(review_columns):
            missing = sorted(set(review_columns).difference(reviews.columns))
            extra = sorted(set(reviews.columns).difference(review_columns))
            raise ValueError(
                f"reviews exact contract mismatch (missing={missing}, extra={extra})"
            )
        if reviews.duplicated(list(REVIEW_KEYS)).any():
            raise ValueError("reviews have duplicate keys")
        status = reviews["review_status"].astype("string")
        if status.isna().any() or not status.isin(REVIEW_STATUSES).all():
            raise ValueError("review_status must be confirmed, probable, or unclear")
        notes = reviews["source_note"].astype("string")
        if notes.isna().any() or notes.str.strip().eq("").any():
            raise ValueError("review source_note is required")
        queue = queue.merge(
            reviews[review_columns], on=list(REVIEW_KEYS), how="left", validate="one_to_one"
        )
    status = queue["review_status"].astype("string")
    notes = queue["source_note"].astype("string")
    queue["review_complete"] = (
        status.isin(REVIEW_STATUSES) & notes.notna() & notes.str.strip().ne("")
    ).astype("int8")
    queue["main_eligible"] = status.isin({"confirmed", "probable"}).fillna(False).astype("int8")
    queue["confirmed_eligible"] = status.eq("confirmed").fillna(False).astype("int8")
    return queue


def _gate(gate: str, status: str, metric: object, detail: str) -> dict:
    if status not in {"pass", "fail", "not_applicable"}:
        raise ValueError("invalid gate status")
    return {"gate": gate, "status": status, "metric": metric, "detail": detail}


def _finite_nonnegative(frame: pd.DataFrame, columns: Iterable[str]) -> bool:
    for column in columns:
        if column not in frame:
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.isna().any() or (values < 0).any():
            return False
        if not values.map(lambda value: math.isfinite(float(value))).all():
            return False
    return True


def _metrics(frame: pd.DataFrame) -> dict[str, float]:
    return {column: float(pd.to_numeric(frame[column]).sum()) for column in ADDITIVE}


def _g0(chunks: Mapping[str, pd.DataFrame], validation: pd.DataFrame) -> dict:
    if tuple(validation.columns) != G0_COLUMNS or validation.duplicated(["game", "quarter"]).any():
        return _gate("G0", "fail", pd.NA, "direct validation contract is malformed")
    checks = []
    for game in ("raw", "finished"):
        rows = validation.loc[
            validation["game"].eq(game) & validation["quarter"].eq("2024Q1")
        ]
        if len(rows) != 1:
            checks.append(False)
            continue
        row = rows.iloc[0]
        checks.append(
            int(row["reconciled"]) == 1
            and int(row["direct_row_count"]) == int(row["isolated_row_count"])
            and math.isclose(float(row["direct_value_usd"]), float(row["isolated_value_usd"]), rel_tol=1e-10, abs_tol=1e-6)
            and math.isclose(float(row["direct_shipment_equivalent"]), float(row["isolated_shipment_equivalent"]), rel_tol=1e-10, abs_tol=1e-8)
        )
    return _gate("G0", "pass" if all(checks) else "fail", int(sum(checks)), "2024Q1 direct SQL and isolated chunk metrics reconcile")


def _g1(origin_panels: Mapping[str, pd.DataFrame]) -> dict:
    valid = set(origin_panels) == {"raw", "finished"}
    for frame in origin_panels.values():
        keys = [column for column in ("manufacturer_parent_id", "origin_country", "import_route", "input_group", "finished_market", "year_quarter") if column in frame]
        valid &= bool(keys) and not frame.duplicated(keys).any()
        valid &= frame["year_quarter"].astype("string").str.fullmatch(r"[0-9]{4}Q[1-4]").all()
        valid &= frame["origin_country"].notna().all() and frame["origin_country"].astype("string").str.strip().ne("").all()
        manufacturer_ids = pd.to_numeric(
            frame["manufacturer_parent_id"], errors="coerce"
        )
        valid &= (
            manufacturer_ids.notna().all()
            and manufacturer_ids.gt(0).all()
            and manufacturer_ids.map(lambda value: float(value).is_integer()).all()
        )
        for code_column in ("input_group", "finished_market", "import_route"):
            if code_column in frame:
                valid &= (
                    frame[code_column].notna().all()
                    and frame[code_column].astype("string").str.strip().ne("").all()
                )
        valid &= _finite_nonnegative(
            frame,
            [*ADDITIVE, "shipment_count_source_group_sum_nonadditive"],
        )
    return _gate("G1", "pass" if valid else "fail", int(valid), "panel keys, codes, dates, and numeric measures are valid")


def _g2(chunks: Mapping[str, pd.DataFrame], panels: Mapping[str, pd.DataFrame]) -> dict:
    valid = set(chunks) == {"raw", "finished"} and set(panels) == {"raw", "finished"}
    if valid:
        for game in ("raw", "finished"):
            source_metrics = _metrics(chunks[game])
            panel_metrics = _metrics(panels[game])
            valid &= all(
                math.isclose(source_metrics[column], panel_metrics[column], rel_tol=1e-10, abs_tol=1e-6)
                for column in ADDITIVE
            )
    return _gate("G2", "pass" if valid else "fail", int(valid), "verified chunk additive totals equal primary panels")


def _g3(chunks: Mapping[str, pd.DataFrame], seed: pd.DataFrame) -> dict:
    required = {"manufacturer_key", "manufacturer_parent_id", "review_status"}
    valid = required.issubset(seed.columns) and len(seed) == 3
    if valid:
        valid &= seed["manufacturer_key"].nunique() == 3
        valid &= set(seed["manufacturer_key"]) == set(MANUFACTURER_KEYS)
        valid &= seed["manufacturer_parent_id"].nunique() == 3
        valid &= seed["review_status"].eq("reviewed").all()
        overlap = 0.0
        for frame in chunks.values():
            for column in (
                "importer_pit_same_parent_overlap_value_usd",
                "shipper_pit_same_parent_overlap_value_usd",
            ):
                if column not in frame:
                    valid = False
                else:
                    overlap += float(frame[column].sum())
        valid &= math.isclose(overlap, 0.0, abs_tol=1e-9)
    return _gate("G3", "pass" if valid else "fail", int(valid), "three unique reviewed parents and zero PIT-overlap value")


def _g4(chunks: Mapping[str, pd.DataFrame]) -> dict:
    conflict = 0.0
    valid = True
    for frame in chunks.values():
        if "manufacturer_conflict_value_usd" not in frame or "estimation_eligible" not in frame:
            valid = False
            continue
        conflict += float(frame.loc[frame["estimation_eligible"].eq(1), "manufacturer_conflict_value_usd"].sum())
    valid &= math.isclose(conflict, 0.0, abs_tol=1e-9)
    return _gate("G4", "pass" if valid else "fail", conflict, "estimation-sample manufacturer-conflict value is zero")


def _g5(finished: pd.DataFrame) -> dict:
    total = float(finished["value_usd"].sum())
    if total <= 0:
        return _gate("G5", "not_applicable", pd.NA, "finished-game value denominator is zero")
    recognized = {"manufacturer_direct", "distributor_intermediated", "unattributed"}
    covered = float(finished.loc[finished["import_route"].isin(recognized), "value_usd"].sum())
    share = covered / total
    return _gate("G5", "pass" if share >= 0.80 else "fail", share, "attributed plus explicit unattributed finished value coverage")


def _g6(raw: pd.DataFrame) -> dict:
    eligible = raw.loc[raw["estimation_eligible"].eq(1)]
    total = float(eligible["value_usd"].sum())
    if total <= 0:
        return _gate("G6", "not_applicable", pd.NA, "raw supplier-match denominator is zero")
    supplier = pd.to_numeric(eligible["shipper_up"], errors="coerce")
    matched = float(eligible.loc[supplier.notna() & supplier.gt(0), "value_usd"].sum())
    share = matched / total
    return _gate("G6", "pass" if share >= 0.70 else "fail", share, "supplier-parent matched raw value share")


def _g7(queue: pd.DataFrame) -> dict:
    if queue.empty:
        return _gate("G7", "not_applicable", pd.NA, "manual review denominator is zero")
    required = queue.loc[queue["required_top90"].eq(1)]
    if required.empty:
        return _gate("G7", "not_applicable", pd.NA, "no positive-value top-90 review set")
    groups = queue[["game", "review_group"]].drop_duplicates()
    covered_groups = required.groupby(["game", "review_group"])["review_complete"].all()
    valid = (
        set(queue["game"]) == {"raw", "finished"}
        and len(covered_groups) == len(groups)
        and bool(covered_groups.all())
    )
    return _gate("G7", "pass" if valid else "fail", int(valid), "top 90 percent cumulative value is manually reviewed in every game/group")


def _g8(annual: pd.DataFrame, seed: pd.DataFrame) -> dict:
    required = {"manufacturer_parent_id", "link_id", "year", "active"}
    if not required.issubset(annual.columns):
        return _gate("G8", "fail", 0, "annual origin contract is malformed")
    scope = annual.loc[annual["year"].isin((2022, 2023, 2024)) & annual["active"].eq(1)]
    counts = scope.groupby(["manufacturer_parent_id", "year"])["link_id"].nunique()
    expected = pd.MultiIndex.from_product(
        [sorted(seed["manufacturer_parent_id"].unique()), [2022, 2023, 2024]],
        names=["manufacturer_parent_id", "year"],
    )
    counts = counts.reindex(expected, fill_value=0)
    valid = len(expected) == 9 and counts.ge(2).all()
    return _gate("G8", "pass" if valid else "fail", int(counts.min()) if len(counts) else 0, "each manufacturer-year has at least two active origin links in 2022-2024")


def _g9(paths: Iterable[Path | str], root: Path | str) -> dict:
    canonical_root = Path(root).resolve(strict=False)
    checked = [Path(path).resolve(strict=False) for path in paths]
    valid = bool(checked) and all(path.is_relative_to(canonical_root) for path in checked)
    return _gate("G9", "pass" if valid else "fail", len(checked), "every licensed path resolves inside LICENSED_OUTPUT_ROOT")


def evaluate_gates(
    *,
    chunks: Mapping[str, pd.DataFrame],
    origin_panels: Mapping[str, pd.DataFrame],
    parent_seed: pd.DataFrame,
    validation_metrics: pd.DataFrame,
    review_queue: pd.DataFrame,
    annual_origin: pd.DataFrame,
    licensed_paths: Iterable[Path | str],
    licensed_root: Path | str,
) -> dict:
    """Evaluate G0--G9; only G6 may fail without invalidating origin games."""

    gates = pd.DataFrame(
        [
            _g0(chunks, validation_metrics),
            _g1(origin_panels),
            _g2(chunks, origin_panels),
            _g3(chunks, parent_seed),
            _g4(chunks),
            _g5(chunks["finished"]),
            _g6(chunks["raw"]),
            _g7(review_queue),
            _g8(annual_origin, parent_seed),
            _g9(licensed_paths, licensed_root),
        ]
    )
    required = gates.loc[gates["gate"].isin(REQUIRED_GATES)]
    exit_code = 0 if required["status"].eq("pass").all() else 1
    g6_status = gates.set_index("gate").loc["G6", "status"]
    return {
        "gates": gates,
        "exit_code": exit_code,
        "origin_game_eligible": exit_code == 0,
        "supplier_game_eligible": exit_code == 0 and g6_status == "pass",
    }


def capture_g0_validation(
    connection,
    quarter: str,
    *,
    parent_ids: Mapping[str, int],
    parent_seed_sha256: str,
    validation_root: Path | str,
    description_identity=None,
    output_root: Path | str = OUTPUT_ROOT,
    query_fn=None,
) -> dict:
    """Persist independent direct-SQL totals inside one isolated validation run."""

    start, end = extraction.quarter_bounds(quarter)
    root = Path(output_root).resolve(strict=False)
    run_root = Path(validation_root).resolve(strict=False)
    if not run_root.is_relative_to(root / "_validation"):
        raise ValueError("G0 validation root must be inside the isolated validation tree")
    if Path(output_root) == OUTPUT_ROOT:
        root = validate_output_path(root)
        run_root = validate_output_path(run_root)
    query = query_fn or extraction.execute_dataframe_query
    records = []
    sql_hashes = {}
    for game in ("raw", "finished"):
        base_sql = extraction._build_sql(
            game, parent_ids, start, end, description_identity
        ).rstrip().rstrip(";")
        direct_sql = f"""
select count(*) as row_count,
       coalesce(sum(value_usd), 0) as value_usd,
       coalesce(sum(shipment_equivalent), 0) as shipment_equivalent
from (
{base_sql}
) as g0_source
""".strip()
        sql_hashes[game] = hashlib.sha256(direct_sql.encode("utf-8")).hexdigest()
        result = query(connection, direct_sql)
        if not isinstance(result, pd.DataFrame):
            raise ValueError("G0 direct SQL result must be a dataframe")
        result = result.copy()
        result.columns = [str(column).strip().lower() for column in result.columns]
        if tuple(result.columns) != ("row_count", "value_usd", "shipment_equivalent") or len(result) != 1:
            raise ValueError("G0 direct SQL result has an invalid exact contract")
        row = result.iloc[0]
        values = [pd.to_numeric(pd.Series([row[column]]), errors="coerce").iat[0] for column in result.columns]
        if any(pd.isna(value) or not math.isfinite(float(value)) or float(value) < 0 for value in values):
            raise ValueError("G0 direct SQL metrics must be finite and nonnegative")
        if not float(values[0]).is_integer():
            raise ValueError("G0 row_count must be integral")
        manifest = extraction._read_manifest(run_root / "_manifests" / f"{game}.json")
        entry = manifest["chunks"].get(f"{game}/{quarter}")
        if (
            not extraction._entry_file_verified(entry)
            or entry.get("parent_seed_sha256") != parent_seed_sha256
        ):
            raise ValueError("G0 isolated validation chunk is stale or unverified")
        reconciled = (
            int(values[0]) == int(entry["row_count"])
            and math.isclose(float(values[1]), float(entry["allocated_value_usd_sum"]), rel_tol=1e-10, abs_tol=1e-6)
            and math.isclose(float(values[2]), float(entry["shipment_equivalent_sum"]), rel_tol=1e-10, abs_tol=1e-8)
        )
        records.append(
            {
                "game": game,
                "quarter": quarter,
                "direct_row_count": int(values[0]),
                "direct_value_usd": float(values[1]),
                "direct_shipment_equivalent": float(values[2]),
                "isolated_row_count": int(entry["row_count"]),
                "isolated_value_usd": float(entry["allocated_value_usd_sum"]),
                "isolated_shipment_equivalent": float(entry["shipment_equivalent_sum"]),
                "reconciled": int(reconciled),
            }
        )
    metrics = pd.DataFrame.from_records(records, columns=G0_COLUMNS)
    metrics_path = run_root / "g0_direct_metrics.parquet"
    metrics_hash = extraction.atomic_parquet(metrics, metrics_path)
    pointer_path = root / "_validation" / f"g0_{quarter}.current.json"
    pointer = {
        "manifest_version": "tire-g0-validation-pointer-v1",
        "quarter": quarter,
        "validation_root": str(run_root),
        "metrics_path": str(metrics_path),
        "metrics_sha256": metrics_hash,
        "parent_seed_sha256": parent_seed_sha256,
        "code_version": extraction.CODE_VERSION,
        "contract_version": extraction.CONTRACT_VERSION,
        "direct_sql_sha256": sql_hashes,
    }
    extraction._atomic_json(pointer, pointer_path)
    return {
        "quarter": quarter,
        "metrics_path": str(metrics_path),
        "pointer_path": str(pointer_path),
    }


def _load_transform_outputs(
    game: str,
    root: Path,
    parent_seed_sha256: str,
    *,
    source_manifest_sha256: str,
    source_chunk_sha256: Iterable[str],
) -> tuple[dict[str, pd.DataFrame], list[Path]]:
    manifest_path = root / "_manifests" / f"transform_{game}.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as error:
        raise ValueError("transform manifest is missing or unreadable") from error
    required = {
        "manifest_version",
        "game",
        "parent_seed_sha256",
        "source_manifest_sha256",
        "source_chunk_sha256",
        "outputs",
        "shipment_measurement_status",
    }
    if set(manifest) != required or manifest.get("manifest_version") != "tire-transform-manifest-v1":
        raise ValueError("transform manifest has an invalid exact contract")
    if (
        manifest.get("game") != game
        or manifest.get("parent_seed_sha256") != parent_seed_sha256
        or manifest.get("source_manifest_sha256") != source_manifest_sha256
        or manifest.get("source_chunk_sha256") != list(source_chunk_sha256)
    ):
        raise ValueError("transform manifest is stale")
    expected_names = {
        "origin_quarterly",
        "supplier_quarterly",
        "annual",
        "dynamic_moments",
        "review_queue",
    }
    if set(manifest["outputs"]) != expected_names:
        raise ValueError("transform output manifest is incomplete or unexpected")
    frames = {}
    paths = [manifest_path]
    for name in sorted(expected_names):
        entry = manifest["outputs"][name]
        if not isinstance(entry, dict) or set(entry) != {"path", "sha256"}:
            raise ValueError("transform output entry has an invalid exact contract")
        path = Path(str(entry["path"])).resolve(strict=False)
        if not path.is_relative_to(root) or extraction.sha256_file(path) != entry["sha256"]:
            raise ValueError("transform output path or checksum is stale")
        frames[name] = pd.read_parquet(path)
        paths.append(path)
    return frames, paths


def _load_g0_metrics(
    root: Path, parent_seed_sha256: str, quarter: str = "2024Q1"
) -> tuple[pd.DataFrame, list[Path]]:
    pointer_path = root / "_validation" / f"g0_{quarter}.current.json"
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except Exception as error:
        raise ValueError("G0 current validation pointer is missing or unreadable") from error
    required = {
        "manifest_version", "quarter", "validation_root", "metrics_path",
        "metrics_sha256", "parent_seed_sha256", "code_version",
        "contract_version", "direct_sql_sha256",
    }
    if set(pointer) != required or pointer["manifest_version"] != "tire-g0-validation-pointer-v1" or pointer["quarter"] != quarter:
        raise ValueError("G0 current validation pointer has an invalid contract")
    if (
        pointer["parent_seed_sha256"] != parent_seed_sha256
        or pointer["code_version"] != extraction.CODE_VERSION
        or pointer["contract_version"] != extraction.CONTRACT_VERSION
        or set(pointer["direct_sql_sha256"]) != {"raw", "finished"}
    ):
        raise ValueError("G0 current validation pointer is stale")
    metrics_path = Path(pointer["metrics_path"]).resolve(strict=False)
    validation_root = Path(pointer["validation_root"]).resolve(strict=False)
    if not validation_root.is_relative_to(root / "_validation") or not metrics_path.is_relative_to(validation_root):
        raise ValueError("G0 current validation path escapes its isolated run")
    if extraction.sha256_file(metrics_path) != pointer["metrics_sha256"]:
        raise ValueError("G0 validation metrics checksum is stale")
    metrics = pd.read_parquet(metrics_path)
    if tuple(metrics.columns) != G0_COLUMNS:
        raise ValueError("G0 validation metrics have an invalid exact contract")
    return metrics, [pointer_path, metrics_path]


def run_runtime_qa(
    *,
    parent_seed_sha256: str,
    output_root: Path | str = OUTPUT_ROOT,
) -> dict:
    """Load current licensed artifacts, evaluate all gates, and atomically report."""

    root = validate_output_path(Path(output_root))
    seed_path = root / "review" / "manufacturer_parent_seed.csv"
    if extraction.sha256_file(seed_path) != parent_seed_sha256:
        raise ValueError("runtime parent seed changed after validation")
    seed = pd.read_csv(seed_path)
    chunks = {
        game: load_verified_game_chunks(
            game,
            parent_seed_sha256=parent_seed_sha256,
            output_root=root,
        )
        for game in ("raw", "finished")
    }
    loaded = {}
    licensed_paths = [seed_path]
    for game in ("raw", "finished"):
        loaded[game], paths = _load_transform_outputs(
            game,
            root,
            parent_seed_sha256,
            source_manifest_sha256=chunks[game].attrs["source_manifest_sha256"],
            source_chunk_sha256=chunks[game].attrs["source_chunk_sha256"],
        )
        licensed_paths.extend(paths)
    validation, paths = _load_g0_metrics(root, parent_seed_sha256)
    licensed_paths.extend(paths)
    review_queue = pd.concat(
        [loaded[game]["review_queue"] for game in ("raw", "finished")],
        ignore_index=True,
    )
    report_parquet = root / "qa_full.parquet"
    report_json = root / "qa_full.json"
    report_markdown = root / "qa_full.md"
    licensed_paths.extend((report_parquet, report_json, report_markdown))
    result = evaluate_gates(
        chunks=chunks,
        origin_panels={game: loaded[game]["origin_quarterly"] for game in chunks},
        parent_seed=seed,
        validation_metrics=validation,
        review_queue=review_queue,
        annual_origin=loaded["raw"]["annual"],
        licensed_paths=licensed_paths,
        licensed_root=root,
    )
    gates = result["gates"]
    artifact_io.atomic_write_parquet(gates, report_parquet)
    gate_records = [
        {
            key: None if pd.isna(value) else value
            for key, value in record.items()
        }
        for record in gates.to_dict(orient="records")
    ]
    payload = {
        "contract_version": "tire-qa-g0-g9-v1",
        "exit_code": int(result["exit_code"]),
        "origin_game_eligible": bool(result["origin_game_eligible"]),
        "supplier_game_eligible": bool(result["supplier_game_eligible"]),
        "shipment_measurement_status": "pilot_proxy_exact_distinct_unavailable",
        "gates": gate_records,
    }
    artifact_io.atomic_write_json(payload, report_json)
    lines = [
        "# Tire AB-entry measurement QA",
        "",
        "Exact panel-level distinct shipments are unavailable in Task 5 output; entry_core uses allocated shipment equivalents as a pilot proxy.",
        "",
        "| Gate | Status | Metric | Detail |",
        "|---|---|---:|---|",
    ]
    for row in gates.itertuples(index=False):
        lines.append(f"| {row.gate} | {row.status} | {row.metric} | {row.detail} |")
    artifact_io.atomic_write_bytes("\n".join(lines).encode("utf-8"), report_markdown)
    return {
        "exit_code": int(result["exit_code"]),
        "origin_game_eligible": bool(result["origin_game_eligible"]),
        "supplier_game_eligible": bool(result["supplier_game_eligible"]),
        "qa_report": str(report_markdown),
    }
