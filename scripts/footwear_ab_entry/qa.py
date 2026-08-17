"""Measurement review queues and fail-closed G0--G9 research gates."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
import math
import hashlib
import json
from pathlib import Path

import pandas as pd

from .config import (
    G10_DISCLOSED_SHARES,
    G10_TOLERANCE,
    GAMES,
    GREATER_CHINA_ORIGINS,
    MANUFACTURER_KEYS,
    OUTPUT_ROOT,
    STRUCTURAL_WINDOW_YEARS,
    iter_quarters,
    validate_output_path,
)
from . import artifacts as artifact_io
from . import extract as extraction
from .sql import build_validation_sql
from .transforms import (
    ADDITIVE_COLUMNS,
    DIAGNOSTIC_VALUE_COLUMNS,
    REVIEW_IDENTITY_COLUMNS,
    apply_ownership_continuity,
    load_verified_game_chunks,
    normalize_review_identity,
    read_manual_review_snapshot,
    read_ownership_continuity_snapshot,
)


REVIEW_KEYS = REVIEW_IDENTITY_COLUMNS
REVIEW_STATUSES = {"confirmed", "probable", "unclear"}
ADDITIVE = ("value_usd", "weight_kg", "teu", "container_count", "shipment_equivalent")
REQUIRED_GATES = {"G0", "G1", "G2", "G3", "G4", "G5", "G7", "G8", "G9", "G10"}
G0_COLUMNS = (
    "game",
    "quarter",
    "direct_output_row_count",
    "direct_unique_shipment_count",
    "direct_value_usd",
    "direct_shipment_equivalent",
    "isolated_row_count",
    "isolated_unique_shipment_count",
    "isolated_value_usd",
    "isolated_shipment_equivalent",
    "reconciled",
)

_PANEL_DERIVED_SUMS = (
    "shipment_count_source_group_sum_nonadditive",
    "current_parent_fallback_value_usd",
    "pit_overlap_value_usd",
    "description_review_value_usd",
    "main_eligible_value_usd",
    "confirmed_sensitivity_value_usd",
    "manual_main_eligible_value_usd",
    "manual_confirmed_value_usd",
    "manual_main_eligible_shipment_equivalent",
    "manual_confirmed_shipment_equivalent",
    "manufacturer_direct_value_usd",
    "distributor_intermediated_value_usd",
    "unattributed_value_usd",
)
_PANEL_DIAGNOSTICS = tuple(
    column
    for column in DIAGNOSTIC_VALUE_COLUMNS
    if column not in ADDITIVE_COLUMNS and column not in _PANEL_DERIVED_SUMS
)
_PANEL_TRAILING = (
    "shipment_measurement_status",
    "current_parent_fallback_value_share",
    "pit_overlap_value_share",
    "description_review_value_share",
    "main_eligible_value_share",
    "confirmed_sensitivity_value_share",
    "manual_main_eligible_value_share",
    "manual_confirmed_value_share",
    "manufacturer_direct_value_share",
    "distributor_intermediated_value_share",
    "unattributed_value_share",
)


def _artifact_columns(name: str, game: str) -> tuple[str, ...]:
    if game not in {"raw", "finished"}:
        raise ValueError("artifact game must be raw or finished")
    market = "input_group" if game == "raw" else "finished_market"
    route = () if game == "raw" else ("import_route",)
    measures = (*ADDITIVE_COLUMNS, *_PANEL_DERIVED_SUMS, *_PANEL_DIAGNOSTICS, *_PANEL_TRAILING)
    if name == "origin_quarterly":
        return (
            "game", "link_level", "link_id", "manufacturer_parent_id",
            "origin_country", *route, market, "year_quarter", *measures,
        )
    if name == "supplier_quarterly":
        return (
            "game", "link_level", "link_id", "manufacturer_parent_id",
            "supplier_parent_id", "supplier_parent_matched", "origin_country",
            *route, market, "year_quarter", *measures,
        )
    if name == "annual":
        return (
            "game", "manufacturer_parent_id", "link_id", "year", "value_usd",
            "shipment_equivalent_sum",
            "shipment_count_source_group_sum_nonadditive", "link_universe_basis",
            "observed", "active",
            "entry_raw", "entry_value", "entry_core", "entry_core_censored",
            "entry_core_count_basis", "entry_core_measurement_status",
        )
    if name == "dynamic_moments":
        return (
            "game", "window_start", "window_end", "entry_risk_n",
            "active_risk_n", "entry_rate", "exit_rate", "persistence_rate",
            "targeted", "measurement_status",
        )
    if name == "review_queue":
        return (
            *REVIEW_IDENTITY_COLUMNS, "value_usd", "cumulative_value_share",
            "required_top90", "review_status", "source_note", "review_complete",
            "main_eligible", "confirmed_eligible",
        )
    raise ValueError("unknown transform artifact")


def _artifact_keys(name: str, game: str) -> tuple[str, ...]:
    columns = _artifact_columns(name, game)
    if name == "origin_quarterly":
        return tuple(columns[: columns.index("value_usd")])
    if name == "supplier_quarterly":
        return tuple(columns[: columns.index("value_usd")])
    if name == "annual":
        return ("game", "manufacturer_parent_id", "link_id", "year")
    if name == "dynamic_moments":
        return ("game", "window_start", "window_end")
    return REVIEW_IDENTITY_COLUMNS


def validate_transform_artifact(name: str, game: str, frame: pd.DataFrame) -> None:
    """Fail closed on exact names, semantic types, keys, and fixed content."""

    expected = _artifact_columns(name, game)
    if tuple(frame.columns) != expected or frame.columns.duplicated().any():
        raise ValueError(f"transform artifact {name} has an invalid exact schema")
    if frame.duplicated(list(_artifact_keys(name, game))).any():
        raise ValueError(f"transform artifact {name} has duplicate keys")
    text = {
        "game", "link_level", "link_id", "origin_country", "year_quarter", "input_group",
        "finished_market", "import_route", "shipment_measurement_status",
        "entry_core_count_basis", "entry_core_measurement_status",
        "link_universe_basis",
        "measurement_status", "review_group", "link_identity_type",
        "link_identity_value", "review_item_id", "review_status", "source_note",
    }
    for column in expected:
        if column in text:
            series = frame[column]
            object_strings = (
                pd.api.types.is_object_dtype(series.dtype)
                and series.dropna().map(lambda value: isinstance(value, str)).all()
            )
            valid_text = (
                object_strings
                if pd.api.types.is_object_dtype(series.dtype)
                else pd.api.types.is_string_dtype(series.dtype)
            )
            if not valid_text:
                raise ValueError(f"transform artifact {name} text type is invalid")
        elif not pd.api.types.is_numeric_dtype(frame[column].dtype):
            raise ValueError(f"transform artifact {name} numeric type is invalid")
    if not frame.empty and not frame["game"].astype("string").eq(game).all():
        raise ValueError(f"transform artifact {name} game content is invalid")
    if name.endswith("quarterly"):
        expected_level = "origin" if name.startswith("origin") else "supplier"
        if not frame["link_level"].astype("string").eq(expected_level).all():
            raise ValueError(f"transform artifact {name} link level is invalid")


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
    items = normalize_review_identity(items)
    if items.duplicated(list(REVIEW_KEYS)).any():
        raise ValueError("review items have duplicate keys")
    queue = items.copy()
    queue["value_usd"] = pd.to_numeric(queue["value_usd"], errors="coerce")
    if queue["value_usd"].isna().any() or (queue["value_usd"] < 0).any():
        raise ValueError("review item value_usd must be finite and nonnegative")
    if not queue["value_usd"].map(lambda value: math.isfinite(float(value))).all():
        raise ValueError("review item value_usd must be finite and nonnegative")
    queue = queue.sort_values(
        [
            "game", "manufacturer_parent_id", "review_group", "value_usd",
            "review_item_id",
        ],
        ascending=[True, True, True, False, True],
        kind="stable",
    ).reset_index(drop=True)
    review_groups = ["game", "manufacturer_parent_id", "review_group"]
    group = queue.groupby(review_groups, sort=False)["value_usd"]
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
        reviews = normalize_review_identity(reviews)
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
    queue["review_status"] = status
    queue["source_note"] = notes
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
    # Mirrors the extraction output contract: physical measures may be
    # unreported (null) in Panjiva, but every reported value must be a finite
    # nonnegative number.
    for column in columns:
        if column not in frame:
            continue
        values = pd.to_numeric(frame[column], errors="coerce").dropna()
        if (values < 0).any():
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
    for game in GAMES:
        rows = validation.loc[
            validation["game"].eq(game) & validation["quarter"].eq("2024Q1")
        ]
        if len(rows) != 1:
            checks.append(False)
            continue
        row = rows.iloc[0]
        checks.append(
            int(row["reconciled"]) == 1
            and int(row["direct_output_row_count"]) == int(row["isolated_row_count"])
            and int(row["direct_unique_shipment_count"])
            == int(row["isolated_unique_shipment_count"])
            and math.isclose(float(row["direct_value_usd"]), float(row["isolated_value_usd"]), rel_tol=1e-10, abs_tol=1e-6)
            and math.isclose(float(row["direct_shipment_equivalent"]), float(row["isolated_shipment_equivalent"]), rel_tol=1e-10, abs_tol=1e-8)
        )
    return _gate("G0", "pass" if all(checks) else "fail", int(sum(checks)), "2024Q1 direct SQL and isolated chunk metrics reconcile")


ORIGIN_VALIDATION_BASIS = (
    "nonblank_trimmed_printable_source_token_or_LITERAL_UNKNOWN_max128"
)
FINISHED_MARKETS = {
    "athletic_sports",
    "athletic_escalated_general",
    "general_footwear_unreviewed",
}
RAW_ROUTES = {"manufacturer_direct", "unattributed"}
FINISHED_ROUTES = {
    "manufacturer_direct",
    "distributor_intermediated",
    "unattributed",
    "conflict",
}


def _valid_origin_tokens(series: pd.Series) -> bool:
    return bool(
        series.notna().all()
        and series.map(
            lambda value: (
                isinstance(value, str)
                and value == value.strip()
                and 1 <= len(value) <= 128
                and value.isprintable()
            )
        ).all()
    )


def _g1(
    origin_panels: Mapping[str, pd.DataFrame],
    chunks: Mapping[str, pd.DataFrame],
) -> dict:
    valid = set(origin_panels) == set(GAMES) and set(chunks) == set(GAMES)
    approved_quarters = set(iter_quarters())
    for game, frame in origin_panels.items():
        if game not in {"raw", "finished"}:
            valid = False
            continue
        market = "input_group" if game == "raw" else "finished_market"
        keys = [
            "manufacturer_parent_id",
            "origin_country",
            *(["import_route"] if game == "finished" else []),
            market,
            "year_quarter",
        ]
        if not set(keys).issubset(frame.columns):
            valid = False
            continue
        valid &= not frame.duplicated(keys).any()
        valid &= frame["year_quarter"].isin(approved_quarters).all()
        valid &= _valid_origin_tokens(frame["origin_country"])
        manufacturer_ids = pd.to_numeric(
            frame["manufacturer_parent_id"], errors="coerce"
        )
        positive_integer = (
            manufacturer_ids.notna()
            & manufacturer_ids.gt(0)
            & manufacturer_ids.map(
                lambda value: pd.notna(value) and float(value).is_integer()
            )
        )
        if game == "finished":
            # Unattributed finished groups legitimately carry no manufacturer;
            # every attributed-route row still requires a positive integer ID.
            unattributed_na = manufacturer_ids.isna() & frame["import_route"].astype(
                "string"
            ).eq("unattributed")
            valid &= bool((positive_integer | unattributed_na).all())
        else:
            valid &= bool(positive_integer.all())
        if game == "raw":
            # The footwear pilot has no raw game; any raw panel is invalid.
            valid = False
        else:
            valid &= frame[market].isin(FINISHED_MARKETS).all()
            valid &= frame["import_route"].isin(FINISHED_ROUTES).all()
            valid &= "input_group" not in frame or frame["input_group"].isna().all()
        valid &= _finite_nonnegative(
            frame,
            [*ADDITIVE, "shipment_count_source_group_sum_nonadditive"],
        )
    for game, frame in chunks.items():
        if game not in {"raw", "finished"}:
            valid = False
            continue
        required = {
            "year_quarter",
            "origin_country",
            "input_group",
            "finished_market",
            "import_route",
        }
        valid &= required.issubset(frame.columns)
        if not required.issubset(frame.columns):
            continue
        valid &= frame["year_quarter"].isin(approved_quarters).all()
        valid &= _valid_origin_tokens(frame["origin_country"])
        if game == "raw":
            # The footwear pilot has no raw game; any raw chunk is invalid.
            valid = False
        else:
            valid &= frame["finished_market"].isin(FINISHED_MARKETS).all()
            valid &= frame["input_group"].isna().all()
            valid &= frame["import_route"].isin(FINISHED_ROUTES).all()
    return _gate(
        "G1",
        "pass" if valid else "fail",
        int(valid),
        "panel/source keys, approved quarters and semantic domains are valid; "
        f"origin_basis={ORIGIN_VALIDATION_BASIS}",
    )


def _g2(chunks: Mapping[str, pd.DataFrame], panels: Mapping[str, pd.DataFrame]) -> dict:
    valid = set(chunks) == set(GAMES) and set(panels) == set(GAMES)
    if valid:
        for game in GAMES:
            source_metrics = _metrics(chunks[game])
            panel_metrics = _metrics(panels[game])
            valid &= all(
                math.isclose(source_metrics[column], panel_metrics[column], rel_tol=1e-10, abs_tol=1e-6)
                for column in ADDITIVE
            )
    return _gate("G2", "pass" if valid else "fail", int(valid), "verified chunk additive totals equal primary panels")


def _g3(chunks: Mapping[str, pd.DataFrame], seed: pd.DataFrame) -> dict:
    required = {"manufacturer_key", "manufacturer_parent_id", "review_status"}
    valid = required.issubset(seed.columns) and len(seed) == len(MANUFACTURER_KEYS)
    if valid:
        valid &= seed["manufacturer_key"].nunique() == len(MANUFACTURER_KEYS)
        valid &= set(seed["manufacturer_key"]) == set(MANUFACTURER_KEYS)
        valid &= seed["manufacturer_parent_id"].nunique() == len(MANUFACTURER_KEYS)
        valid &= seed["review_status"].eq("reviewed").all()
        overlap = 0.0
        for frame in chunks.values():
            for side in ("importer", "shipper"):
                columns = (
                    f"{side}_pit_same_parent_overlap",
                    f"{side}_pit_same_parent_overlap_shipment_count_nonadditive",
                    f"{side}_pit_same_parent_overlap_shipment_equivalent",
                    f"{side}_pit_same_parent_overlap_value_usd",
                )
                for column in columns:
                    if column not in frame:
                        valid = False
                    else:
                        overlap += float(pd.to_numeric(frame[column]).sum())
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
    group_columns = ["game", "manufacturer_parent_id", "review_group"]
    group_total = queue.groupby(group_columns, dropna=False)["value_usd"].transform("sum")
    positive = queue.loc[group_total.gt(0)].copy()
    if positive.empty or set(positive["game"]) != set(GAMES):
        return _gate(
            "G7",
            "not_applicable",
            pd.NA,
            "a required game has no positive manual-review denominator",
        )
    required = positive.loc[positive["required_top90"].eq(1)]
    if required.empty:
        return _gate("G7", "not_applicable", pd.NA, "no positive-value top-90 review set")
    groups = positive[group_columns].drop_duplicates()
    covered_groups = required.groupby(group_columns)["review_complete"].all()
    valid = (
        len(covered_groups) == len(groups)
        and bool(covered_groups.all())
    )
    return _gate("G7", "pass" if valid else "fail", int(valid), "top 90 percent cumulative value is manually reviewed in every game/group")


def _g8(annual: Mapping[str, pd.DataFrame], seed: pd.DataFrame) -> dict:
    required = {"manufacturer_parent_id", "link_id", "year", "active"}
    if set(annual) != set(GAMES):
        return _gate("G8", "fail", 0, "raw and finished annual games are required")
    # The design gate reads "at least two active origin links during the
    # structural window", so links are counted distinct across the window.
    expected = pd.Index(
        sorted(seed["manufacturer_parent_id"].unique()),
        name="manufacturer_parent_id",
    )
    minima = {}
    valid = len(expected) == len(MANUFACTURER_KEYS)
    for game in GAMES:
        frame = annual[game]
        if not required.issubset(frame.columns):
            minima[game] = 0
            valid = False
            continue
        scope = frame.loc[
            frame["year"].isin(STRUCTURAL_WINDOW_YEARS) & frame["active"].eq(1)
        ]
        counts = scope.groupby("manufacturer_parent_id")["link_id"].nunique()
        counts = counts.reindex(expected, fill_value=0)
        minima[game] = int(counts.min()) if len(counts) else 0
        valid &= counts.ge(2).all()
    detail = (
        "minimum active origins by game: "
        + ", ".join(f"{game}={minima.get(game, 0)}" for game in GAMES)
    )
    return _gate(
        "G8",
        "pass" if valid else "fail",
        min(minima.values()) if minima else 0,
        detail,
    )


def _g10(finished: pd.DataFrame, seed: pd.DataFrame) -> dict:
    """Panjiva athletic origin value shares versus 10-K production shares.

    Shipment origins in Greater China are aggregated (re-invoicing hubs mask
    the production country); the value-versus-pairs basis difference is part
    of the documented tolerance.  Applies only to firms with disclosed
    country shares.
    """

    labels = dict(
        zip(
            pd.to_numeric(seed["manufacturer_parent_id"], errors="coerce"),
            seed["manufacturer_key"].astype(str),
        )
    )
    scoped = finished.copy()
    scoped["manufacturer"] = pd.to_numeric(
        scoped["manufacturer_parent_id"], errors="coerce"
    ).map(labels)
    scoped["year"] = (
        scoped["year_quarter"].astype("string").str.slice(0, 4).astype(int)
    )
    scoped = scoped[
        scoped["manufacturer"].notna()
        & scoped["year"].isin(STRUCTURAL_WINDOW_YEARS)
        & pd.to_numeric(scoped["hs_eligible"], errors="coerce").eq(1)
    ]
    worst_gap = 0.0
    checked = []
    valid = True
    for firm, disclosed in G10_DISCLOSED_SHARES.items():
        firm_rows = scoped[scoped["manufacturer"].eq(firm)]
        total = float(firm_rows["value_usd"].sum())
        if total <= 0:
            valid = False
            checked.append(f"{firm}=no_value")
            continue
        origin = firm_rows["origin_country"].astype(str)
        grouped = origin.where(
            ~origin.isin(GREATER_CHINA_ORIGINS), "GREATER_CHINA"
        )
        shares = firm_rows.groupby(grouped)["value_usd"].sum() / total
        for country, target in disclosed.items():
            gap = abs(float(shares.get(country, 0.0)) - float(target))
            worst_gap = max(worst_gap, gap)
            valid &= gap <= G10_TOLERANCE
        checked.append(firm)
    detail = (
        "Panjiva athletic origin value shares within "
        f"{G10_TOLERANCE:.2f} of 10-K production shares "
        f"(Greater China aggregated; value-vs-pairs basis documented); "
        f"firms={','.join(checked)}"
    )
    return _gate("G10", "pass" if valid else "fail", worst_gap, detail)


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
    annual_origin: Mapping[str, pd.DataFrame],
    licensed_paths: Iterable[Path | str],
    licensed_root: Path | str,
) -> dict:
    """Evaluate G0--G9; only G6 may fail without invalidating origin games."""

    gates = pd.DataFrame(
        [
            _g0(chunks, validation_metrics),
            _g1(origin_panels, chunks),
            _g2(chunks, origin_panels),
            _g3(chunks, parent_seed),
            _g4(chunks),
            _g5(chunks["finished"]),
            _g6(chunks[GAMES[0]]),
            _g7(review_queue),
            _g8(annual_origin, parent_seed),
            _g9(licensed_paths, licensed_root),
            _g10(chunks[GAMES[0]], parent_seed),
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
    for game in GAMES:
        direct_sql = build_validation_sql(
            game, parent_ids, start, end, description_identity
        )
        sql_hashes[game] = hashlib.sha256(direct_sql.encode("utf-8")).hexdigest()
        result = query(connection, direct_sql)
        if not isinstance(result, pd.DataFrame):
            raise ValueError("G0 direct SQL result must be a dataframe")
        result = result.copy()
        result.columns = [str(column).strip().lower() for column in result.columns]
        if tuple(result.columns) != (
            "output_row_count",
            "unique_shipment_count",
            "value_usd",
            "shipment_equivalent",
        ) or len(result) != 1:
            raise ValueError("G0 direct SQL result has an invalid exact contract")
        row = result.iloc[0]
        values = [pd.to_numeric(pd.Series([row[column]]), errors="coerce").iat[0] for column in result.columns]
        if any(pd.isna(value) or not math.isfinite(float(value)) or float(value) < 0 for value in values):
            raise ValueError("G0 direct SQL metrics must be finite and nonnegative")
        if not float(values[0]).is_integer() or not float(values[1]).is_integer():
            raise ValueError("G0 output and unique shipment counts must be integral")
        manifest = extraction._read_manifest(run_root / "_manifests" / f"{game}.json")
        entry = manifest["chunks"].get(f"{game}/{quarter}")
        if (
            not extraction._entry_file_verified(entry)
            or entry.get("parent_seed_sha256") != parent_seed_sha256
        ):
            raise ValueError("G0 isolated validation chunk is stale or unverified")
        reconciled = (
            int(values[0]) == int(entry["row_count"])
            and int(values[1])
            == int(entry["unique_shipment_count_nonadditive"])
            and math.isclose(float(values[2]), float(entry["allocated_value_usd_sum"]), rel_tol=1e-10, abs_tol=1e-6)
            and math.isclose(float(values[3]), float(entry["shipment_equivalent_sum"]), rel_tol=1e-10, abs_tol=1e-8)
        )
        records.append(
            {
                "game": game,
                "quarter": quarter,
                "direct_output_row_count": int(values[0]),
                "direct_unique_shipment_count": int(values[1]),
                "direct_value_usd": float(values[2]),
                "direct_shipment_equivalent": float(values[3]),
                "isolated_row_count": int(entry["row_count"]),
                "isolated_unique_shipment_count": int(
                    entry["unique_shipment_count_nonadditive"]
                ),
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
        "reconciled": bool(metrics["reconciled"].eq(1).all()),
    }


def _load_transform_outputs(
    game: str,
    root: Path,
    parent_seed_sha256: str,
    *,
    source_manifest_sha256: str,
    source_chunk_sha256: Iterable[str],
    manual_review_snapshot: Mapping[str, object],
    ownership_continuity_snapshot: Mapping[str, object],
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
        "manual_review_snapshot",
        "ownership_continuity_snapshot",
        "outputs",
        "shipment_measurement_status",
    }
    if set(manifest) != required or manifest.get("manifest_version") != "tire-transform-manifest-v4":
        raise ValueError("transform manifest has an invalid exact contract")
    if (
        manifest.get("game") != game
        or manifest.get("parent_seed_sha256") != parent_seed_sha256
        or manifest.get("source_manifest_sha256") != source_manifest_sha256
        or manifest.get("source_chunk_sha256") != list(source_chunk_sha256)
        or manifest.get("manual_review_snapshot") != dict(manual_review_snapshot)
        or manifest.get("ownership_continuity_snapshot")
        != dict(ownership_continuity_snapshot)
    ):
        raise ValueError("transform manifest or review snapshot is stale")
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
        if not isinstance(entry, dict) or set(entry) != {
            "path", "sha256", "columns", "dtypes"
        }:
            raise ValueError("transform output entry has an invalid exact contract")
        path = Path(str(entry["path"])).resolve(strict=False)
        if not path.is_relative_to(root) or extraction.sha256_file(path) != entry["sha256"]:
            raise ValueError("transform output path or checksum is stale")
        frame = pd.read_parquet(path)
        if (
            entry["columns"] != list(frame.columns)
            or entry["dtypes"] != [str(dtype) for dtype in frame.dtypes]
        ):
            raise ValueError("transform output manifest schema is stale")
        validate_transform_artifact(name, game, frame)
        frames[name] = frame
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
        or set(pointer["direct_sql_sha256"]) != set(GAMES)
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
        for game in GAMES
    }
    loaded = {}
    licensed_paths = [seed_path]
    review_path = root / "review" / "manual_link_reviews.csv"
    manual_review_snapshot, _ = read_manual_review_snapshot(review_path)
    if manual_review_snapshot["state"] == "present":
        licensed_paths.append(Path(str(manual_review_snapshot["path"])))
    continuity_path = root / "review" / "importer_ownership_continuity.csv"
    continuity_snapshot, continuity = read_ownership_continuity_snapshot(
        continuity_path
    )
    if continuity_snapshot["state"] == "present":
        licensed_paths.append(Path(str(continuity_snapshot["path"])))
    for game in GAMES:
        source_attrs = dict(chunks[game].attrs)
        chunks[game], _released = apply_ownership_continuity(
            chunks[game], continuity
        )
        loaded[game], paths = _load_transform_outputs(
            game,
            root,
            parent_seed_sha256,
            source_manifest_sha256=source_attrs["source_manifest_sha256"],
            source_chunk_sha256=source_attrs["source_chunk_sha256"],
            manual_review_snapshot=manual_review_snapshot,
            ownership_continuity_snapshot=continuity_snapshot,
        )
        licensed_paths.extend(paths)
    validation, paths = _load_g0_metrics(root, parent_seed_sha256)
    licensed_paths.extend(paths)
    review_queue = pd.concat(
        [loaded[game]["review_queue"] for game in GAMES],
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
        annual_origin={game: loaded[game]["annual"] for game in loaded},
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

