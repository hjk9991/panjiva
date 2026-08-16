"""Deterministic link construction for the licensed tire sourcing games.

The primary structural link is manufacturer by origin.  Supplier-parent links
are retained as an explicitly gated extension.  Shipment equivalents are
additive by construction; source-group distinct counts are deliberately
labelled nonadditive and are never represented as exact panel counts.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
import hashlib
from io import BytesIO
import json
import math
from pathlib import Path

import pandas as pd

from .config import (
    ENTRY_CORE_SHIPMENTS,
    ENTRY_CORE_VALUE_USD,
    ENTRY_VALUE_USD,
    OUTPUT_ROOT,
    PRE_ENTRY_YEARS,
    iter_quarters,
    validate_output_path,
)
from .extract import REQUIRED_OUTPUT_COLUMNS
from . import artifacts as artifact_io
from . import extract as extraction


ADDITIVE_COLUMNS = (
    "value_usd",
    "weight_kg",
    "teu",
    "container_count",
    "shipment_equivalent",
)
SOURCE_REQUIRED = {
    "manufacturer_parent_id",
    "shipper_up",
    "origin_country",
    "year_quarter",
    "input_group",
    "finished_market",
    "import_route",
    "shipment_count_nonadditive",
    "estimation_eligible",
    "sensitivity_eligible",
    "review_pending_technically_eligible",
    "hs_eligible",
    "manufacturer_conflict",
    "description_candidate",
    "description_candidate_parent_id",
    "description_ambiguous",
    "importer_xref_ambiguous",
    "shipper_xref_ambiguous",
    "importer_pit_ambiguous",
    "shipper_pit_ambiguous",
    "importer_historical_backcast",
    "shipper_historical_backcast",
    *ADDITIVE_COLUMNS,
}
DIAGNOSTIC_VALUE_COLUMNS = tuple(
    column for column in REQUIRED_OUTPUT_COLUMNS if column.endswith("_value_usd")
)
MANUAL_SOURCE_COLUMNS = {
    "manual_review_status",
    "manual_review_source_note",
    "manual_main_eligible",
    "manual_confirmed_eligible",
}
ALLOWED_SOURCE_COLUMNS = set(REQUIRED_OUTPUT_COLUMNS) | MANUAL_SOURCE_COLUMNS
REVIEW_IDENTITY_COLUMNS = (
    "game",
    "manufacturer_parent_id",
    "review_group",
    "origin_country",
    "input_group",
    "finished_market",
    "link_identity_type",
    "link_identity_value",
    "review_item_id",
)
MANUAL_REVIEW_COLUMNS = (
    *REVIEW_IDENTITY_COLUMNS,
    "review_status",
    "source_note",
)
MANUAL_REVIEW_SNAPSHOT_VERSION = "manual-link-reviews-snapshot-v1"
MANUAL_REVIEW_SCHEMA_VERSION = "manual-link-reviews-schema-v1"


def read_manual_review_snapshot(
    path: Path | str,
) -> tuple[dict[str, object], pd.DataFrame | None]:
    """Read one immutable byte snapshot and its exact review-table contract."""

    canonical = Path(path).resolve(strict=False)
    try:
        before = canonical.stat()
    except FileNotFoundError:
        return (
            {
                "contract_version": MANUAL_REVIEW_SNAPSHOT_VERSION,
                "schema_version": MANUAL_REVIEW_SCHEMA_VERSION,
                "state": "missing",
                "path": str(canonical),
                "sha256": None,
                "size_bytes": 0,
                "columns": [],
                "row_count": 0,
            },
            None,
        )
    if not canonical.is_file():
        raise ValueError("manual review snapshot path is not a regular file")
    try:
        payload = canonical.read_bytes()
        after = canonical.stat()
    except OSError as error:
        raise ValueError("manual review snapshot is unreadable") from error
    identity_before = (before.st_size, before.st_mtime_ns, before.st_ino)
    identity_after = (after.st_size, after.st_mtime_ns, after.st_ino)
    if identity_before != identity_after or len(payload) != after.st_size:
        raise ValueError("manual review snapshot changed while being read")
    try:
        frame = pd.read_csv(BytesIO(payload), dtype="string")
    except Exception as error:
        raise ValueError("manual review snapshot CSV is malformed") from error
    if tuple(frame.columns) != MANUAL_REVIEW_COLUMNS:
        raise ValueError("manual review snapshot has an invalid exact schema")
    metadata = {
        "contract_version": MANUAL_REVIEW_SNAPSHOT_VERSION,
        "schema_version": MANUAL_REVIEW_SCHEMA_VERSION,
        "state": "present",
        "path": str(canonical),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size_bytes": len(payload),
        "columns": list(frame.columns),
        "row_count": int(len(frame)),
    }
    return metadata, frame


def normalize_review_identity(frame: pd.DataFrame) -> pd.DataFrame:
    """Canonicalize CSV/Parquet review keys before any exact-key merge."""

    result = frame.copy()
    missing = set(REVIEW_IDENTITY_COLUMNS).difference(result.columns)
    if missing:
        raise ValueError(f"review identity is missing columns: {sorted(missing)}")
    manufacturer = pd.to_numeric(result["manufacturer_parent_id"], errors="coerce")
    if (
        manufacturer.isna().any()
        or manufacturer.le(0).any()
        or not manufacturer.map(lambda value: float(value).is_integer()).all()
    ):
        raise ValueError("review manufacturer identity must be a positive integer")
    result["manufacturer_parent_id"] = manufacturer.astype("Int64")
    for column in REVIEW_IDENTITY_COLUMNS:
        if column != "manufacturer_parent_id":
            result[column] = result[column].astype("string")
    return result


def _validate_source(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.columns.duplicated().any():
        raise ValueError("source has duplicate columns")
    actual = set(map(str, frame.columns))
    missing = SOURCE_REQUIRED.difference(actual)
    unexpected = actual.difference(ALLOWED_SOURCE_COLUMNS)
    if missing:
        raise ValueError(f"source is missing required columns: {sorted(missing)}")
    if unexpected:
        raise ValueError(f"source has unexpected columns: {sorted(unexpected)}")
    if frame.duplicated().any():
        raise ValueError("source has duplicate rows")
    result = frame.copy()
    if not result["year_quarter"].astype("string").str.fullmatch(
        r"[0-9]{4}Q[1-4]"
    ).all():
        raise ValueError("source has invalid year_quarter")
    if result["origin_country"].isna().any() or result["origin_country"].astype(
        "string"
    ).str.strip().eq("").any():
        raise ValueError("source has invalid origin_country")
    numeric = set(ADDITIVE_COLUMNS) | {
        "shipment_count_nonadditive",
        "manufacturer_parent_id",
        "shipper_up",
        "estimation_eligible",
        "sensitivity_eligible",
        "review_pending_technically_eligible",
        "description_candidate_parent_id",
    }
    numeric.update(column for column in result if column.endswith("_value_usd"))
    for column in numeric.intersection(result.columns):
        values = pd.to_numeric(result[column], errors="coerce")
        finite = values.dropna().map(lambda value: math.isfinite(float(value)))
        if not finite.all() or (values.dropna() < 0).any():
            raise ValueError(f"source numeric column is invalid: {column}")
        result[column] = values
    effective = result["manufacturer_parent_id"].fillna(
        pd.to_numeric(result["description_candidate_parent_id"], errors="coerce")
    )
    if (effective.notna() & effective.le(0)).any():
        raise ValueError("source manufacturer identity must be positive")
    # Only explicitly unattributed rows may lack both identities: the finished
    # SQL retains them so attribution coverage (G5) keeps an honest denominator.
    attributed_route = (
        result["import_route"]
        .astype("string")
        .isin(("manufacturer_direct", "distributor_intermediated"))
    )
    if (attributed_route & effective.isna()).any():
        raise ValueError(
            "source requires manufacturer_parent_id or description candidate parent"
        )
    result["manufacturer_parent_id"] = effective.astype("Int64")
    return result


def _group_sum(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    working = frame.copy()
    working["current_parent_fallback_value_usd"] = working[
        [
            column
            for column in (
                "importer_current_parent_fallback_value_usd",
                "shipper_current_parent_fallback_value_usd",
            )
            if column in working
        ]
    ].max(axis=1)
    working["pit_overlap_value_usd"] = working[
        [
            column
            for column in (
                "importer_pit_same_parent_overlap_value_usd",
                "shipper_pit_same_parent_overlap_value_usd",
            )
            if column in working
        ]
    ].max(axis=1)
    description_columns = [
        column
        for column in ("description_candidate_value_usd", "description_ambiguous_value_usd")
        if column in working
    ]
    working["description_review_value_usd"] = working[description_columns].max(axis=1)
    working["main_eligible_value_usd"] = working["value_usd"].where(
        working["estimation_eligible"].eq(1), 0.0
    )
    working["confirmed_sensitivity_value_usd"] = working["value_usd"].where(
        working["sensitivity_eligible"].eq(1), 0.0
    )
    working["manual_main_eligible_value_usd"] = working["value_usd"].where(
        working.get("manual_main_eligible", pd.Series(0, index=working.index)).eq(1),
        0.0,
    )
    working["manual_confirmed_value_usd"] = working["value_usd"].where(
        working.get(
            "manual_confirmed_eligible", pd.Series(0, index=working.index)
        ).eq(1),
        0.0,
    )
    working["manual_main_eligible_shipment_equivalent"] = working[
        "shipment_equivalent"
    ].where(
        working.get("manual_main_eligible", pd.Series(0, index=working.index)).eq(1),
        0.0,
    )
    working["manual_confirmed_shipment_equivalent"] = working[
        "shipment_equivalent"
    ].where(
        working.get(
            "manual_confirmed_eligible", pd.Series(0, index=working.index)
        ).eq(1),
        0.0,
    )
    for route in ("manufacturer_direct", "distributor_intermediated", "unattributed"):
        working[f"{route}_value_usd"] = working["value_usd"].where(
            working["import_route"].eq(route), 0.0
        )
    sum_columns = list(ADDITIVE_COLUMNS) + [
        "shipment_count_nonadditive",
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
    ]
    for column in DIAGNOSTIC_VALUE_COLUMNS:
        if column in working and column not in sum_columns:
            sum_columns.append(column)
    grouped = (
        working.groupby(keys, dropna=False, sort=True, as_index=False)[sum_columns]
        .sum(min_count=1)
        .sort_values(keys, kind="stable")
        .reset_index(drop=True)
    )
    grouped = grouped.rename(
        columns={
            "shipment_count_nonadditive": "shipment_count_source_group_sum_nonadditive"
        }
    )
    # The source-group distinct-count sum is not a distinct panel count.  The
    # only additive shipment measure available after HS allocation remains
    # explicitly named shipment_equivalent.
    grouped["shipment_measurement_status"] = (
        "shipment_equivalent_additive;distinct_panel_count_unavailable"
    )
    denominator = grouped["value_usd"]
    share_inputs = {
        "current_parent_fallback_value_share": "current_parent_fallback_value_usd",
        "pit_overlap_value_share": "pit_overlap_value_usd",
        "description_review_value_share": "description_review_value_usd",
        "main_eligible_value_share": "main_eligible_value_usd",
        "confirmed_sensitivity_value_share": "confirmed_sensitivity_value_usd",
        "manual_main_eligible_value_share": "manual_main_eligible_value_usd",
        "manual_confirmed_value_share": "manual_confirmed_value_usd",
        "manufacturer_direct_value_share": "manufacturer_direct_value_usd",
        "distributor_intermediated_value_share": "distributor_intermediated_value_usd",
        "unattributed_value_share": "unattributed_value_usd",
    }
    for output, numerator in share_inputs.items():
        grouped[output] = grouped[numerator].div(denominator.where(denominator.gt(0)))
    return grouped


def build_quarterly_panels(frame: pd.DataFrame, *, game: str) -> dict[str, pd.DataFrame]:
    """Build deterministic primary-origin and supplier-extension panels."""

    if game not in {"raw", "finished"}:
        raise ValueError("game must be raw or finished")
    source = _validate_source(frame)
    market = "input_group" if game == "raw" else "finished_market"
    route_keys = [] if game == "raw" else ["import_route"]
    origin_keys = [
        "manufacturer_parent_id",
        "origin_country",
        *route_keys,
        market,
        "year_quarter",
    ]
    supplier_source = source.copy()
    supplier_source["supplier_parent_id"] = pd.to_numeric(
        supplier_source["shipper_up"], errors="coerce"
    ).mask(lambda values: values.le(0)).astype("Int64")
    supplier_source["supplier_parent_matched"] = supplier_source[
        "supplier_parent_id"
    ].notna().astype("int8")
    supplier_keys = [
        "manufacturer_parent_id",
        "supplier_parent_id",
        "supplier_parent_matched",
        "origin_country",
        *route_keys,
        market,
        "year_quarter",
    ]
    origin = _group_sum(source, origin_keys)
    origin.insert(0, "game", game)
    origin.insert(1, "link_level", "origin")
    origin.insert(2, "link_id", origin["origin_country"].astype("string"))
    supplier = _group_sum(supplier_source, supplier_keys)
    supplier.insert(0, "game", game)
    supplier.insert(1, "link_level", "supplier")
    supplier.insert(
        2,
        "link_id",
        supplier["supplier_parent_id"].astype("string").fillna("UNMATCHED"),
    )
    return {"origin": origin, "supplier": supplier}


def _ensure_unique(frame: pd.DataFrame, keys: Sequence[str], label: str) -> None:
    if frame.duplicated(list(keys)).any():
        raise ValueError(f"{label} has duplicate keys")


def build_annual_entries(
    frame: pd.DataFrame,
    *,
    study_years: Iterable[int] | None = None,
) -> pd.DataFrame:
    """Balance manufacturer-link years and construct three separate entry objects."""

    required = {
        "manufacturer_parent_id",
        "link_id",
        "year",
        "value_usd",
    }
    count_column = (
        "shipment_equivalent_sum"
        if "shipment_equivalent_sum" in frame.columns
        else "shipment_count"
        if "shipment_count" in frame.columns
        else None
    )
    if count_column is None:
        raise ValueError(
            "annual entry source requires shipment_equivalent_sum pilot proxy"
        )
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"annual entry source is missing columns: {sorted(missing)}")
    source = frame.copy()
    if "observed" not in source:
        source["observed"] = 1
    keys = ["manufacturer_parent_id", "link_id", "year"]
    _ensure_unique(source, keys, "annual entry source")
    years = sorted(
        {int(value) for value in (study_years if study_years is not None else source["year"])}
    )
    if not years:
        raise ValueError("study years cannot be empty")
    identities = source[["manufacturer_parent_id", "link_id"]].drop_duplicates()
    grid = identities.merge(pd.DataFrame({"year": years}), how="cross")
    result = grid.merge(source, on=keys, how="left", validate="one_to_one")
    result["observed"] = result["observed"].fillna(0).astype("int8")
    for column in ("value_usd", count_column):
        result[column] = pd.to_numeric(result[column], errors="coerce")
        result.loc[result["observed"].eq(0), column] = pd.NA
        if (result.loc[result["observed"].eq(1), column].isna()).any():
            raise ValueError(f"observed annual rows require {column}")
        if (result[column].dropna() < 0).any():
            raise ValueError(f"annual {column} must be nonnegative")
    active = (result["value_usd"].gt(0) | result[count_column].gt(0)).astype("Int8")
    active = active.mask(result["observed"].eq(0), pd.NA)
    result["active"] = active
    result = result.sort_values(keys, kind="stable").reset_index(drop=True)
    group = result.groupby(["manufacturer_parent_id", "link_id"], sort=False)
    previous_inactive = pd.Series(True, index=result.index)
    previous_observed = pd.Series(True, index=result.index)
    for lag in range(1, PRE_ENTRY_YEARS + 1):
        lag_active = group["active"].shift(lag)
        lag_observed = group["observed"].shift(lag)
        previous_inactive &= lag_active.eq(0)
        previous_observed &= lag_observed.eq(1)
    risk = previous_inactive & previous_observed & result["observed"].eq(1)
    raw = (risk & result["active"].eq(1)).astype("Int8")
    value = (raw.eq(1) & result["value_usd"].ge(ENTRY_VALUE_USD)).astype("Int8")
    next_active = group["active"].shift(-1)
    next_observed = group["observed"].shift(-1)
    core_candidate = (
        raw.eq(1)
        & result["value_usd"].ge(ENTRY_CORE_VALUE_USD)
        & result[count_column].ge(ENTRY_CORE_SHIPMENTS)
    )
    core = (core_candidate & next_observed.eq(1) & next_active.eq(1)).astype("Int8")
    unknown_risk = ~risk & (
        result["observed"].eq(0) | ~previous_observed
    )
    raw = raw.mask(unknown_risk, pd.NA)
    value = value.mask(unknown_risk, pd.NA)
    censored = (core_candidate & result["year"].eq(2025)).astype("int8")
    core = core.mask(censored.eq(1), pd.NA)
    # A core candidate outside 2025 with an unobserved lead also remains unknown.
    core = core.mask(core_candidate & ~next_observed.eq(1), pd.NA)
    result["entry_raw"] = raw
    result["entry_value"] = value
    result["entry_core"] = core
    result["entry_core_censored"] = censored
    result["entry_core_count_basis"] = "allocated_shipment_equivalent"
    result["entry_core_measurement_status"] = (
        "pilot_proxy_exact_distinct_unavailable"
    )
    return result


def build_dynamic_link_moments(
    frame: pd.DataFrame,
    *,
    start_year: int = 2016,
    end_year: int = 2021,
) -> pd.DataFrame:
    """Compute untargeted pre-fit transition moments on adjacent observed pairs."""

    if start_year >= end_year:
        raise ValueError("dynamic moment window must contain at least two years")
    required = {"manufacturer_parent_id", "link_id", "year", "active"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"dynamic source is missing columns: {sorted(missing)}")
    source = frame.copy()
    if "observed" not in source:
        source["observed"] = 1
    source = source.loc[source["year"].between(start_year, end_year)].copy()
    _ensure_unique(source, ["manufacturer_parent_id", "link_id", "year"], "dynamic source")
    identities = frame[["manufacturer_parent_id", "link_id"]].drop_duplicates()
    grid = identities.merge(
        pd.DataFrame({"year": list(range(start_year, end_year + 1))}), how="cross"
    )
    balanced = grid.merge(
        source,
        on=["manufacturer_parent_id", "link_id", "year"],
        how="left",
        validate="one_to_one",
    )
    balanced["observed"] = balanced["observed"].fillna(0).astype("int8")
    balanced["active"] = pd.to_numeric(balanced["active"], errors="coerce").astype("Int8")
    balanced.loc[balanced["observed"].eq(0), "active"] = pd.NA
    balanced = balanced.sort_values(
        ["manufacturer_parent_id", "link_id", "year"], kind="stable"
    )
    group = balanced.groupby(["manufacturer_parent_id", "link_id"], sort=False)
    lag_active = group["active"].shift(1)
    lag_observed = group["observed"].shift(1)
    adjacent = balanced["observed"].eq(1) & lag_observed.eq(1)
    entry_risk = adjacent & lag_active.eq(0)
    active_risk = adjacent & lag_active.eq(1)
    entry_events = (entry_risk & balanced["active"].eq(1)).sum()
    exit_events = (active_risk & balanced["active"].eq(0)).sum()
    persistence_events = (active_risk & balanced["active"].eq(1)).sum()
    entry_n = int(entry_risk.sum())
    active_n = int(active_risk.sum())
    result = pd.DataFrame(
        {
            "window_start": [int(start_year)],
            "window_end": [int(end_year)],
            "entry_risk_n": [entry_n],
            "active_risk_n": [active_n],
            "entry_rate": [float(entry_events / entry_n) if entry_n else pd.NA],
            "exit_rate": [float(exit_events / active_n) if active_n else pd.NA],
            "persistence_rate": [
                float(persistence_events / active_n) if active_n else pd.NA
            ],
            "targeted": [0],
        }
    )
    for column in ("entry_rate", "exit_rate", "persistence_rate"):
        result[column] = pd.to_numeric(result[column], errors="coerce").astype("Float64")
    return result


def load_verified_game_chunks(
    game: str,
    *,
    parent_seed_sha256: str,
    output_root: Path | str = OUTPUT_ROOT,
    expected_quarters: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Read only the exact, current, file-verified Task 6 chunk set."""

    if game not in {"raw", "finished"}:
        raise ValueError("game must be raw or finished")
    root = Path(output_root).resolve(strict=False)
    if Path(output_root) == OUTPUT_ROOT:
        root = validate_output_path(root)
    quarters = tuple(expected_quarters if expected_quarters is not None else iter_quarters())
    if len(set(quarters)) != len(quarters):
        raise ValueError("expected quarters must be unique")
    expected_keys = {f"{game}/{quarter}" for quarter in quarters}
    manifest_target = root / "_manifests" / f"{game}.json"
    manifest = extraction._read_manifest(manifest_target)
    if set(manifest["chunks"]) != expected_keys:
        raise ValueError("manifest does not contain the exact expected chunks")
    frames = []
    source_hashes = []
    for quarter in quarters:
        key = f"{game}/{quarter}"
        entry = manifest["chunks"][key]
        if entry.get("parent_seed_sha256") != parent_seed_sha256:
            raise ValueError("chunk manifest parent seed is stale")
        if not extraction._entry_file_verified(entry):
            raise ValueError("chunk file or manifest is stale, incomplete, or corrupt")
        path = Path(str(entry["output_path"])).resolve(strict=False)
        if not path.is_relative_to(root):
            raise ValueError("chunk path escapes the licensed output root")
        frame = pd.read_parquet(path)
        extraction.validate_output_frame(frame, quarter)
        frames.append(frame)
        source_hashes.append(str(entry["file_sha256"]))
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=REQUIRED_OUTPUT_COLUMNS)
    combined.attrs["source_manifest_sha256"] = extraction.sha256_file(manifest_target)
    combined.attrs["source_chunk_sha256"] = tuple(source_hashes)
    combined.attrs["parent_seed_sha256"] = parent_seed_sha256
    return combined


def _annual_origin_source(
    origin: pd.DataFrame,
    study_years: Iterable[int],
    manufacturer_parent_ids: Iterable[int] | None = None,
) -> pd.DataFrame:
    years = tuple(sorted({int(year) for year in study_years}))
    if not years:
        raise ValueError("study years cannot be empty")
    source = origin.assign(year=origin["year_quarter"].str.slice(0, 4).astype(int))
    # Unattributed groups carry no manufacturer, so they are not sourcing links;
    # both the activity aggregation and the ever-observed origin universe use
    # attributed flows only.
    source = source.loc[source["manufacturer_parent_id"].notna()]
    value_column = (
        "manual_main_eligible_value_usd"
        if "manual_main_eligible_value_usd" in source
        else "value_usd"
    )
    equivalent_column = (
        "manual_main_eligible_shipment_equivalent"
        if "manual_main_eligible_shipment_equivalent" in source
        else "shipment_equivalent"
    )
    active = source.groupby(
        ["manufacturer_parent_id", "link_id", "year"], as_index=False, sort=True
    ).agg(
        value_usd=(value_column, "sum"),
        shipment_equivalent_sum=(equivalent_column, "sum"),
        shipment_count_source_group_sum_nonadditive=(
            "shipment_count_source_group_sum_nonadditive",
            "sum",
        ),
    )
    observed_manufacturers = set(
        pd.to_numeric(source["manufacturer_parent_id"], errors="raise").astype(int)
    )
    manufacturers = sorted(
        {
            int(value)
            for value in (
                manufacturer_parent_ids
                if manufacturer_parent_ids is not None
                else observed_manufacturers
            )
        }
    )
    if not manufacturers or any(value <= 0 for value in manufacturers):
        raise ValueError("annual origin grid requires positive seed manufacturer IDs")
    if not observed_manufacturers.issubset(manufacturers):
        raise ValueError("observed manufacturer is outside the seed manufacturer grid")
    universe = sorted(source["link_id"].astype("string").dropna().unique())
    if not universe:
        raise ValueError("annual origin grid requires an observed game-wide universe")
    identities = pd.DataFrame(
        {"manufacturer_parent_id": manufacturers}
    ).merge(pd.DataFrame({"link_id": universe}), how="cross")
    grid = identities.merge(pd.DataFrame({"year": years}), how="cross")
    annual = grid.merge(
        active,
        on=["manufacturer_parent_id", "link_id", "year"],
        how="left",
        validate="one_to_one",
    )
    for column in (
        "value_usd",
        "shipment_equivalent_sum",
        "shipment_count_source_group_sum_nonadditive",
    ):
        annual[column] = annual[column].fillna(0.0)
    annual["link_universe_basis"] = "game_wide_ever_observed_origin_links"
    annual["observed"] = 1
    return annual


def build_game_artifacts(
    frame: pd.DataFrame,
    *,
    game: str,
    study_years: Iterable[int] = range(2014, 2026),
    manufacturer_parent_ids: Iterable[int] | None = None,
) -> dict[str, pd.DataFrame]:
    """Build quarterly extensions, balanced annual origin links, and diagnostics."""

    quarterly = build_quarterly_panels(frame, game=game)
    annual_source = _annual_origin_source(
        quarterly["origin"], study_years, manufacturer_parent_ids
    )
    annual = build_annual_entries(annual_source, study_years=study_years)
    annual.insert(0, "game", game)
    dynamic = build_dynamic_link_moments(
        annual,
        start_year=2016,
        end_year=2021,
    )
    dynamic.insert(0, "game", game)
    dynamic["measurement_status"] = (
        "untargeted_static_bne_diagnostic;"
        "origin_universe=game_wide_ever_observed_origin_links"
    )
    return {
        "origin_quarterly": quarterly["origin"],
        "supplier_quarterly": quarterly["supplier"],
        "annual": annual,
        "dynamic_moments": dynamic,
    }


def build_review_items(frame: pd.DataFrame, *, game: str) -> pd.DataFrame:
    """Create value-ranked human-review units without assigning a status."""

    source = _validate_source(frame)
    source = source.loc[source["manufacturer_parent_id"].notna()]
    source = _with_review_keys(source, game)
    return (
        source.groupby(
            list(REVIEW_IDENTITY_COLUMNS),
            as_index=False,
            sort=True,
            dropna=False,
        )["value_usd"]
        .sum()
        .loc[:, [*REVIEW_IDENTITY_COLUMNS, "value_usd"]]
    )


def _with_review_keys(frame: pd.DataFrame, game: str) -> pd.DataFrame:
    source = frame.copy()
    effective_manufacturer = pd.to_numeric(
        source["manufacturer_parent_id"], errors="coerce"
    )
    candidate_manufacturer = pd.to_numeric(
        source["description_candidate_parent_id"], errors="coerce"
    )
    source["manufacturer_parent_id"] = effective_manufacturer.fillna(
        candidate_manufacturer
    ).astype("Int64")
    if source["manufacturer_parent_id"].isna().any():
        raise ValueError("review item lacks a manufacturer or description candidate")
    shipper_parent = pd.to_numeric(source["shipper_up"], errors="coerce")
    shipper_company = pd.to_numeric(source["shipper_companyid"], errors="coerce")
    shipper_panjiva = pd.to_numeric(source["shipper_panjiva_id"], errors="coerce")
    identity_type = pd.Series("unresolved", index=source.index, dtype="string")
    identity_value = pd.Series(pd.NA, index=source.index, dtype="string")
    for values, label in (
        (shipper_parent, "shipper_ultimate_parent"),
        (shipper_company, "shipper_company"),
        (shipper_panjiva, "shipper_panjiva"),
    ):
        mask = identity_value.isna() & values.notna() & values.gt(0)
        identity_type.loc[mask] = label
        identity_value.loc[mask] = values.loc[mask].astype("int64").astype("string")
    # Rows attributed only through a supplier-side description candidate have no
    # reviewable entity without a supplier identity; importer-attributed rows keep
    # an explicit unknown-supplier bucket so their value stays in review coverage.
    # A present description candidate implies candidate-only attribution because
    # the SQL emits candidates only where importer and shipper parents missed.
    missing_identity = identity_value.isna()
    if (missing_identity & candidate_manufacturer.notna()).any():
        raise ValueError("review item lacks a stable supplier/plant identity")
    identity_type.loc[missing_identity] = "unknown_supplier"
    identity_value.loc[missing_identity] = "none"
    source["link_identity_type"] = identity_type
    source["link_identity_value"] = identity_value
    if game == "raw":
        source["review_group"] = source["input_group"].astype("string")
    elif game == "finished":
        source["review_group"] = source["finished_market"].astype("string")
    else:
        raise ValueError("game must be raw or finished")
    source["game"] = game
    identity_columns = [
        "game",
        "manufacturer_parent_id",
        "review_group",
        "origin_country",
        "input_group",
        "finished_market",
        "link_identity_type",
        "link_identity_value",
    ]
    source["review_item_id"] = source[identity_columns].apply(
        lambda row: hashlib.sha256(
            json.dumps(
                [None if pd.isna(value) else str(value) for value in row],
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode("ascii")
        ).hexdigest(),
        axis=1,
    ).astype("string")
    return normalize_review_identity(source)


def apply_manual_reviews(
    frame: pd.DataFrame,
    *,
    game: str,
    reviews: pd.DataFrame | None,
) -> pd.DataFrame:
    """Apply explicit human decisions; never infer or auto-approve a status."""

    validated = _validate_source(frame).reset_index(drop=True)
    validated["_source_row_order"] = range(len(validated))
    keyable_mask = validated["manufacturer_parent_id"].notna()
    unkeyable = validated.loc[~keyable_mask].copy()
    source = _with_review_keys(validated.loc[keyable_mask], game)
    review_columns = [*REVIEW_IDENTITY_COLUMNS, "review_status", "source_note"]
    if reviews is None:
        source["manual_review_status"] = pd.NA
        source["manual_review_source_note"] = pd.NA
    else:
        if set(reviews.columns) != set(review_columns):
            raise ValueError("manual reviews have an invalid exact contract")
        if reviews.duplicated(list(REVIEW_IDENTITY_COLUMNS)).any():
            raise ValueError("manual reviews have duplicate keys")
        status = reviews["review_status"].astype("string")
        notes = reviews["source_note"].astype("string")
        if (
            status.isna().any()
            or not status.isin({"confirmed", "probable", "unclear"}).all()
            or notes.isna().any()
            or notes.str.strip().eq("").any()
        ):
            raise ValueError("manual reviews require approved status and source_note")
        decisions = normalize_review_identity(reviews).rename(
            columns={
                "review_status": "manual_review_status",
                "source_note": "manual_review_source_note",
            }
        )
        source = source.merge(
            decisions,
            on=list(REVIEW_IDENTITY_COLUMNS),
            how="left",
            validate="many_to_one",
            sort=False,
        )
    status = source["manual_review_status"].astype("string")
    route_allowed = source["import_route"].isin(
        {"manufacturer_direct", "distributor_intermediated"}
    )
    if game == "finished":
        route_allowed |= (
            source["description_candidate"].eq(1)
            & pd.to_numeric(
                source["description_candidate_parent_id"], errors="coerce"
            ).notna()
        )
    technical = (
        source["review_pending_technically_eligible"].eq(1)
        & source["hs_eligible"].eq(1)
        & source["manufacturer_conflict"].eq(0)
        & source["description_ambiguous"].eq(0)
        & source["importer_xref_ambiguous"].eq(0)
        & source["shipper_xref_ambiguous"].eq(0)
        & source["importer_pit_ambiguous"].eq(0)
        & source["shipper_pit_ambiguous"].eq(0)
        & source["importer_historical_backcast"].eq(0)
        & source["shipper_historical_backcast"].eq(0)
        & route_allowed
    )
    source["manual_main_eligible"] = (
        status.isin({"confirmed", "probable"}).fillna(False) & technical
    ).astype("int8")
    source["manual_confirmed_eligible"] = (
        status.eq("confirmed").fillna(False) & technical
    ).astype("int8")
    released_description = (
        source["manual_main_eligible"].eq(1)
        & source["description_candidate"].eq(1)
        & source["import_route"].eq("unattributed")
    )
    source.loc[released_description, "import_route"] = "distributor_intermediated"
    processed = source.drop(
        columns=[
            "game",
            "review_group",
            "link_identity_type",
            "link_identity_value",
            "review_item_id",
        ]
    )
    if not unkeyable.empty:
        unkeyable["manual_review_status"] = pd.Series(
            pd.NA, index=unkeyable.index, dtype="string"
        )
        unkeyable["manual_review_source_note"] = pd.Series(
            pd.NA, index=unkeyable.index, dtype="string"
        )
        unkeyable["manual_main_eligible"] = pd.Series(
            0, index=unkeyable.index, dtype="int8"
        )
        unkeyable["manual_confirmed_eligible"] = pd.Series(
            0, index=unkeyable.index, dtype="int8"
        )
        processed = pd.concat(
            [processed, unkeyable.loc[:, list(processed.columns)]],
            ignore_index=True,
        )
    return (
        processed.sort_values("_source_row_order", kind="stable")
        .drop(columns="_source_row_order")
        .reset_index(drop=True)
    )


def build_game_outputs(
    *,
    game: str,
    parent_seed_sha256: str,
    manufacturer_parent_ids: Iterable[int],
    output_root: Path | str = OUTPUT_ROOT,
) -> dict:
    """Serialize and atomically publish one game's licensed Task 7 artifacts."""

    root = validate_output_path(Path(output_root))
    build_lock = root / "_manifests" / ".transform-build.lock"
    with extraction.extraction_lock(build_lock):
        return _build_game_outputs_locked(
            game=game,
            parent_seed_sha256=parent_seed_sha256,
            manufacturer_parent_ids=manufacturer_parent_ids,
            root=root,
        )


def _build_game_outputs_locked(
    *,
    game: str,
    parent_seed_sha256: str,
    manufacturer_parent_ids: Iterable[int],
    root: Path,
) -> dict:
    chunks = load_verified_game_chunks(
        game,
        parent_seed_sha256=parent_seed_sha256,
        output_root=root,
    )
    source_manifest_sha256 = chunks.attrs["source_manifest_sha256"]
    source_chunk_sha256 = list(chunks.attrs["source_chunk_sha256"])
    from .qa import build_manual_review_queue, validate_transform_artifact

    review_items = build_review_items(chunks, game=game)
    review_path = root / "review" / "manual_link_reviews.csv"
    review_snapshot, reviews = read_manual_review_snapshot(review_path)
    review_queue = build_manual_review_queue(review_items, reviews)
    reviewed_chunks = apply_manual_reviews(chunks, game=game, reviews=reviews)
    outputs = build_game_artifacts(
        reviewed_chunks,
        game=game,
        manufacturer_parent_ids=manufacturer_parent_ids,
    )
    targets = {
        "origin_quarterly": root / f"panel_source_quarter_{game}.parquet",
        "supplier_quarterly": root / f"panel_supplier_quarter_{game}.parquet",
        "annual": root / f"panel_annual_{game}.parquet",
        "dynamic_moments": root / f"moments_dynamic_links_{game}.parquet",
        "review_queue": root / "review" / f"manual_review_queue_{game}.parquet",
    }
    hashes = {}
    for name, frame in {**outputs, "review_queue": review_queue}.items():
        validate_transform_artifact(name, game, frame)
        hashes[name] = artifact_io.atomic_write_parquet(frame, targets[name])
    manifest = {
        "manifest_version": "tire-transform-manifest-v3",
        "game": game,
        "parent_seed_sha256": parent_seed_sha256,
        "source_manifest_sha256": source_manifest_sha256,
        "source_chunk_sha256": source_chunk_sha256,
        "manual_review_snapshot": review_snapshot,
        "outputs": {
            name: {
                "path": str(targets[name]),
                "sha256": hashes[name],
                "columns": list(frame.columns),
                "dtypes": [str(dtype) for dtype in frame.dtypes],
            }
            for name, frame in {**outputs, "review_queue": review_queue}.items()
        },
        "shipment_measurement_status": (
            "shipment_equivalent_pilot_proxy;exact_panel_distinct_unavailable"
        ),
    }
    manifest_path = root / "_manifests" / f"transform_{game}.json"
    artifact_io.atomic_write_json(manifest, manifest_path)
    return {
        "status": "built",
        "game": game,
        "manifest_path": str(manifest_path),
        "outputs": {name: str(path) for name, path in targets.items()},
    }
